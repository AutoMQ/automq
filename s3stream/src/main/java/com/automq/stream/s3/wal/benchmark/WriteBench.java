/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;
import static com.automq.stream.s3.wal.benchmark.BenchTool.resetWALHeader;

/**
 * WriteBench is a tool for benchmarking write performance of {@link BlockWALService}
 */
public class WriteBench implements AutoCloseable {
    private static final int LOG_INTERVAL_SECONDS = 1;
    private static final int TRIM_INTERVAL_MILLIS = 100;

    private final WriteAheadLog log;
    private final TrimOffset trimOffset = new TrimOffset();

    // Generate random payloads for this benchmark tool
    private Random random = new Random();

    public WriteBench(Config config) throws IOException {
        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(config.path, config.capacity);
        if (config.depth != null) {
            builder.ioThreadNums(config.depth);
        }
        if (config.iops != null) {
            builder.writeRateLimit(config.iops);
        }
        this.log = builder.build();
        this.log.start();
        this.log.reset();
    }

    public static void main(String[] args) throws IOException {
        Namespace ns = parseArgs(Config.parser(), args);
        Config config = new Config(ns);

        resetWALHeader(config.path);
        try (WriteBench bench = new WriteBench(config)) {
            bench.run(config);
        }
    }

    private static Runnable logIt(Config config, Stat stat) {
        ScheduledExecutorService statExecutor = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("stat-thread-%d", true), null);
        statExecutor.scheduleAtFixedRate(() -> {
            Stat.Result result = stat.reset();
            if (0 != result.count()) {
                System.out.printf("Append task | Append Rate %d msg/s %d KB/s | Avg Latency %.3f ms | Max Latency %.3f ms\n",
                    TimeUnit.SECONDS.toNanos(1) * result.count() / result.elapsedTimeNanos(),
                    TimeUnit.SECONDS.toNanos(1) * (result.count() * config.recordSizeBytes) / result.elapsedTimeNanos() / 1024,
                    (double) result.costNanos() / TimeUnit.MILLISECONDS.toNanos(1) / result.count(),
                    (double) result.maxCostNanos() / TimeUnit.MILLISECONDS.toNanos(1));
            }
        }, LOG_INTERVAL_SECONDS, LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
        return statExecutor::shutdownNow;
    }

    private void run(Config config) {
        System.out.println("Starting benchmark");

        ExecutorService executor = Threads.newFixedThreadPool(
            config.threads, ThreadUtils.createThreadFactory("append-thread-%d", false), null);
        AppendTaskConfig appendTaskConfig = new AppendTaskConfig(config);
        Stat stat = new Stat();
        Runnable stopTrim = runTrimTask();
        for (int i = 0; i < config.threads; i++) {
            int index = i;
            executor.submit(() -> {
                try {
                    runAppendTask(index, appendTaskConfig, stat);
                } catch (Exception e) {
                    System.err.printf("Append task %d failed, %s\n", index, e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        Runnable stopLog = logIt(config, stat);

        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.durationSeconds + 10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        stopLog.run();
        stopTrim.run();

        System.out.println("Benchmark finished");
    }

    private Runnable runTrimTask() {
        ScheduledExecutorService trimExecutor = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("trim-thread-%d", true), null);
        trimExecutor.scheduleAtFixedRate(() -> {
            try {
                log.trim(trimOffset.get());
            } catch (Exception e) {
                System.err.printf("Trim task failed, %s\n", e.getMessage());
                e.printStackTrace();
            }
        }, TRIM_INTERVAL_MILLIS, TRIM_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        return trimExecutor::shutdownNow;
    }

    private void runAppendTask(int index, AppendTaskConfig config, Stat stat) throws Exception {
        System.out.printf("Append task %d started\n", index);

        byte[] bytes = new byte[config.recordSizeBytes];
        random.nextBytes(bytes);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes).retain();
        int intervalNanos = (int) TimeUnit.SECONDS.toNanos(1) / Math.max(1, config.throughputBytes / config.recordSizeBytes);
        long lastAppendTimeNanos = System.nanoTime();
        long taskStartTimeMillis = System.currentTimeMillis();

        while (true) {
            while (true) {
                long now = System.nanoTime();
                long elapsedNanos = now - lastAppendTimeNanos;
                if (elapsedNanos >= intervalNanos) {
                    lastAppendTimeNanos += intervalNanos;
                    break;
                }
                LockSupport.parkNanos((intervalNanos - elapsedNanos) >> 2);
            }

            long now = System.currentTimeMillis();
            if (now - taskStartTimeMillis > TimeUnit.SECONDS.toMillis(config.durationSeconds)) {
                break;
            }

            long appendStartTimeNanos = System.nanoTime();
            AppendResult result;
            try {
                result = log.append(payload.retainedDuplicate());
            } catch (OverCapacityException e) {
                System.err.printf("Append task %d failed, retry it, %s\n", index, e.getMessage());
                continue;
            }
            trimOffset.appended(result.recordOffset());
            result.future().thenAccept(v -> {
                long costNanosValue = System.nanoTime() - appendStartTimeNanos;
                stat.update(costNanosValue);
                trimOffset.flushed(v.flushedOffset());
            }).whenComplete((v, e) -> {
                if (e != null) {
                    System.err.printf("Append task %d failed, %s\n", index, e.getMessage());
                    e.printStackTrace();
                }
            });
        }

        System.out.printf("Append task %d finished\n", index);
    }

    @Override
    public void close() {
        log.shutdownGracefully();
    }

    static class Config {
        // following fields are WAL configuration
        final String path;
        final Long capacity;
        final Integer depth;
        final Integer iops;

        // following fields are benchmark configuration
        final Integer threads;
        final Integer throughputBytes;
        final Integer recordSizeBytes;
        final Long durationSeconds;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
            this.depth = ns.getInt("depth");
            this.iops = ns.getInt("iops");
            this.threads = ns.getInt("threads");
            this.throughputBytes = ns.getInt("throughput");
            this.recordSizeBytes = ns.getInt("recordSize");
            this.durationSeconds = ns.getLong("duration");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newArgumentParser("WriteBench")
                .description("Benchmark write performance of BlockWALService");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Path of the WAL file");
            parser.addArgument("-c", "--capacity")
                .type(Long.class)
                .setDefault((long) 1 << 30)
                .help("Capacity of the WAL in bytes");
            parser.addArgument("-d", "--depth")
                .type(Integer.class)
                .help("IO depth of the WAL");
            parser.addArgument("--iops")
                .type(Integer.class)
                .help("IOPS of the WAL");
            parser.addArgument("--threads")
                .type(Integer.class)
                .setDefault(1)
                .help("Number of threads to use to write");
            parser.addArgument("--throughput")
                .type(Integer.class)
                .setDefault(1 << 20)
                .help("Expected throughput in total in bytes per second");
            parser.addArgument("--record-size")
                .dest("recordSize")
                .type(Integer.class)
                .setDefault(1 << 10)
                .help("Size of each record in bytes");
            parser.addArgument("--duration")
                .type(Long.class)
                .setDefault(60L)
                .help("Duration of the benchmark in seconds");
            return parser;
        }
    }

    static class AppendTaskConfig {
        final int throughputBytes;
        final int recordSizeBytes;
        final long durationSeconds;

        AppendTaskConfig(Config config) {
            this.throughputBytes = config.throughputBytes / config.threads;
            this.recordSizeBytes = config.recordSizeBytes;
            this.durationSeconds = config.durationSeconds;
        }
    }

    static class Stat {
        final AtomicLong count = new AtomicLong();
        final AtomicLong costNanos = new AtomicLong();
        final AtomicLong maxCostNanos = new AtomicLong();
        long lastResetTimeNanos = System.nanoTime();

        public void update(long costNanosValue) {
            count.incrementAndGet();
            costNanos.addAndGet(costNanosValue);
            maxCostNanos.accumulateAndGet(costNanosValue, Math::max);
        }

        /**
         * NOT thread-safe
         */
        public Result reset() {
            long countValue = count.getAndSet(0);
            long costNanosValue = costNanos.getAndSet(0);
            long maxCostNanosValue = maxCostNanos.getAndSet(0);

            long now = System.nanoTime();
            long elapsedTimeNanos = now - lastResetTimeNanos;
            lastResetTimeNanos = now;

            return new Result(countValue, costNanosValue, maxCostNanosValue, elapsedTimeNanos);
        }

        public static final class Result {
            private final long count;
            private final long costNanos;
            private final long maxCostNanos;
            private final long elapsedTimeNanos;

            public Result(long count, long costNanos, long maxCostNanos, long elapsedTimeNanos) {
                this.count = count;
                this.costNanos = costNanos;
                this.maxCostNanos = maxCostNanos;
                this.elapsedTimeNanos = elapsedTimeNanos;
            }

            public long count() {
                return count;
            }

            public long costNanos() {
                return costNanos;
            }

            public long maxCostNanos() {
                return maxCostNanos;
            }

            public long elapsedTimeNanos() {
                return elapsedTimeNanos;
            }
        }
    }

    public static class TrimOffset {
        private final Lock lock = new ReentrantLock();
        // Offsets of all data appended but not yet flushed to disk
        private final NavigableSet<Long> appendedOffsets = new ConcurrentSkipListSet<>();
        // Offset before which all data has been flushed to disk
        private long flushedOffset = -1;
        // Offset at which all data has been flushed to disk
        private long committedOffset = -1;

        public void appended(long offset) {
            appendedOffsets.add(offset);
        }

        public void flushed(long offset) {
            lock.lock();
            try {
                if (offset > flushedOffset) {
                    flushedOffset = offset;
                    Long lower = appendedOffsets.lower(flushedOffset);
                    if (lower != null) {
                        appendedOffsets.headSet(lower).clear();
                        committedOffset = lower;
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * @return the offset at which all data has been flushed to disk, or -1 if no data has been flushed to disk
         */
        public long get() {
            lock.lock();
            try {
                return committedOffset;
            } finally {
                lock.unlock();
            }
        }
    }
}
