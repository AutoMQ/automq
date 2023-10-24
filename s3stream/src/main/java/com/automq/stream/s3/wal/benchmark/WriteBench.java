/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WriteBench is a tool for benchmarking write performance of {@link BlockWALService}
 */
public class WriteBench implements AutoCloseable {
    private static final int LOG_INTERVAL_SECONDS = 1;

    private final WriteAheadLog log;
    private final FlushedOffset flushedOffset = new FlushedOffset();

    public WriteBench(Config config) throws IOException {
        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(config.path, config.capacity);
        if (config.depth != null) {
            builder.ioThreadNums(config.depth);
        }
        this.log = builder.build();
        this.log.start();
        this.log.reset();
    }

    public static void main(String[] args) throws IOException {
        Namespace ns = null;
        ArgumentParser parser = Config.parser();
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        Config config = new Config(ns);

        try (WriteBench bench = new WriteBench(config)) {
            bench.run(config);
        }
    }

    private void run(Config config) {
        System.out.println("Starting benchmark");

        ExecutorService executor = Threads.newFixedThreadPool(
                config.threads, ThreadUtils.createThreadFactory("append-thread-%d", false), null);
        AppendTaskConfig appendTaskConfig = new AppendTaskConfig(config);
        for (int i = 0; i < config.threads; i++) {
            int index = i;
            executor.submit(() -> {
                try {
                    runAppendTask(index, appendTaskConfig);
                } catch (Exception e) {
                    System.err.printf("Append task %d failed, %s\n", index, e.getMessage());
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.durationSeconds + 10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        System.out.println("Benchmark finished");
    }

    private void runAppendTask(int index, AppendTaskConfig config) throws Exception {
        System.out.printf("Append task %d started\n", index);

        byte[] bytes = new byte[config.recordSizeBytes];
        new Random().nextBytes(bytes);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes).retain();
        int intervalNanos = (int) TimeUnit.SECONDS.toNanos(1) / Math.max(1, config.throughputBytes / config.recordSizeBytes);
        long lastAppendTimeNanos = System.nanoTime();
        long lastLogTimeMillis = System.currentTimeMillis();
        long taskStartTimeMillis = System.currentTimeMillis();
        AtomicLong count = new AtomicLong();
        AtomicLong costNanos = new AtomicLong();
        AtomicLong maxCostNanos = new AtomicLong();

        while (true) {
            while (true) {
                long now = System.nanoTime();
                long elapsedNanos = now - lastAppendTimeNanos;
                if (elapsedNanos >= intervalNanos) {
                    lastAppendTimeNanos = now;
                    break;
                }
                LockSupport.parkNanos((intervalNanos - elapsedNanos) >> 2);
            }

            long now = System.currentTimeMillis();
            if (now - taskStartTimeMillis > TimeUnit.SECONDS.toMillis(config.durationSeconds)) {
                break;
            }

            if (now - lastLogTimeMillis > TimeUnit.SECONDS.toMillis(LOG_INTERVAL_SECONDS)) {
                long countValue = count.getAndSet(0);
                long costNanosValue = costNanos.getAndSet(0);
                long maxCostNanosValue = maxCostNanos.getAndSet(0);
                if (0 != countValue) {
                    System.out.printf("Append task %d | Append Rate %d msg/s %d KB/s | Avg Latency %.3f ms | Max Latency %.3f ms\n",
                            index,
                            countValue / LOG_INTERVAL_SECONDS,
                            (countValue * config.recordSizeBytes) / LOG_INTERVAL_SECONDS / 1024,
                            costNanosValue / 1_000_000.0 / countValue,
                            maxCostNanosValue / 1_000_000.0);
                    lastLogTimeMillis = now;
                }
            }

            long appendStartTimeNanos = System.nanoTime();
            WriteAheadLog.AppendResult result;
            try {
                result = log.append(payload.retainedDuplicate());
            } catch (WriteAheadLog.OverCapacityException e) {
                log.trim(flushedOffset.get());
                continue;
            }
            result.future().thenAccept(v -> {
                long costNanosValue = System.nanoTime() - appendStartTimeNanos;
                count.incrementAndGet();
                costNanos.addAndGet(costNanosValue);
                maxCostNanos.accumulateAndGet(costNanosValue, Math::max);
                flushedOffset.update(v.flushedOffset());
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

        // following fields are benchmark configuration
        final Integer threads;
        final Integer throughputBytes;
        final Integer recordSizeBytes;
        final Long durationSeconds;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
            this.depth = ns.getInt("depth");
            this.threads = ns.getInt("threads");
            this.throughputBytes = ns.getInt("throughput");
            this.recordSizeBytes = ns.getInt("recordSize");
            this.durationSeconds = ns.getLong("duration");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                    .newFor("WriteBench")
                    .build()
                    .defaultHelp(true)
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

    static class FlushedOffset {
        private final Lock lock = new ReentrantLock();
        // Offset before which all data has been flushed to disk
        private long flushedOffset = 0;
        // Offset at which all data has been flushed to disk
        private long committedOffset = 0;

        public void update(long offset) {
            lock.lock();
            try {
                if (offset > flushedOffset) {
                    committedOffset = flushedOffset;
                    flushedOffset = offset;
                }
            } finally {
                lock.unlock();
            }
        }

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
