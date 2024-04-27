/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.time.StopWatch;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;
import static com.automq.stream.s3.wal.benchmark.BenchTool.recoverAndReset;
import static com.automq.stream.s3.wal.benchmark.BenchTool.resetWALHeader;

/**
 * RecoveryBench is a tool to benchmark the recovery performance of {@link BlockWALService}
 */
public class RecoveryBench implements AutoCloseable {

    private final WriteAheadLog log;
    private Random random = new Random();

    public RecoveryBench(Config config) throws IOException {
        this.log = BlockWALService.builder(config.path, config.capacity).build().start();
        recoverAndReset(log);
    }

    public static void main(String[] args) throws Exception {
        Namespace ns = parseArgs(Config.parser(), args);
        Config config = new Config(ns);

        resetWALHeader(config.path);
        try (RecoveryBench bench = new RecoveryBench(config)) {
            bench.run(config);
        }
    }

    private void run(Config config) throws Exception {
        writeRecords(config.numRecords, config.recordSizeBytes);
        recoverRecords(config.path);
    }

    private void writeRecords(int numRecords, int recordSizeBytes) throws WriteAheadLog.OverCapacityException {
        System.out.println("Writing " + numRecords + " records of size " + recordSizeBytes + " bytes");
        byte[] bytes = new byte[recordSizeBytes];
        random.nextBytes(bytes);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes).retain();

        AtomicInteger appended = new AtomicInteger();
        for (int i = 0; i < numRecords; i++) {
            WriteAheadLog.AppendResult result = log.append(payload.retainedDuplicate());
            result.future().whenComplete((r, e) -> {
                if (e != null) {
                    System.err.println("Failed to append record: " + e.getMessage());
                    e.printStackTrace();
                } else {
                    appended.incrementAndGet();
                }
            });
        }
        System.out.println("Appended " + appended.get() + " records (may not be the final number)");
    }

    private void recoverRecords(String path) throws IOException {
        System.out.println("Recovering records from " + path);
        WriteAheadLog recoveryLog = BlockWALService.recoveryBuilder(path).build().start();
        StopWatch stopWatch = StopWatch.createStarted();
        int recovered = recoverAndReset(recoveryLog);
        System.out.println("Recovered " + recovered + " records in " + stopWatch.getTime() + " ms");
    }

    @Override
    public void close() {
        log.shutdownGracefully();
    }

    static class Config {
        // following fields are WAL configuration
        final String path;
        final Long capacity;

        // following fields are benchmark configuration
        final Integer numRecords;
        final Integer recordSizeBytes;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.capacity = ns.getLong("capacity");
            this.numRecords = ns.getInt("records");
            this.recordSizeBytes = ns.getInt("recordSize");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newArgumentParser("RecoveryBench")
                 .description("Benchmark the recovery performance of BlockWALService");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Path of the WAL file");
            parser.addArgument("-c", "--capacity")
                .type(Long.class)
                .setDefault((long) 3 << 30)
                .help("Capacity of the WAL in bytes");
            parser.addArgument("--records")
                .type(Integer.class)
                .setDefault(1 << 20)
                .help("number of records to write");
            parser.addArgument("--record-size")
                .dest("recordSize")
                .type(Integer.class)
                .setDefault(1 << 10)
                .help("size of each record in bytes");
            return parser;
        }
    }
}
