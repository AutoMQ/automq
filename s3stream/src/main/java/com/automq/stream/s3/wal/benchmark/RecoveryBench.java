/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.block.BlockWALService;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;
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

    private static int recoverAndReset(WriteAheadLog wal) {
        int recovered = 0;
        for (Iterator<RecoverResult> it = wal.recover(); it.hasNext(); ) {
            it.next().record().release();
            recovered++;
        }
        wal.reset().join();
        return recovered;
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

    private void writeRecords(int numRecords, int recordSizeBytes) throws OverCapacityException {
        System.out.println("Writing " + numRecords + " records of size " + recordSizeBytes + " bytes");
        byte[] bytes = new byte[recordSizeBytes];
        random.nextBytes(bytes);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes).retain();

        AtomicInteger appended = new AtomicInteger();
        for (int i = 0; i < numRecords; i++) {
            AppendResult result = log.append(payload.retainedDuplicate());
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
                .defaultHelp(true)
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
