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

import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.impl.block.BlockWALHeader;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.util.WALUtil;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;

/**
 * RecoverTool is a tool to recover records in a WAL manually.
 * It extends {@link BlockWALService} to use tools provided by {@link BlockWALService}
 */
public class RecoverTool extends BlockWALService implements AutoCloseable {

    @SuppressWarnings("this-escape")
    public RecoverTool(Config config) throws IOException {
        super(BlockWALService.recoveryBuilder(config.path));
        super.start();
    }

    public static void main(String[] args) throws IOException {
        Namespace ns = parseArgs(Config.parser(), args);
        Config config = new Config(ns);

        try (RecoverTool tool = new RecoverTool(config)) {
            tool.run(config);
        }
    }

    private void run(Config config) throws IOException {
        BlockWALHeader header = super.tryReadWALHeader();
        System.out.println(header);

        Iterable<RecoverResult> recordsSupplier = () -> recover(header, config);
        Function<ByteBuf, StreamRecordBatch> decoder = StreamRecordBatchCodec::decode;
        Function<ByteBuf, String> stringer = decoder.andThen(StreamRecordBatch::toString);
        Function<Long, String> offsetStringer = offset -> readableOffset(offset, header.getCapacity());
        StreamSupport.stream(recordsSupplier.spliterator(), false)
            .map(it -> new RecoverResultWrapper(it, stringer, offsetStringer))
            .peek(System.out::println)
            .forEach(RecoverResultWrapper::release);
    }

    private Iterator<RecoverResult> recover(BlockWALHeader header, Config config) {
        long recoverOffset = config.offset != null ? config.offset : header.getTrimOffset();
        long windowLength = config.windowLength != -1 ? config.windowLength : header.getSlidingWindowMaxLength();
        RecoverIteratorV0 iterator = new RecoverIteratorV0(recoverOffset, windowLength, -1);
        if (config.strict) {
            iterator.strictMode();
        }
        if (config.showInvalid) {
            iterator.reportError();
        }
        return iterator;
    }

    private String readableOffset(long offset, long capacity) {
        long physical = WALUtil.recordOffsetToPosition(offset, capacity, WAL_HEADER_TOTAL_CAPACITY);
        long mod = physical % 4096;
        return String.format("Offset{logical=%d, physical=%d, mod=%d}", offset, physical, mod);
    }

    @Override
    public void close() {
        super.shutdownGracefully();
    }

    /**
     * A wrapper for {@link RecoverResult} to provide a function to convert {@link RecoverResult#record} to string
     */
    public static class RecoverResultWrapper {
        private final RecoverResult inner;
        /**
         * A function to convert {@link RecoverResult#record} to string
         */
        private final Function<ByteBuf, String> stringer;
        private final Function<Long, String> offsetStringer;

        public RecoverResultWrapper(RecoverResult inner, Function<ByteBuf, String> stringer, Function<Long, String> offsetStringer) {
            this.inner = inner;
            this.stringer = stringer;
            this.offsetStringer = offsetStringer;
        }

        public void release() {
            inner.record().release();
        }

        @Override
        public String toString() {
            String offset = offsetStringer.apply(inner.recordOffset());
            if (inner instanceof InvalidRecoverResult) {
                InvalidRecoverResult invalid = (InvalidRecoverResult) inner;
                return String.format("%s{", inner.getClass().getSimpleName())
                    + "offset=" + offset
                    + ", error=" + invalid.detail()
                    + '}';
            }
            return String.format("%s{", inner.getClass().getSimpleName())
                + "offset=" + offset
                + String.format(", record=(%d)", inner.record().readableBytes()) + stringer.apply(inner.record())
                + '}';
        }
    }

    public static class Config {
        final String path;
        final Long offset;
        final Long windowLength;
        final Boolean strict;
        final Boolean showInvalid;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.offset = ns.getLong("offset");
            this.windowLength = ns.getLong("windowLength");
            this.strict = ns.getBoolean("strict");
            this.showInvalid = ns.getBoolean("showInvalid");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newArgumentParser("RecoverTool")
                .defaultHelp(true)
                .description("Recover records in a WAL file");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Path of the WAL file");
            parser.addArgument("-o", "--offset")
                .type(Long.class)
                .help("Offset to start recovering, default to the trimmed offset in the WAL header");
            parser.addArgument("-w", "--window-length")
                .dest("windowLength")
                .type(Long.class)
                .setDefault(-1L)
                .help("Length of the sliding window, default to the value in the WAL header");
            parser.addArgument("-s", "--strict")
                .type(Boolean.class)
                .action(Arguments.storeTrue())
                .setDefault(false)
                .help("Strict mode, which will stop when reaching the end of the window, default to false");
            parser.addArgument("-i", "--show-invalid")
                .dest("showInvalid")
                .type(Boolean.class)
                .action(Arguments.storeTrue())
                .setDefault(false)
                .help("Show invalid records, default to false");
            return parser;
        }
    }
}
