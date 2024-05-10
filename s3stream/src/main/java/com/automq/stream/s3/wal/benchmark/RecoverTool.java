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

import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.BlockWALService;
import com.automq.stream.s3.wal.WALHeader;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import static com.automq.stream.s3.wal.benchmark.BenchTool.parseArgs;

/**
 * RecoverTool is a tool to recover records in a WAL manually.
 * It extends {@link BlockWALService} to use tools provided by {@link BlockWALService}
 */
public class RecoverTool extends BlockWALService implements AutoCloseable {

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

    private void run(Config config) {
        WALHeader header = super.tryReadWALHeader();
        System.out.println(header);

        Iterable<RecoverResult> recordsSupplier = () -> recover(header, config);
        Function<ByteBuf, StreamRecordBatch> decoder = StreamRecordBatchCodec::decode;
        Function<ByteBuf, String> stringer = decoder.andThen(StreamRecordBatch::toString);
        StreamSupport.stream(recordsSupplier.spliterator(), false)
            .map(it -> new RecoverResultWrapper(it, stringer))
            .peek(System.out::println)
            .forEach(RecoverResultWrapper::release);
    }

    private Iterator<RecoverResult> recover(WALHeader header, Config config) {
        long recoverOffset = config.offset != null ? config.offset : header.getTrimOffset();
        long windowLength = header.getSlidingWindowMaxLength();
        return new RecoverIterator(recoverOffset, windowLength, -1);
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

        public RecoverResultWrapper(RecoverResult inner, Function<ByteBuf, String> stringer) {
            this.inner = inner;
            this.stringer = stringer;
        }

        public void release() {
            inner.record().release();
        }

        @Override
        public String toString() {
            return String.format("%s{", inner.getClass().getSimpleName())
                + String.format("record=(%d)", inner.record().readableBytes()) + stringer.apply(inner.record())
                + ", offset=" + inner.recordOffset()
                + '}';
        }
    }

    public static class Config {
        final String path;
        final Long offset;

        Config(Namespace ns) {
            this.path = ns.getString("path");
            this.offset = ns.getLong("offset");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                .newFor("RecoverTool")
                .build()
                .defaultHelp(true)
                .description("Recover records in a WAL file");
            parser.addArgument("-p", "--path")
                .required(true)
                .help("Path of the WAL file");
            parser.addArgument("--offset")
                .type(Long.class)
                .help("Offset to start recovering, default to the trimmed offset in the WAL header");
            return parser;
        }
    }
}
