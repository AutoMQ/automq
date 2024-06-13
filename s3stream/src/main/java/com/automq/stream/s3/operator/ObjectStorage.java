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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectStorage {

    /**
     * Get {@link Writer} for the object.
     */
    Writer writer(WriteOptions options, String objectPath);

    /**
     * Range read object from the object.
     */
    CompletableFuture<ByteBuf> rangeRead(ReadOptions options, S3ObjectMetadata objectMetadata, long start, long end);

    CompletableFuture<ByteBuf> rangeRead(S3ObjectMetadata objectMetadata, long start, long end);

    /**
     * Delete a list of objects.
     *
     * @param objectMetadataList object metadata list to delete.
     * @return deleted object keys.
     */
    CompletableFuture<List<String>> delete(List<S3ObjectMetadata> objectMetadataList);

    class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private Writer.Context context = Writer.Context.DEFAULT;

        public WriteOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public WriteOptions context(Writer.Context context) {
            this.context = context;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public Writer.Context context() {
            return context;
        }

    }

    class ReadOptions {
        public static final ReadOptions DEFAULT = new ReadOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;

        public ReadOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }
    }
}
