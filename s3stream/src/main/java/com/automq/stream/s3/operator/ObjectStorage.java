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

import com.automq.stream.s3.ByteBufAlloc;
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

    default CompletableFuture<List<String>> delete(List<S3ObjectMetadata> objectMetadataList) {
        throw new UnsupportedOperationException();
    }


    class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private int allocType = ByteBufAlloc.DEFAULT;

        public WriteOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public WriteOptions allocType(int allocType) {
            this.allocType = allocType;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public int allocType() {
            return allocType;
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
