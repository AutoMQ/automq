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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metadata.S3ObjectMetadata;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

/**
 * Multipart object writer.
 * <p>
 * Writer should ensure that a part, even with size smaller than {@link Writer#MIN_PART_SIZE}, can still be uploaded.
 * For other S3 limits, it is upper layer's responsibility to prevent reaching the limits.
 */
public interface Writer {
    /**
     * The max number of parts. It comes from the limit of S3 multipart upload.
     */
    int MAX_PART_COUNT = 10000;
    /**
     * The max size of a part, i.e. 5GB. It comes from the limit of S3 multipart upload.
     */
    long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;
    /**
     * The min size of a part, i.e. 5MB. It comes from the limit of S3 multipart upload.
     * Note that the last part can be smaller than this size.
     */
    int MIN_PART_SIZE = 5 * 1024 * 1024;
    /**
     * The max size of an object, i.e. 5TB. It comes from the limit of S3 object size.
     */
    long MAX_OBJECT_SIZE = 5L * 1024 * 1024 * 1024 * 1024;

    /**
     * Write a part of the object. The parts will parallel upload to S3.
     *
     * @param data object data.
     */
    CompletableFuture<Void> write(ByteBuf data);

    /**
     * Make a copy of all cached buffer and release old one to prevent outside modification to underlying data and
     * avoid holding buffer reference for too long.
     */
    void copyOnWrite();

    /**
     * Copy a part of the object.
     *
     * @param s3ObjectMetadata source object metadata.
     * @param start      start position of the source object.
     * @param end        end position of the source object.
     */
    void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end);

    boolean hasBatchingPart();

    /**
     * Complete the object.
     */
    CompletableFuture<Void> close();

    /**
     * Release all resources held by this writer.
     */
    CompletableFuture<Void> release();

    /**
     * The bucket which the data was written.
     */
    short bucketId();

    class Context {
        public static final Context DEFAULT = new Context(ByteBufAlloc.DEFAULT);

        private final int allocType;

        public Context(int allocType) {
            this.allocType = allocType;
        }

        public int allocType() {
            return allocType;
        }

    }
}
