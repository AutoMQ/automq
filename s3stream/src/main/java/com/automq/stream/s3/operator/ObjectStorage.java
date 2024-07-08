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
import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectStorage {

    void close();

    /**
     * Get {@link Writer} for the object.
     */
    Writer writer(WriteOptions options, String objectPath);

    /**
     * Range read object from the object.
     */
    CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end);

    // Low level API
    CompletableFuture<Void> write(WriteOptions options, String objectPath, ByteBuf buf);

    CompletableFuture<List<ObjectInfo>> list(String prefix);

    /**
     * The deleteObjects API have max batch limit.
     * see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html"/>
     * Implementation should handle the objectPaths size exceeded limit condition.
     * When batch split logic is triggered the CompletableFuture means all the deleteBatch if success.
     * The caller may do the batch split logic if the delete operation need fine-grained control
     */
    CompletableFuture<Void> delete(List<ObjectPath> objectPaths);

    /**
     * return the max batch delete key number in single deleteObjectBatch.
     */
    int getMaxDeleteObjectsNumber();

    class ObjectPath {
        private final short bucket;
        private final String key;

        public ObjectPath(short bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }

        public short bucket() {
            return bucket;
        }

        public String key() {
            return key;
        }
    }

    class ObjectInfo extends ObjectPath {
        private final long timestamp;
        private final long size;

        public ObjectInfo(short bucket, String key, long timestamp, long size) {
            super(bucket, key);
            this.timestamp = timestamp;
            this.size = size;
        }

        public long timestamp() {
            return timestamp;
        }

        public long size() {
            return size;
        }
    }

    class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private int allocType = ByteBufAlloc.DEFAULT;
        private long apiCallAttemptTimeout = -1L;

        public WriteOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public WriteOptions allocType(int allocType) {
            this.allocType = allocType;
            return this;
        }

        public WriteOptions apiCallAttemptTimeout(long apiCallAttemptTimeout) {
            this.apiCallAttemptTimeout = apiCallAttemptTimeout;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public int allocType() {
            return allocType;
        }

        public long apiCallAttemptTimeout() {
            return apiCallAttemptTimeout;
        }

    }

    class ReadOptions {
        public static final ReadOptions DEFAULT = new ReadOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private short bucket = (short) 0;

        public ReadOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public ReadOptions bucket(short bucket) {
            this.bucket = bucket;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public short bucket() {
            return bucket;
        }
    }
}
