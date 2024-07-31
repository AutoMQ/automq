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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectStorage {
    long RANGE_READ_TO_END = -1L;

    void close();

    /**
     * Get {@link Writer} for the object.
     */
    Writer writer(WriteOptions options, String objectPath);

    /**
     * Read object from the object storage.
     * It will throw {@link ObjectNotFoundException} if the object not found.
     */
    default CompletableFuture<ByteBuf> read(ReadOptions options, String objectPath) {
        return rangeRead(options, objectPath, 0, RANGE_READ_TO_END);
    }

    /**
     * Range read object from the object storage.
     * It will throw {@link ObjectNotFoundException} if the object not found.
     */
    CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end);

    // Low level API
    CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf);

    CompletableFuture<List<ObjectInfo>> list(String prefix);

    // NOTE: this is a temporary method to get bucketId for direct read with object storage interface
    short bucketId();

    /**
     * The deleteObjects API have max batch limit.
     * see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html"/>
     * Implementation should handle the objectPaths size exceeded limit condition.
     * When batch split logic is triggered the CompletableFuture means all the deleteBatch if success.
     * The caller may do the batch split logic if the delete operation need fine-grained control
     */
    CompletableFuture<Void> delete(List<ObjectPath> objectPaths);

    class ObjectPath {
        private final short bucketId;
        private final String key;

        public ObjectPath(short bucketId, String key) {
            this.bucketId = bucketId;
            this.key = key;
        }

        public short bucketId() {
            return bucketId;
        }

        public String key() {
            return key;
        }

        @Override
        public String toString() {
            return "ObjectPath{" +
                "bucketId=" + bucketId +
                ", key='" + key + '\'' +
                '}';
        }
    }

    class ObjectInfo extends ObjectPath {
        private final long timestamp;
        private final long size;

        public ObjectInfo(short bucketId, String key, long timestamp, long size) {
            super(bucketId, key);
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
        private short bucketId;
        private boolean enableFastRetry;
        private boolean retry;

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

        public WriteOptions enableFastRetry(boolean enableFastRetry) {
            this.enableFastRetry = enableFastRetry;
            return this;
        }

        public WriteOptions retry(boolean retry) {
            this.retry = retry;
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

        // The value will be set by writer
        WriteOptions bucketId(short bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public short bucketId() {
            return bucketId;
        }

        public boolean enableFastRetry() {
            return enableFastRetry;
        }

        public boolean retry() {
            return retry;
        }

        public WriteOptions copy() {
            WriteOptions copy = new WriteOptions();
            copy.throttleStrategy = throttleStrategy;
            copy.allocType = allocType;
            copy.apiCallAttemptTimeout = apiCallAttemptTimeout;
            copy.bucketId = bucketId;
            copy.enableFastRetry = enableFastRetry;
            copy.retry = retry;
            return copy;
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

    class WriteResult {
        private final short bucket;

        public WriteResult(short bucket) {
            this.bucket = bucket;
        }

        public short bucket() {
            return bucket;
        }
    }

    class ObjectNotFoundException extends Exception {
        public ObjectNotFoundException(Throwable cause) {
            super(cause);
        }
    }
}
