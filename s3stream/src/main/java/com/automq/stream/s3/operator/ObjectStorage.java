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
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.network.ThrottleStrategy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

public interface ObjectStorage {
    long RANGE_READ_TO_END = -1L;

    /**
     * Check whether the object storage is available.
     * @return available or not
     */
    boolean readinessCheck();

    void close();

    /**
     * Get {@link Writer} for the object.
     */
    Writer writer(WriteOptions options, String objectPath);

    /**
     * Read object from the object storage.
     * It will throw {@link ObjectNotExistException} if the object not found.
     */
    default CompletableFuture<ByteBuf> read(ReadOptions options, String objectPath) {
        return rangeRead(options, objectPath, 0, RANGE_READ_TO_END);
    }

    /**
     * Range read object from the object storage.
     * It will failFuture with {@link ObjectNotExistException} if the object not found.
     * @param options {@link ReadOptions}
     * @param objectPath the object path
     * @param start inclusive start position
     * @param end exclusive end position
     * @return read result
     */
    CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end);

    // Low level API
    default CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf) {
        Writer writer = writer(options, objectPath);
        writer.write(buf);
        return writer.close().thenApply(nil -> new WriteResult(bucketId()));
    }

    CompletableFuture<List<ObjectInfo>> list(String prefix);

    /**
     * The deleteObjects API have max batch limit.
     * see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html"/>
     * Implementation should handle the objectPaths size exceeded limit condition.
     * When batch split logic is triggered the CompletableFuture means all the deleteBatch if success.
     * The caller may do the batch split logic if the delete operation need fine-grained control
     */
    CompletableFuture<Void> delete(List<ObjectPath> objectPaths);

    short bucketId();

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
        // timeout for one single network rpc
        private long apiCallAttemptTimeout = -1L;
        // timeout for the whole write operation
        private long timeout = Long.MAX_VALUE;
        private short bucketId;
        private boolean enableFastRetry;
        // write context start
        private boolean retry;
        private int retryCount;
        private long requestTime = System.nanoTime();
        // write context end

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

        // If enable the fast retry, the data buffer may be released after the write future is completed.
        // Be careful to use this option, ensure that you reuse the data buffer only after
        // it has been released by the writer.
        public WriteOptions enableFastRetry(boolean enableFastRetry) {
            this.enableFastRetry = enableFastRetry;
            return this;
        }

        public WriteOptions retry(boolean retry) {
            this.retry = retry;
            return this;
        }

        public WriteOptions requestTime(long requestTime) {
            this.requestTime = requestTime;
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

        // Writer will set the value
        WriteOptions bucketId(short bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public short bucketId() {
            return bucketId;
        }

        public WriteOptions timeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public boolean enableFastRetry() {
            return enableFastRetry;
        }

        public boolean retry() {
            return retry;
        }

        public int retryCountGetAndAdd() {
            int oldRetryCount = this.retryCount;
            this.retryCount = retryCount + 1;
            return oldRetryCount;
        }

        public int retryCount() {
            return retryCount;
        }

        public long requestTime() {
            return requestTime;
        }

        public long timeout() {
            return timeout;
        }

        public WriteOptions copy() {
            WriteOptions copy = new WriteOptions();
            copy.throttleStrategy = throttleStrategy;
            copy.allocType = allocType;
            copy.apiCallAttemptTimeout = apiCallAttemptTimeout;
            copy.bucketId = bucketId;
            copy.enableFastRetry = enableFastRetry;
            copy.retry = retry;
            copy.retryCount = retryCount;
            copy.requestTime = requestTime;
            copy.timeout = timeout;
            return copy;
        }
    }

    class ReadOptions {
        public static final short UNSET_BUCKET = (short) -2;

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private short bucket = UNSET_BUCKET;
        private int retryCount;

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

        public int retryCountGetAndAdd() {
            int oldRetryCount = this.retryCount;
            this.retryCount = retryCount + 1;
            return oldRetryCount;
        }

        public int retryCount() {
            return retryCount;
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
}
