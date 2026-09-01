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
import java.util.Objects;
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

    /**
     * Copy one complete object inside object storage without transferring its payload through the Broker.
     *
     * @param sourceBucket source bucket name
     * @param sourcePath source object key
     * @param destinationPath destination object key
     * @return copy completion
     */
    default CompletableFuture<Void> copy(String sourceBucket, String sourcePath, String destinationPath) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    // Low level API
    default CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf) {
        Writer writer = writer(options, objectPath);
        writer.write(buf);
        return writer.close().thenApply(nil -> new WriteResult(writer.bucketId()));
    }

    /**
     * Start a multipart upload for the target object path.
     * <p>
     * Implementations that do not support low-level multipart operations fail with
     * {@link UnsupportedOperationException}.
     */
    default CompletableFuture<String> createMultipartUpload(WriteOptions options, String path) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    /**
     * Upload one multipart part to the target object path.
     * <p>
     * The implementation owns {@code data} after this call and is responsible for releasing it when the returned future
     * completes. Implementations that do not support low-level multipart operations fail with
     * {@link UnsupportedOperationException}.
     */
    default CompletableFuture<ObjectStorageCompletedPart> uploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf data) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    /**
     * Copy one source range into a multipart part for the target object path.
     * <p>
     * This low-level operation is same-storage copy. Cross-bucket routing should fall back to range read plus
     * {@link #uploadPart(WriteOptions, String, String, int, ByteBuf)} before calling this method.
     */
    default CompletableFuture<ObjectStorageCompletedPart> uploadPartCopy(WriteOptions options, String sourcePath,
        String path, long start, long end, String uploadId, int partNumber) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    /**
     * Complete a multipart upload for the target object path using the completed parts in part-number order.
     */
    default CompletableFuture<Void> completeMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException());
    }

    /**
     * List objects under a prefix in lexicographic key order.
     *
     * @param options prefix, exclusive cursor, and result bound
     * @return one logical ordered result; provider pagination is not exposed
     */
    CompletableFuture<List<ObjectInfo>> list(ListOptions options);

    /**
     * List every object under a prefix.
     *
     * @param prefix required object-key prefix
     * @return every matching object in lexicographic key order
     */
    default CompletableFuture<List<ObjectInfo>> list(String prefix) {
        return list(new ListOptions(prefix));
    }

    /**
     * The deleteObjects API have max batch limit.
     * see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html"/>
     * Implementation should handle the objectPaths size exceeded limit condition.
     * When batch split logic is triggered the CompletableFuture means all the deleteBatch if success.
     * The caller may do the batch split logic if the delete operation need fine-grained control
     */
    CompletableFuture<Void> delete(List<ObjectPath> objectPaths);

    short bucketId();

    /**
     * Resolve the physical bucket URI for a bucket identity.
     */
    BucketURI bucketURI(short bucketId);

    /**
     * Return the stable concrete storage used for Archive objects.
     */
    default ObjectStorage primary() {
        return this;
    }

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

    class ObjectStorageCompletedPart {
        private final int partNumber;
        private final String partId;
        private final String checkSum;

        public ObjectStorageCompletedPart(int partNumber, String partId, String checkSum) {
            this.partNumber = partNumber;
            this.partId = partId;
            this.checkSum = checkSum;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getPartId() {
            return partId;
        }

        public String getCheckSum() {
            return checkSum;
        }
    }

    /**
     * Options for one logical ordered object listing. Provider page state is intentionally not represented here.
     */
    class ListOptions {
        public static final int UNLIMITED = -1;

        private final String prefix;
        private String startAfter;
        private int maxKeys = UNLIMITED;

        /**
         * Create options for a required key prefix with unlimited results.
         *
         * @param prefix required object-key prefix
         */
        public ListOptions(String prefix) {
            this.prefix = Objects.requireNonNull(prefix, "prefix");
        }

        /**
         * Set the exclusive key cursor.
         *
         * @param startAfter exclusive key cursor, or null to start at the prefix beginning
         * @return these options
         */
        public ListOptions startAfter(String startAfter) {
            this.startAfter = startAfter;
            return this;
        }

        /**
         * Set the result bound.
         *
         * @param maxKeys -1 for unlimited results, zero for none, or a positive bound
         * @return these options
         */
        public ListOptions maxKeys(int maxKeys) {
            if (maxKeys < UNLIMITED) {
                throw new IllegalArgumentException("maxKeys must be -1 or non-negative");
            }
            this.maxKeys = maxKeys;
            return this;
        }

        /**
         * Return the required object-key prefix.
         */
        public String prefix() {
            return prefix;
        }

        /**
         * Return the exclusive key cursor, or null when unset.
         */
        public String startAfter() {
            return startAfter;
        }

        /**
         * Return -1 for unlimited results, zero for none, or a positive result bound.
         */
        public int maxKeys() {
            return maxKeys;
        }
    }

    class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions();
        public static final short UNSET_BUCKET = (short) -2;

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private int allocType = ByteBufAlloc.DEFAULT;
        // timeout for one single network rpc
        private long apiCallAttemptTimeout = -1L;
        // timeout for the whole write operation
        private long timeout = Long.MAX_VALUE;
        private short bucketId = UNSET_BUCKET;
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

        // Writer will set the value.
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
