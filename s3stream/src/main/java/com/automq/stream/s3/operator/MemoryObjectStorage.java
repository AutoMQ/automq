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
import com.automq.stream.s3.exceptions.ObjectStorageConditionNotMetException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.test.RecordTestNetworkBandwidthLimiter;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class MemoryObjectStorage extends AbstractObjectStorage {
    private final Map<String, StoredObject> storage = new ConcurrentHashMap<>();
    private final Set<String> deleteObjectKeys = new ConcurrentSkipListSet<>();
    private final AtomicLong etag = new AtomicLong();
    private long delay = 0;
    private final short bucketId;

    public MemoryObjectStorage(boolean manualMergeRead, short bucketId) {
        super(BucketURI.parse(bucketId + "@s3://b"),
            new RecordTestNetworkBandwidthLimiter(), new RecordTestNetworkBandwidthLimiter(),
            50, 0, true, false, manualMergeRead, "memory");
        this.bucketId = bucketId;
    }

    public MemoryObjectStorage(int concurrencyCount) {
        super(BucketURI.parse(0 + "@s3://b"),
            new RecordTestNetworkBandwidthLimiter(), new RecordTestNetworkBandwidthLimiter(),
            concurrencyCount, 0, true, false, false, "memory");
        this.bucketId = 0;
    }

    public MemoryObjectStorage(short bucketId) {
        this(false, bucketId);
    }

    public MemoryObjectStorage() {
        this(false, (short) 0);
    }

    public MemoryObjectStorage(boolean manualMergeRead) {
        this(manualMergeRead, (short) 0);
    }

    @Override
    CompletableFuture<ReadResult> doRangeRead(ReadOptions options, String path, long start, long end) {
        StoredObject object = storage.get(path);
        if (object == null) {
            return FutureUtil.failedFuture(new ObjectNotExistException("object not exist"));
        }
        ByteBuf value = object.data;
        int length = end != -1L ? (int) (end - start) : (int) (value.readableBytes() - start);
        ByteBuf rst = value.retainedSlice(value.readerIndex() + (int) start, length);
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        buf.addComponent(true, rst);
        ReadResult result = ReadResult.of(buf, ObjectMetadata.of(new Etag(Long.toString(object.etag))));
        if (delay == 0) {
            return CompletableFuture.completedFuture(result);
        } else {
            CompletableFuture<ReadResult> cf = new CompletableFuture<>();
            Threads.COMMON_SCHEDULER.schedule(() -> cf.complete(result), delay, TimeUnit.MILLISECONDS);
            return cf;
        }
    }

    @Override
    CompletableFuture<Void> doWrite(WriteOptions options, String path, ByteBuf data, WriteCondition condition) {
        if (data == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("data to write cannot be null"));
        }
        ByteBuf buf = Unpooled.buffer(data.readableBytes());
        buf.writeBytes(data.duplicate());
        AtomicReference<ObjectStorageConditionNotMetException> conditionFailure = new AtomicReference<>();
        storage.compute(path, (key, current) -> {
            if (condition instanceof WriteCondition.IfMatch ifMatch) {
                if (current == null || !Long.toString(current.etag).equals(ifMatch.etag().value())) {
                    conditionFailure.set(new ObjectStorageConditionNotMetException("object etag does not match"));
                    return current;
                }
            } else if (condition instanceof WriteCondition.IfAbsent) {
                if (current != null) {
                    conditionFailure.set(new ObjectStorageConditionNotMetException("object already exists"));
                    return current;
                }
            }
            if (current != null) {
                current.data.release();
            }
            return new StoredObject(buf, etag.incrementAndGet());
        });
        ObjectStorageConditionNotMetException failure = conditionFailure.get();
        if (failure != null) {
            buf.release();
            return FutureUtil.failedFuture(failure);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean readinessCheck() {
        return true;
    }

    @Override
    public Writer writer(WriteOptions writeOptions, String path) {
        ByteBuf buf = Unpooled.buffer();
        StoredObject old = storage.put(path, new StoredObject(buf, etag.incrementAndGet()));
        if (old != null) {
            old.data.release();
        }
        return new Writer() {
            @Override
            public CompletableFuture<Void> write(ByteBuf part) {
                buf.writeBytes(part);
                // Release the part after write
                part.release();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void copyOnWrite() {

            }

            @Override
            public boolean hasBatchingPart() {
                return false;
            }

            @Override
            public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
                StoredObject sourceObject = storage.get(s3ObjectMetadata.key());
                if (sourceObject == null) {
                    throw new IllegalArgumentException("object not exist");
                }
                ByteBuf source = sourceObject.data;
                buf.writeBytes(source.slice(source.readerIndex() + (int) start, (int) (end - start)));
            }

            @Override
            public CompletableFuture<Void> close() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> release() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public short bucketId() {
                return bucketId;
            }
        };
    }

    @Override
    CompletableFuture<String> doCreateMultipartUpload(WriteOptions options, String path) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPart(WriteOptions options, String path, String uploadId,
        int partNumber, ByteBuf part) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<ObjectStorageCompletedPart> doUploadPartCopy(WriteOptions options, String sourcePath, String path,
        long start, long end, String uploadId, int partNumber) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<Void> doCompleteMultipartUpload(WriteOptions options, String path, String uploadId,
        List<ObjectStorageCompletedPart> parts) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    CompletableFuture<Void> doDeleteObjects(List<String> objectKeys) {
        for (String objectKey : objectKeys) {
            StoredObject object = storage.remove(objectKey);
            if (object != null) {
                object.data.release();
                deleteObjectKeys.add(objectKey);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    public Set<String> getDeleteObjectKeys() {
        return this.deleteObjectKeys;
    }

    @Override
    Pair<RetryStrategy, Throwable> toRetryStrategyAndCause(Throwable ex, S3Operation operation) {
        Throwable cause = FutureUtil.cause(ex);
        RetryStrategy strategy = (cause instanceof UnsupportedOperationException || cause instanceof IllegalArgumentException
            || cause instanceof ObjectNotExistException || cause instanceof ObjectStorageConditionNotMetException)
            ? RetryStrategy.ABORT : RetryStrategy.RETRY;
        return Pair.of(strategy, cause);
    }

    @Override
    void doClose() {
        storage.values().forEach(object -> object.data.release());
        storage.clear();
    }

    @Override
    CompletableFuture<List<ObjectInfo>> doList(String prefix) {
        return CompletableFuture.completedFuture(storage.entrySet()
            .stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .map(entry -> new ObjectInfo((short) 0, entry.getKey(), 0L, entry.getValue().data.readableBytes()))
            .collect(Collectors.toList()));
    }

    @Override
    protected <T> boolean bucketCheck(int bucketId, CompletableFuture<T> cf) {
        return true;
    }

    public ByteBuf get() {
        if (storage.size() != 1) {
            throw new IllegalStateException("expect only one object in storage");
        }
        return storage.values().iterator().next().data;
    }

    public boolean contains(String path) {
        return storage.containsKey(path);
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public NetworkBandwidthLimiter getNetworkInboundBandwidthLimiter() {
        return this.networkInboundBandwidthLimiter;
    }

    public NetworkBandwidthLimiter getNetworkOutboundBandwidthLimiter() {
        return this.networkOutboundBandwidthLimiter;
    }

    private static class StoredObject {
        private final ByteBuf data;
        private final long etag;

        private StoredObject(ByteBuf data, long etag) {
            this.data = data;
            this.etag = etag;
        }
    }
}
