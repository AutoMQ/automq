/*
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

package com.automq.stream.s3.memory;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.CommitStreamObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.CommitWALObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class MemoryMetadataManager implements StreamManager, ObjectManager {
    private final AtomicLong objectIdAlloc = new AtomicLong();
    private final Map<Long, List<S3ObjectMetadata>> streamObjects = new HashMap<>();
    private final Map<Long, S3ObjectMetadata> walObjects = new HashMap<>();

    @Override
    public synchronized CompletableFuture<Long> prepareObject(int count, long ttl) {
        return CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement());
    }

    @Override
    public synchronized CompletableFuture<CommitWALObjectResponse> commitWALObject(CommitWALObjectRequest request) {
        long dataTimeInMs = System.currentTimeMillis();
        if (!request.getCompactedObjectIds().isEmpty()) {
            for (long id : request.getCompactedObjectIds()) {
                dataTimeInMs = Math.min(walObjects.get(id).dataTimeInMs(), dataTimeInMs);
                walObjects.remove(id);
            }
        }
        long now = System.currentTimeMillis();
        if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
            S3ObjectMetadata object = new S3ObjectMetadata(
                    request.getObjectId(), S3ObjectType.WAL, request.getStreamRanges().stream().map(MemoryMetadataManager::to).collect(Collectors.toList()),
                    dataTimeInMs, now, request.getObjectSize(), request.getOrderId());
            walObjects.put(request.getObjectId(), object);
        }
        for (StreamObject r : request.getStreamObjects()) {
            List<S3ObjectMetadata> objects = streamObjects.computeIfAbsent(r.getStreamId(), id -> new LinkedList<>());
            objects.add(
                    new S3ObjectMetadata(
                            r.getObjectId(), S3ObjectType.STREAM, List.of(new StreamOffsetRange(r.getStreamId(), r.getStartOffset(), r.getEndOffset())),
                            dataTimeInMs, now, r.getObjectSize(), 0
                    )
            );
        }
        request.getCompactedObjectIds().forEach(walObjects::remove);
        return CompletableFuture.completedFuture(new CommitWALObjectResponse());
    }

    @Override
    public synchronized CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        return CompletableFuture.completedFuture(new LinkedList<>(walObjects.values()));
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public synchronized CompletableFuture<Long> createStream() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> closeStream(long streamId, long epoch) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public synchronized CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    private static StreamOffsetRange to(ObjectStreamRange s) {
        return new StreamOffsetRange(s.getStreamId(), s.getStartOffset(), s.getEndOffset());
    }
}
