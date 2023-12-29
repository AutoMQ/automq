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
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectResponse;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.streams.StreamManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class MemoryMetadataManager implements StreamManager, ObjectManager {
    private final static AtomicLong NODE_ID_ALLOC = new AtomicLong();

    // Data structure of stream metadata
    private final AtomicLong streamIdAlloc = new AtomicLong();
    private final ConcurrentMap<Long, StreamMetadata> streams = new ConcurrentHashMap<>();

    // Data structure of object metadata
    private final AtomicLong objectIdAlloc = new AtomicLong();
    private final ConcurrentMap<Long, List<S3ObjectMetadata>> streamObjects = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Pair<Long, S3ObjectMetadata>> streamSetObjects = new ConcurrentHashMap<>();

    public static void advanceNodeId() {
        NODE_ID_ALLOC.getAndIncrement();
    }

    @Override
    public synchronized CompletableFuture<Long> prepareObject(int count, long ttl) {
        return CompletableFuture.completedFuture(objectIdAlloc.getAndAdd(count));
    }

    private static StreamOffsetRange to(ObjectStreamRange s) {
        return new StreamOffsetRange(s.getStreamId(), s.getStartOffset(), s.getEndOffset());
    }

    @Override
    public synchronized CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject(
        CommitStreamSetObjectRequest request) {
        long dataTimeInMs = System.currentTimeMillis();
        if (!request.getCompactedObjectIds().isEmpty()) {
            for (long id : request.getCompactedObjectIds()) {
                dataTimeInMs = Math.min(streamSetObjects.get(id).getRight().dataTimeInMs(), dataTimeInMs);
                streamSetObjects.remove(id);
            }
        }
        long now = System.currentTimeMillis();
        if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
            for (ObjectStreamRange range : request.getStreamRanges()) {
                StreamMetadata stream = streams.get(range.getStreamId());
                assert stream != null;
                if (request.getCompactedObjectIds().isEmpty()) {
                    // Commit new object.
                    if (stream.endOffset() != range.getStartOffset()) {
                        throw new IllegalArgumentException("stream " + range.getStreamId() + " end offset " + stream.endOffset() + " is not equal to start offset of request " + range.getStartOffset());
                    }
                    stream.endOffset(range.getEndOffset());
                } else {
                    // Compact old object.
                    if (stream.endOffset() < range.getEndOffset()) {
                        throw new IllegalArgumentException("stream " + range.getStreamId() + " end offset " + stream.endOffset() + " is lesser than request " + range.getEndOffset());
                    }
                    if (stream.startOffset() > range.getStartOffset()) {
                        throw new IllegalArgumentException("stream " + range.getStreamId() + " start offset " + stream.startOffset() + " is greater than request " + range.getStartOffset());
                    }
                }
            }

            S3ObjectMetadata object = new S3ObjectMetadata(
                request.getObjectId(), S3ObjectType.STREAM_SET, request.getStreamRanges().stream().map(MemoryMetadataManager::to).collect(Collectors.toList()),
                dataTimeInMs, now, request.getObjectSize(), request.getOrderId());
            streamSetObjects.put(request.getObjectId(), Pair.of(NODE_ID_ALLOC.get(), object));
        }

        for (StreamObject streamObject : request.getStreamObjects()) {
            long streamId = streamObject.getStreamId();
            StreamMetadata stream = streams.get(streamId);
            assert stream != null;
            if (request.getCompactedObjectIds().isEmpty()) {
                // Commit new object.
                if (stream.endOffset() != streamObject.getStartOffset()) {
                    throw new IllegalArgumentException("stream " + streamObject.getStreamId() + " end offset " + stream.endOffset() + " is not equal to start offset of request " + streamObject.getStartOffset());
                }
                stream.endOffset(streamObject.getEndOffset());
            } else {
                // Compact old object.
                if (stream.endOffset() < streamObject.getEndOffset()) {
                    throw new IllegalArgumentException("stream " + streamObject.getStreamId() + " end offset " + stream.endOffset() + " is lesser than request " + streamObject.getEndOffset());
                }
                if (stream.startOffset() > streamObject.getStartOffset()) {
                    throw new IllegalArgumentException("stream " + streamObject.getStreamId() + " start offset " + stream.startOffset() + " is greater than request " + streamObject.getStartOffset());
                }
            }

            List<S3ObjectMetadata> metadataList = streamObjects.computeIfAbsent(streamId, id -> new LinkedList<>());
            metadataList.add(
                new S3ObjectMetadata(
                    streamObject.getObjectId(), S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, streamObject.getStartOffset(), streamObject.getEndOffset())),
                    dataTimeInMs, now, streamObject.getObjectSize(), 0
                )
            );
        }
        request.getCompactedObjectIds().forEach(streamSetObjects::remove);
        return CompletableFuture.completedFuture(new CommitStreamSetObjectResponse());
    }

    @Override
    public synchronized CompletableFuture<Void> compactStreamObject(CompactStreamObjectRequest request) {
        long streamId = request.getStreamId();
        StreamMetadata stream = streams.get(streamId);
        assert stream != null;
        if (stream.endOffset() < request.getEndOffset()) {
            throw new IllegalArgumentException("stream " + streamId + " end offset " + stream.endOffset() + " is lesser than request " + request.getEndOffset());
        }
        if (stream.startOffset() > request.getStartOffset()) {
            throw new IllegalArgumentException("stream " + streamId + " start offset " + stream.startOffset() + " is greater than request " + request.getStartOffset());
        }

        streamObjects.computeIfAbsent(streamId, id -> new LinkedList<>())
            .add(new S3ObjectMetadata(
                request.getObjectId(), S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, request.getStartOffset(), request.getEndOffset())),
                System.currentTimeMillis(), System.currentTimeMillis(), request.getObjectSize(), 0
            ));

        HashSet<Long> idSet = new HashSet<>(request.getSourceObjectIds());
        streamObjects.get(streamId).removeIf(metadata -> idSet.contains(metadata.objectId()));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        List<S3ObjectMetadata> streamSetObjectList = streamSetObjects.values()
            .stream()
            .map(Pair::getRight)
            .filter(o -> o.getOffsetRanges().stream().anyMatch(r -> r.getStreamId() == streamId && r.getEndOffset() > startOffset && (r.getStartOffset() < endOffset || endOffset == -1)))
            .toList();
        List<S3ObjectMetadata> streamObjectList = streamObjects.computeIfAbsent(streamId, id -> new LinkedList<>())
            .stream()
            .filter(o -> o.getOffsetRanges().stream().anyMatch(r -> r.getStreamId() == streamId && r.getEndOffset() > startOffset && (r.getStartOffset() < endOffset || endOffset == -1)))
            .toList();

        List<S3ObjectMetadata> result = new ArrayList<>();
        result.addAll(streamSetObjectList);
        result.addAll(streamObjectList);
        result.sort((o1, o2) -> {
            long startOffset1 = o1.getOffsetRanges().stream().filter(r -> r.getStreamId() == streamId).findFirst().get().getStartOffset();
            long startOffset2 = o2.getOffsetRanges().stream().filter(r -> r.getStreamId() == streamId).findFirst().get().getStartOffset();
            return Long.compare(startOffset1, startOffset2);
        });

        return CompletableFuture.completedFuture(result.stream().limit(limit).toList());
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        List<S3ObjectMetadata> result = streamSetObjects.values()
            .stream()
            .filter(pair -> pair.getLeft() == NODE_ID_ALLOC.get())
            .map(Pair::getRight).toList();
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public synchronized CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        List<S3ObjectMetadata> streamObjectList = streamObjects.computeIfAbsent(streamId, id -> new LinkedList<>())
            .stream()
            .filter(o -> o.getOffsetRanges().stream().anyMatch(r -> r.getStreamId() == streamId && r.getEndOffset() > startOffset && (r.getStartOffset() < endOffset || endOffset == -1)))
            .limit(limit)
            .toList();
        return CompletableFuture.completedFuture(streamObjectList);
    }

    @Override
    public synchronized CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        return CompletableFuture.completedFuture(streams.values().stream().filter(stream -> stream.state() == StreamState.OPENED).toList());
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        return CompletableFuture.completedFuture(streamIds.stream().map(streams::get).filter(Objects::nonNull).toList());
    }

    @Override
    public synchronized CompletableFuture<Long> createStream() {
        long streamId = streamIdAlloc.getAndIncrement();
        streams.put(streamId, new StreamMetadata(streamId, -1, 0, 0, StreamState.CLOSED));
        return CompletableFuture.completedFuture(streamId);
    }

    @Override
    public synchronized CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        StreamMetadata stream = streams.get(streamId);
        if (stream == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " not found"));
        }
        if (stream.state() == StreamState.OPENED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " has been opened"));
        }
        if (stream.epoch() >= epoch) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " epoch " + epoch + " is not newer than current epoch " + stream.epoch()));
        }
        stream.epoch(epoch);
        stream.state(StreamState.OPENED);
        return CompletableFuture.completedFuture(stream);
    }

    @Override
    public synchronized CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        StreamMetadata stream = streams.get(streamId);
        if (stream == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " not found"));
        }
        if (stream.state() != StreamState.OPENED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " is not opened"));
        }
        if (stream.epoch() != epoch) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch()));
        }
        if (newStartOffset < stream.startOffset()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " new start offset " + newStartOffset + " is less than current start offset " + stream.startOffset()));
        }
        if (newStartOffset > stream.endOffset()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " new start offset " + newStartOffset + " is greater than current end offset " + stream.endOffset()));
        }
        stream.startOffset(newStartOffset);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> closeStream(long streamId, long epoch) {
        StreamMetadata stream = streams.get(streamId);
        if (stream == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " not found"));
        }
        if (stream.state() != StreamState.OPENED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " is not opened"));
        }
        if (stream.epoch() != epoch) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch()));
        }
        stream.state(StreamState.CLOSED);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        StreamMetadata stream = streams.get(streamId);
        if (stream == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " not found"));
        }
        if (stream.state() != StreamState.CLOSED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " is not closed"));
        }
        if (stream.epoch() != epoch) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("stream " + streamId + " epoch " + epoch + " is not equal to current epoch " + stream.epoch()));
        }
        streams.remove(streamId);
        return CompletableFuture.completedFuture(null);
    }
}
