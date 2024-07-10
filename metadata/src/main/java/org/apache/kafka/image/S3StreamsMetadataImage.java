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

package org.apache.kafka.image;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.TimelineHashMap;

public final class S3StreamsMetadataImage extends AbstractReferenceCounted {

    public static final S3StreamsMetadataImage EMPTY =
        new S3StreamsMetadataImage(
            -1,
            RegistryRef.NOOP,
            new DeltaMap<>(new int[] {1000, 10000}), new DeltaMap<>(new int[] {1000, 10000}),
            new DeltaMap<>(new int[] {1000, 10000}), new DeltaMap<>(new int[] {1000, 10000}),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0)
        );

    private final long nextAssignedStreamId;

    private final DeltaMap<Long/*streamId*/, S3StreamMetadataImage> streamsMetadata;

    private final DeltaMap<Integer/*nodeId*/, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata;

    // Partition <-> Streams mapping in memory
    // this should be created only once in each image and not be modified
    private final DeltaMap<TopicIdPartition, Set<Long>> partition2streams;
    // this should be created only once in each image and not be modified
    private final DeltaMap<Long, TopicIdPartition> stream2partition;

    private final TimelineHashMap<Long, Long> streamEndOffsets;
    private final RegistryRef registryRef;

    public S3StreamsMetadataImage(
        long assignedStreamId,
        RegistryRef registryRef,
        DeltaMap<Long, S3StreamMetadataImage> streamsMetadata,
        DeltaMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata,
        DeltaMap<TopicIdPartition, Set<Long>> partition2streams,
        DeltaMap<Long, TopicIdPartition> stream2partition,
        TimelineHashMap<Long, Long> streamEndOffsets
    ) {
        this.nextAssignedStreamId = assignedStreamId + 1;
        this.streamsMetadata = streamsMetadata;
        this.nodeStreamSetObjectMetadata = nodeStreamSetObjectMetadata;
        this.partition2streams = partition2streams;
        this.stream2partition = stream2partition;
        this.streamEndOffsets = streamEndOffsets;
        this.registryRef = registryRef;
    }

    boolean isEmpty() {
        return this.nodeStreamSetObjectMetadata.isEmpty() && this.streamsMetadata.isEmpty();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(
            new ApiMessageAndVersion(
                new AssignedStreamIdRecord().setAssignedStreamId(nextAssignedStreamId - 1), (short) 0));
        streamsMetadata.forEach((k, v) -> v.write(writer, options));
        nodeStreamSetObjectMetadata.forEach((k, v) -> v.write(writer, options));
        if (registryRef != RegistryRef.NOOP && options.metadataVersion().autoMQVersion().isHugeClusterSupported()) {
            List<StreamEndOffset> endOffsets = registryRef.inLock(() -> streamEndOffsets.entrySet(registryRef.epoch()).stream()
                .map(e -> new StreamEndOffset(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));
            writer.write(new ApiMessageAndVersion(
                new S3StreamEndOffsetsRecord().setEndOffsets(S3StreamEndOffsetsCodec.encode(endOffsets)),
                (short) 0
            ));
        }
    }

    /**
     * Get objects in range [startOffset, endOffset) with limit.
     *
     * @param streamId    stream id
     * @param startOffset inclusive start offset of the stream
     * @param endOffset   exclusive end offset of the stream.
     *                    NOTE: NOOP_OFFSET means to retrieve as many objects as it's within the limit.
     * @param limit       max number of s3 objects to return
     * @return s3 objects within the range
     */
    public CompletableFuture<InRangeObjects> getObjects(long streamId, long startOffset, long endOffset, int limit,
        RangeGetter rangeGetter) {
        GetObjectsContext ctx = new GetObjectsContext(streamId, startOffset, endOffset, limit, rangeGetter);
        try {
            getObjects0(ctx);
        } catch (Throwable e) {
            ctx.cf.completeExceptionally(e);
        }
        return ctx.cf;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    void getObjects0(GetObjectsContext ctx) {
        long streamId = ctx.streamId;
        long startOffset = ctx.startOffset;
        long endOffset = ctx.endOffset;
        int limit = ctx.limit;
        if (streamId < 0 || limit < 0 || (endOffset != ObjectUtils.NOOP_OFFSET && startOffset > endOffset)) {
            ctx.cf.complete(InRangeObjects.INVALID);
            return;
        }
        S3StreamMetadataImage stream = streamsMetadata.get(streamId);
        if (stream == null || startOffset < stream.startOffset()) {
            ctx.cf.complete(InRangeObjects.INVALID);
            return;
        }
        List<S3ObjectMetadata> objects = new LinkedList<>();

        // floor value < 0 means that all stream objects' ranges are greater than startOffset
        int streamObjectIndex = Math.max(0, stream.floorStreamObjectIndex(startOffset));

        final List<S3StreamObject> streamObjects = stream.getStreamObjects();

        int lastRangeIndex = -1;
        int streamSetObjectIndex = 0;
        fillObjects(ctx, stream, objects, lastRangeIndex, streamObjectIndex, streamObjects, streamSetObjectIndex,
            null, null);
    }

    void fillObjects(GetObjectsContext ctx, S3StreamMetadataImage stream, List<S3ObjectMetadata> objects, int lastRangeIndex,
        int streamObjectIndex, List<S3StreamObject> streamObjects,
        int streamSetObjectIndex, List<S3StreamSetObject> streamSetObjects,
        NodeS3StreamSetObjectMetadataImage node) {
        long nextStartOffset = ctx.startOffset;
        for (; ; ) {
            int roundStartObjectSize = objects.size();

            // try to find consistent stream objects
            for (; streamObjectIndex < streamObjects.size(); streamObjectIndex++) {
                S3StreamObject streamObject = streamObjects.get(streamObjectIndex);
                if (streamObject.startOffset() != nextStartOffset) {
                    //noinspection StatementWithEmptyBody
                    if (objects.isEmpty() && streamObject.startOffset() <= nextStartOffset && streamObject.endOffset() > nextStartOffset) {
                        // it's the first object, we only need the stream object contains the nextStartOffset
                    } else if (streamObject.endOffset() <= nextStartOffset) {
                        // the stream object not match the requirement, move to the next stream object
                        continue;
                    } else {
                        // the streamObject.startOffset() > nextStartOffset
                        break;
                    }
                }
                objects.add(streamObject.toMetadata());
                nextStartOffset = streamObject.endOffset();
                if (objects.size() >= ctx.limit || (ctx.endOffset != ObjectUtils.NOOP_OFFSET && nextStartOffset >= ctx.endOffset)) {
                    ctx.cf.complete(new InRangeObjects(ctx.streamId, objects));
                    return;
                }
            }

            if (streamSetObjects == null) {
                int rangeIndex = stream.getRangeContainsOffset(nextStartOffset);
                // 1. can not find the range containing nextStartOffset, or
                // 2. the range is the same as the last one, which means the nextStartOffset does not move on.
                if (rangeIndex < 0 || lastRangeIndex == rangeIndex) {
                    ctx.cf.complete(new InRangeObjects(ctx.streamId, objects));
                    break;
                }
                lastRangeIndex = rangeIndex;
                RangeMetadata range = stream.getRanges().get(rangeIndex);
                node = nodeStreamSetObjectMetadata.get(range.nodeId());
                if (node != null) {
                    streamSetObjects = node.orderList();
                    streamSetObjectIndex = node.floorStreamSetObjectIndex(ctx.streamId, nextStartOffset);
                } else {
                    streamSetObjects = Collections.emptyList();
                }
                // load stream set object index
                final int finalLastRangeIndex = lastRangeIndex;
                final long finalNextStartOffset = nextStartOffset;
                final int finalStreamObjectIndex = streamObjectIndex;
                final int finalStreamSetObjectIndex = streamSetObjectIndex;
                final List<S3StreamSetObject> finalStreamSetObjects = streamSetObjects;
                final NodeS3StreamSetObjectMetadataImage finalNode = node;
                loadStreamSetObjectInfo(ctx, streamSetObjects, streamSetObjectIndex).thenAccept(v -> {
                    ctx.startOffset = finalNextStartOffset;
                    fillObjects(ctx, stream, objects, finalLastRangeIndex, finalStreamObjectIndex, streamObjects,
                        finalStreamSetObjectIndex, finalStreamSetObjects, finalNode);
                });
                return;
            }

            final int streamSetObjectsSize = streamSetObjects.size();
            for (; streamSetObjectIndex < streamSetObjectsSize; streamSetObjectIndex++) {
                S3StreamSetObject streamSetObject = streamSetObjects.get(streamSetObjectIndex);
                StreamOffsetRange streamOffsetRange = findStreamInStreamSetObject(ctx, streamSetObject).orElse(null);
                // skip the stream set object not containing the stream or the range is before the nextStartOffset
                if (streamOffsetRange == null || streamOffsetRange.endOffset() <= nextStartOffset) {
                    continue;
                }
                if ((streamOffsetRange.startOffset() == nextStartOffset)
                    || (objects.isEmpty() && streamOffsetRange.startOffset() < nextStartOffset)) {
                    if (node != null) {
                        node.recordStreamSetObjectIndex(ctx.streamId, nextStartOffset, streamSetObjectIndex);
                    }
                    objects.add(new S3ObjectMetadata(streamSetObject.objectId(), S3ObjectType.STREAM_SET, List.of(streamOffsetRange),
                        streamSetObject.dataTimeInMs()));
                    nextStartOffset = streamOffsetRange.endOffset();
                    if (objects.size() >= ctx.limit || (ctx.endOffset != ObjectUtils.NOOP_OFFSET && nextStartOffset >= ctx.endOffset)) {
                        ctx.cf.complete(new InRangeObjects(ctx.streamId, objects));
                        return;
                    }
                } else {
                    // We keep the corresponding object ( with a range startOffset > nextStartOffset) by not changing
                    // the streamSetObjectIndex. This object may be picked up in the next round.
                    break;
                }
            }
            // case 1. streamSetObjectIndex >= streamSetObjects.size(), which means we have reached the end of the stream set objects.
            // case 2. objects.size() == roundStartObjectSize, which means we have not found any new object in this round.
            if (streamSetObjectIndex >= streamSetObjects.size() || objects.size() == roundStartObjectSize) {
                // move to the next range
                // This can ensure that we can break the loop.
                streamSetObjects = null;
            }
        }
    }

    /**
     * Load the stream set object range info is missing
     *
     * @return async load
     */
    private CompletableFuture<Void> loadStreamSetObjectInfo(GetObjectsContext ctx, List<S3StreamSetObject> streamSetObjects,
        int startSearchIndex) {
        final int streamSetObjectsSize = streamSetObjects.size();
        List<CompletableFuture<Void>> loadIndexCfList = new LinkedList<>();
        for (int i = startSearchIndex; i < streamSetObjectsSize; i++) {
            S3StreamSetObject streamSetObject = streamSetObjects.get(i);
            if (streamSetObject.ranges().length != 0) {
                continue;
            }
            if (ctx.object2range.containsKey(streamSetObject.objectId())) {
                continue;
            }
            loadIndexCfList.add(
                ctx.rangeGetter
                    .find(streamSetObject.objectId(), ctx.streamId)
                    .thenAccept(range -> ctx.object2range.put(streamSetObject.objectId(), range))
            );
        }
        if (loadIndexCfList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(loadIndexCfList.toArray(new CompletableFuture[0]))
            .exceptionally(ex -> {
                ctx.cf.completeExceptionally(ex);
                return null;
            });
    }

    private Optional<StreamOffsetRange> findStreamInStreamSetObject(GetObjectsContext ctx, S3StreamSetObject object) {
        if (object.ranges().length == 0) {
            return ctx.object2range.get(object.objectId());
        } else {
            return object.find(ctx.streamId);
        }
    }

    /**
     * Get stream objects in range [startOffset, endOffset) with limit. It will throw IllegalArgumentException if limit or streamId is invalid.
     *
     * @param streamId    stream id
     * @param startOffset inclusive start offset of the stream
     * @param endOffset   exclusive end offset of the stream
     * @param limit       max number of stream objects to return
     * @return stream objects
     */
    public List<S3StreamObject> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException(String.format("limit %d is invalid", limit));
        }
        S3StreamMetadataImage stream = streamsMetadata.get(streamId);
        if (stream == null) {
            throw new IllegalArgumentException(String.format("stream %d not found", streamId));
        }
        List<S3StreamObject> streamObjectsMetadata = stream.getStreamObjects();
        if (streamObjectsMetadata == null || streamObjectsMetadata.isEmpty()) {
            return Collections.emptyList();
        }
        return streamObjectsMetadata.stream().filter(obj -> {
            long objectStartOffset = obj.streamOffsetRange().startOffset();
            long objectEndOffset = obj.streamOffsetRange().endOffset();
            return objectStartOffset < endOffset && objectEndOffset > startOffset;
        }).sorted(Comparator.comparing(S3StreamObject::streamOffsetRange)).limit(limit).collect(Collectors.toCollection(ArrayList::new));
    }

    public List<S3StreamSetObject> getStreamSetObjects(int nodeId) {
        NodeS3StreamSetObjectMetadataImage wal = nodeStreamSetObjectMetadata.get(nodeId);
        if (wal == null) {
            return Collections.emptyList();
        }
        return wal.orderList();
    }

    public S3StreamMetadataImage getStreamMetadata(long streamId) {
        return streamsMetadata.get(streamId);
    }

    public Set<Long> getTopicPartitionStreams(Uuid topicId, int partition) {
        return partition2streams.getOrDefault(new TopicIdPartition(topicId, partition), Collections.emptySet());
    }

    public TopicIdPartition getStreamTopicPartition(long streamId) {
        return stream2partition.get(streamId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        S3StreamsMetadataImage other = (S3StreamsMetadataImage) obj;
        return this.nextAssignedStreamId == other.nextAssignedStreamId
            && this.streamsMetadata.equals(other.streamsMetadata)
            && this.nodeStreamSetObjectMetadata.equals(other.nodeStreamSetObjectMetadata)
            && this.streamEndOffsets().equals(other.streamEndOffsets());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedStreamId, streamsMetadata, nodeStreamSetObjectMetadata, streamEndOffsets());
    }

    public DeltaMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeWALMetadata() {
        return nodeStreamSetObjectMetadata;
    }

    public DeltaMap<Long, S3StreamMetadataImage> streamsMetadata() {
        return streamsMetadata;
    }

    public long nextAssignedStreamId() {
        return nextAssignedStreamId;
    }

    DeltaMap<TopicIdPartition, Set<Long>> partition2streams() {
        return partition2streams;
    }

    DeltaMap<Long, TopicIdPartition> stream2partition() {
        return stream2partition;
    }

    RegistryRef registryRef() {
        return registryRef;
    }

    // caller use this value should be protected by registryRef lock
    TimelineHashMap<Long, Long> timelineStreamEndOffsets() {
        return streamEndOffsets;
    }

    Map<Long, Long> streamEndOffsets() {
        if (registryRef == RegistryRef.NOOP) {
            return Collections.emptyMap();
        }

        return registryRef.inLock(() -> {
            Map<Long, Long> map = new HashMap<>();
            streamEndOffsets.entrySet(registryRef.epoch()).forEach(e -> map.put(e.getKey(), e.getValue()));
            return map;
        });
    }

    @Override
    public String toString() {
        return "S3StreamsMetadataImage{nextAssignedStreamId=" + nextAssignedStreamId + '}';
    }

    @Override
    protected void deallocate() {
        if (registryRef == RegistryRef.NOOP) {
            return;
        }
        registryRef.release();
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }

    static class GetObjectsContext {
        long streamId;
        long startOffset;
        long endOffset;
        int limit;
        RangeGetter rangeGetter;

        CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
        Map<Long, Optional<StreamOffsetRange>> object2range = new HashMap<>();

        GetObjectsContext(long streamId, long startOffset, long endOffset, int limit,
            RangeGetter rangeGetter) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.limit = limit;
            this.rangeGetter = rangeGetter;
        }
    }

    public interface RangeGetter {
        CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId);
    }

}
