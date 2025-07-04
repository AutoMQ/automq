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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.index.NodeRangeIndexCache;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.MetadataStats;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import static com.automq.stream.utils.FutureUtil.exec;

public final class S3StreamsMetadataImage extends AbstractReferenceCounted {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamsMetadataImage.class);

    public static final S3StreamsMetadataImage EMPTY =
        new S3StreamsMetadataImage(
            -1,
            RegistryRef.NOOP,
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0)
        );

    private final long nextAssignedStreamId;

    private final TimelineHashMap<Long/*streamId*/, S3StreamMetadataImage> streamMetadataMap;
    private final TimelineHashMap<Integer/*nodeId*/, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap;

    // Partition <-> Streams mapping in memory
    // this should be created only once in each image and not be modified
    private final TimelineHashMap<TopicIdPartition, Set<Long>> partition2streams;
    // this should be created only once in each image and not be modified
    private final TimelineHashMap<Long, TopicIdPartition> stream2partition;

    private final TimelineHashMap<Long, Long> streamEndOffsets;
    private final RegistryRef registryRef;

    public S3StreamsMetadataImage(
        long assignedStreamId,
        RegistryRef registryRef,
        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataMap,
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap,
        TimelineHashMap<TopicIdPartition, Set<Long>> partition2streams,
        TimelineHashMap<Long, TopicIdPartition> stream2partition,
        TimelineHashMap<Long, Long> streamEndOffsets
    ) {
        this.nextAssignedStreamId = assignedStreamId + 1;
        this.streamMetadataMap = streamMetadataMap;
        this.nodeMetadataMap = nodeMetadataMap;
        this.partition2streams = partition2streams;
        this.stream2partition = stream2partition;
        this.streamEndOffsets = streamEndOffsets;
        this.registryRef = registryRef;
    }

    boolean isEmpty() {
        if (registryRef == RegistryRef.NOOP) {
            return true;
        }
        return registryRef.inLock(() ->
            this.nodeMetadataMap.isEmpty(registryRef.epoch()) && this.streamMetadataMap.isEmpty(registryRef.epoch())
        );
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(
            new ApiMessageAndVersion(
                new AssignedStreamIdRecord().setAssignedStreamId(nextAssignedStreamId - 1), (short) 0));

        List<S3StreamMetadataImage> streamMetadataList = this.streamMetadataList();
        streamMetadataList.forEach(v -> v.write(writer, options));

        List<NodeS3StreamSetObjectMetadataImage> nodeMetadataList = this.nodeMetadataList();
        nodeMetadataList.forEach(v -> v.write(writer, options));

        if (options.metadataVersion().autoMQVersion().isHugeClusterSupported()) {
            Map<Long, Long> streamEndOffsetMap = this.streamEndOffsets();
            List<StreamEndOffset> endOffsets = streamEndOffsetMap.entrySet().stream()
                .map(e -> new StreamEndOffset(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
            writer.write(new ApiMessageAndVersion(
                new S3StreamEndOffsetsRecord().setEndOffsets(S3StreamEndOffsetsCodec.encode(endOffsets)),
                (short) 0
            ));
        }
    }

    public CompletableFuture<InRangeObjects> getObjects(long streamId, long startOffset, long endOffset, int limit,
        RangeGetter rangeGetter) {
        return getObjects(streamId, startOffset, endOffset, limit, rangeGetter, null);
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
        RangeGetter rangeGetter, LocalStreamRangeIndexCache indexCache) {
        long startTimeNanos = System.nanoTime();
        GetObjectsContext ctx = new GetObjectsContext(streamId, startOffset, endOffset, limit, rangeGetter, indexCache);
        try {
            getObjects0(ctx);
        } catch (Throwable e) {
            ctx.cf.completeExceptionally(e);
        }
        ctx.cf.whenComplete((r, ex) -> {
            long timeElapsedNanos = TimerUtil.timeElapsedSince(startTimeNanos, TimeUnit.NANOSECONDS);
            if (ex != null) {
                MetadataStats.getInstance().getObjectsTimeFailedStats().record(timeElapsedNanos);
            } else {
                MetadataStats.getInstance().getObjectsTimeSuccessStats().record(timeElapsedNanos);
            }
        });
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
        S3StreamMetadataImage stream = getStreamMetadata(streamId);
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

    private void fillObjects(
        GetObjectsContext ctx,
        S3StreamMetadataImage stream,
        List<S3ObjectMetadata> objects,
        int lastRangeIndex,
        int streamObjectIndex,
        List<S3StreamObject> streamObjects,
        int streamSetObjectIndex,
        List<S3StreamSetObject> streamSetObjects,
        NodeS3StreamSetObjectMetadataImage node
    ) {
        exec(() -> fillObjects0(ctx, stream, objects, lastRangeIndex, streamObjectIndex, streamObjects,
            streamSetObjectIndex, streamSetObjects, node), ctx.cf, LOGGER, "fillObjects");
    }

    void fillObjects0(
        GetObjectsContext ctx,
        S3StreamMetadataImage stream,
        List<S3ObjectMetadata> objects,
        int lastRangeIndex,
        int streamObjectIndex,
        List<S3StreamObject> streamObjects,
        int streamSetObjectIndex,
        List<S3StreamSetObject> streamSetObjects,
        NodeS3StreamSetObjectMetadataImage node
    ) {
        ctx.record(objects.size(), lastRangeIndex, streamObjectIndex, streamSetObjectIndex);
        long nextStartOffset = ctx.nextStartOffset;
        boolean firstTimeSearchInSSO = true;
        int finalStartSearchIndex = streamSetObjectIndex;
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
                    completeWithSanityCheck(ctx, objects);
                    return;
                }
            }

            if (streamSetObjects == null) {
                int rangeIndex = stream.getRangeContainsOffset(nextStartOffset);
                // 1. can not find the range containing nextStartOffset, or
                // 2. the range is the same as the last one, which means the nextStartOffset does not move on.
                if (rangeIndex < 0 || lastRangeIndex == rangeIndex) {
                    completeWithSanityCheck(ctx, objects);
                    break;
                }
                lastRangeIndex = rangeIndex;
                RangeMetadata range = stream.getRanges().get(rangeIndex);
                node = getNodeMetadata(range.nodeId());
                if (node != null) {
                    streamSetObjects = node.orderList();
                } else {
                    streamSetObjects = Collections.emptyList();
                }

                ctx.isFromSparseIndex = false;
                CompletableFuture<Integer> startSearchIndexCf = getStartSearchIndex(node, nextStartOffset, ctx);
                final int finalLastRangeIndex = lastRangeIndex;
                final long finalNextStartOffset = nextStartOffset;
                final int finalStreamObjectIndex = streamObjectIndex;
                final List<S3StreamSetObject> finalStreamSetObjects = streamSetObjects;
                final NodeS3StreamSetObjectMetadataImage finalNode = node;
                startSearchIndexCf.whenComplete((index, ex) -> {
                    if (ex != null) {
                        if (!(FutureUtil.cause(ex) instanceof ObjectNotExistException)) {
                            LOGGER.error("Failed to get start search index", ex);
                        }
                        index = 0;
                    }
                    // load stream set object index
                    int finalIndex = index;
                    loadStreamSetObjectInfo(ctx, finalStreamSetObjects, index).thenAccept(v -> {
                        ctx.nextStartOffset = finalNextStartOffset;
                        fillObjects(ctx, stream, objects, finalLastRangeIndex, finalStreamObjectIndex, streamObjects,
                            finalIndex, finalStreamSetObjects, finalNode);
                    }).exceptionally(exception -> {
                        ctx.cf.completeExceptionally(exception);
                        return null;
                    });
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
                    if (firstTimeSearchInSSO && ctx.isFromSparseIndex) {
                        MetadataStats.getInstance().getRangeIndexSkippedObjectNumStats().record(streamSetObjectIndex - finalStartSearchIndex);
                        firstTimeSearchInSSO = false;
                    }
                    if (objects.size() >= ctx.limit || (ctx.endOffset != ObjectUtils.NOOP_OFFSET && nextStartOffset >= ctx.endOffset)) {
                        completeWithSanityCheck(ctx, objects);
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

    private void completeWithSanityCheck(GetObjectsContext ctx, List<S3ObjectMetadata> objects) {
        try {
            sanityCheck(ctx, objects);
        } catch (Throwable t) {
            ctx.cf.completeExceptionally(new IllegalStateException(String.format("Get objects failed for streamId=%d," +
                " range=[%d, %d), limit=%d", ctx.streamId, ctx.startOffset, ctx.endOffset, ctx.limit), t));
            return;
        }
        ctx.cf.complete(new InRangeObjects(ctx.streamId, objects));
    }

    /**
     * Check for continuity of stream range and range matching.
     * Note: insufficient range is possible and caller should retry after
     */
    void sanityCheck(GetObjectsContext ctx, List<S3ObjectMetadata> objects) {
        if (objects.size() > ctx.limit) {
            throw new IllegalArgumentException(String.format("number of objects %d exceeds limit %d", objects.size(), ctx.limit));
        }
        long nextStartOffset = -1L;
        for (S3ObjectMetadata object : objects) {
            StreamOffsetRange range = findStreamOffsetRange(object.getOffsetRanges(), ctx.streamId);
            if (range == null) {
                throw new IllegalArgumentException(String.format("range not found in object %s", object));
            }
            if (nextStartOffset == -1L) {
                if (range.startOffset() > ctx.startOffset) {
                    throw new IllegalArgumentException(String.format("first matched range %s in object %s is greater than" +
                        " requested start offset %d", range, object, ctx.startOffset));
                }
            } else if (nextStartOffset != range.startOffset()) {
                throw new IllegalArgumentException(String.format("range is not continuous, expected start offset %d," +
                    " actual %d in object %s", nextStartOffset, range.startOffset(), object));
            }
            nextStartOffset = range.endOffset();
        }
    }

    private StreamOffsetRange findStreamOffsetRange(List<StreamOffsetRange> ranges, long streamId) {
        for (StreamOffsetRange range : ranges) {
            if (range.streamId() == streamId) {
                return range;
            }
        }
        return null;
    }

    private int findStartSearchIndex(long startObjectId, List<S3StreamSetObject> streamSetObjects) {
        for (int i = 0; i < streamSetObjects.size(); i++) {
            S3StreamSetObject sso = streamSetObjects.get(i);
            if (Objects.equals(sso.objectId(), startObjectId)) {
                return i;
            }
        }
        return -1;
    }

    private CompletableFuture<Integer> getStartSearchIndex(NodeS3StreamSetObjectMetadataImage node, long startOffset,
        GetObjectsContext ctx) {
        return exec(() -> getStartSearchIndex0(node, startOffset, ctx), LOGGER, "getStartSearchIndex");
    }

    /**
     * Get the index where to start search stream set object from.
     */
    private CompletableFuture<Integer> getStartSearchIndex0(NodeS3StreamSetObjectMetadataImage node, long startOffset,
        GetObjectsContext ctx) {
        if (node == null) {
            return CompletableFuture.completedFuture(0);
        }
        // search in compact cache first
        int index = node.floorStreamSetObjectIndex(ctx.streamId, startOffset);
        if (index > 0) {
            return CompletableFuture.completedFuture(index);
        }
        // search in sparse index
        ctx.isFromSparseIndex = true;
        return getStartStreamSetObjectId(node.getNodeId(), startOffset, ctx)
            .thenApply(objectId -> {
                int startIndex = -1;
                if (objectId >= 0) {
                    startIndex = findStartSearchIndex(objectId, node.orderList());
                    MetadataStats.getInstance().getRangeIndexHitCountStats().add(MetricsLevel.INFO, 1);
                } else {
                    MetadataStats.getInstance().getRangeIndexMissCountStats().add(MetricsLevel.INFO, 1);
                }
                if (startIndex < 0 && ctx.indexCache.nodeId() != node.getNodeId()) {
                    NodeRangeIndexCache.getInstance().invalidate(node.getNodeId());
                    MetadataStats.getInstance().getRangeIndexInvalidateCountStats().add(MetricsLevel.INFO, 1);
                }
                return Math.max(0, startIndex);
            });
    }

    /**
     * Get the object id of the first stream set object to start search from.
     * Possible results:
     * <p>
     * a. -1, no stream set object can be found, this can happen when index cache is not exist or is invalidated,
     * searching should be done from the beginning of the objects in this case.
     * <p>
     * b. non-negative value:
     * 1. the object exists in stream set objects in node image, search should start from that object
     * 2. the object does not exist in stream set objects in node image, that means the index cache is out of date and
     * should be invalidated, so we can refresh the index from object storage next time
     */
    private CompletableFuture<Long> getStartStreamSetObjectId(int nodeId, long startOffset, GetObjectsContext ctx) {
        if (ctx.indexCache != null && ctx.indexCache.nodeId() == nodeId) {
            return ctx.indexCache.searchObjectId(ctx.streamId, startOffset);
        }
        // search from cache and refresh the cache from remote if necessary
        return NodeRangeIndexCache.getInstance().searchObjectId(nodeId, ctx.streamId, startOffset,
            () -> ctx.rangeGetter.readNodeRangeIndex(nodeId).thenApply(buff -> {
                try {
                    return LocalStreamRangeIndexCache.fromBuffer(buff);
                } finally {
                    buff.release();
                }
            }));
    }

    /**
     * Load the stream set object range info is missing
     *
     * @return async load
     */
    private CompletableFuture<Void> loadStreamSetObjectInfo(GetObjectsContext ctx,
        List<S3StreamSetObject> streamSetObjects,
        int startSearchIndex) {
        return exec(() -> {
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
            return CompletableFuture.allOf(loadIndexCfList.toArray(new CompletableFuture[0]));
        }, LOGGER, "loadStreamSetObjectInfo");
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
        S3StreamMetadataImage stream = getStreamMetadata(streamId);
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
        NodeS3StreamSetObjectMetadataImage wal = getNodeMetadata(nodeId);
        if (wal == null) {
            return Collections.emptyList();
        }
        return wal.orderList();
    }

    public S3StreamMetadataImage getStreamMetadata(long streamId) {
        if (registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> streamMetadataMap.get(streamId, registryRef.epoch()));
    }

    public NodeS3StreamSetObjectMetadataImage getNodeMetadata(int nodeId) {
        if (registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> nodeMetadataMap.get(nodeId, registryRef.epoch()));
    }

    public Set<Long> getTopicPartitionStreams(Uuid topicId, int partition) {
        if (registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> partition2streams.getOrDefault(new TopicIdPartition(topicId, partition), Collections.emptySet()));
    }

    public TopicIdPartition getStreamTopicPartition(long streamId) {
        if (registryRef == RegistryRef.NOOP) {
            return null;
        }
        return registryRef.inLock(() -> stream2partition.get(streamId));
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
            && this.streamMetadataList().equals(other.streamMetadataList())
            && this.nodeMetadataList().equals(other.nodeMetadataList())
            && this.streamEndOffsets().equals(other.streamEndOffsets());
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedStreamId, streamMetadataList(), nodeMetadataList(), streamEndOffsets());
    }

    // caller use this value should be protected by registryRef lock
    public TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> timelineNodeMetadata() {
        return nodeMetadataMap;
    }

    List<NodeS3StreamSetObjectMetadataImage> nodeMetadataList() {
        if (registryRef == RegistryRef.NOOP) {
            return Collections.emptyList();
        }
        return registryRef.inLock(() -> {
            List<NodeS3StreamSetObjectMetadataImage> list = new ArrayList<>(nodeMetadataMap.size());
            list.addAll(nodeMetadataMap.values(registryRef.epoch()));
            return list;
        });
    }

    // caller use this value should be protected by registryRef lock
    public TimelineHashMap<Long, S3StreamMetadataImage> timelineStreamMetadata() {
        return streamMetadataMap;
    }

    public void inLockRun(Runnable runnable) {
        registryRef.inLock(runnable);
    }

    List<S3StreamMetadataImage> streamMetadataList() {
        if (registryRef == RegistryRef.NOOP) {
            return Collections.emptyList();
        }
        return registryRef.inLock(() -> {
            List<S3StreamMetadataImage> list = new ArrayList<>(streamMetadataMap.size());
            list.addAll(streamMetadataMap.values(registryRef.epoch()));
            return list;
        });
    }

    public long nextAssignedStreamId() {
        return nextAssignedStreamId;
    }

    public OptionalLong streamEndOffset(long streamId) {
        if (registryRef == RegistryRef.NOOP) {
            return OptionalLong.empty();
        }
        return registryRef.inLock(() -> {
            Long endOffset = streamEndOffsets.get(streamId);
            if (endOffset != null) {
                return OptionalLong.of(endOffset);
            }
            // There is no record in a new stream
            if (streamMetadataMap.containsKey(streamId)) {
                return OptionalLong.of(0L);
            } else {
                return OptionalLong.empty();
            }
        });
    }

    // caller use this value should be protected by registryRef lock
    TimelineHashMap<TopicIdPartition, Set<Long>> partition2streams() {
        return partition2streams;
    }

    // caller use this value should be protected by registryRef lock
    TimelineHashMap<Long, TopicIdPartition> stream2partition() {
        return stream2partition;
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
            Map<Long, Long> map = new HashMap<>(streamEndOffsets.size());
            streamEndOffsets.entrySet(registryRef.epoch()).forEach(e -> map.put(e.getKey(), e.getValue()));
            return map;
        });
    }

    RegistryRef registryRef() {
        return registryRef;
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
        final long streamId;
        long startOffset;
        long nextStartOffset;
        long endOffset;
        int limit;
        boolean isFromSparseIndex;
        RangeGetter rangeGetter;
        LocalStreamRangeIndexCache indexCache;

        CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
        Map<Long, Optional<StreamOffsetRange>> object2range = new ConcurrentHashMap<>();
        List<String> debugContext = new ArrayList<>();

        GetObjectsContext(long streamId, long startOffset, long endOffset, int limit,
            RangeGetter rangeGetter, LocalStreamRangeIndexCache indexCache) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.nextStartOffset = startOffset;
            this.endOffset = endOffset;
            this.limit = limit;
            this.rangeGetter = rangeGetter;
            this.indexCache = indexCache;
            this.isFromSparseIndex = false;
        }

        public void record(int objectSize, int lastRangeIndex, int streamObjectIndex, int streamSetObjectIndex) {
            debugContext.add(String.format("nextStartOffset=%d, objectSize=%d, lastRangeIndex=%d, streamObjectIndex=%d, streamSetObjectIndex=%d", nextStartOffset,
                objectSize, lastRangeIndex, streamObjectIndex, streamSetObjectIndex));
            if (debugContext.size() > 20) {
                LOGGER.error("GetObjects may has endless loop: streamId={}, startOffset={}, endOffset={}, limit={}, debugContext={}",
                    streamId, startOffset, endOffset, limit, debugContext);
                Runtime.getRuntime().halt(1);
            }
        }
    }

    public interface RangeGetter {
        CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId);

        CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId);
    }

}
