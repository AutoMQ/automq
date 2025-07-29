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
import com.automq.stream.s3.index.lazy.StreamSetObjectRangeIndex;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.MetadataStats;
import com.automq.stream.utils.FutureUtil;
import com.google.common.annotations.VisibleForTesting;

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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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

    @VisibleForTesting
    public CompletableFuture<InRangeObjects> getObjects(long streamId, long startOffset, long endOffset, int limit,
        RangeGetter rangeGetter) {
        return getObjects(streamId, startOffset, endOffset, limit, rangeGetter, LocalStreamRangeIndexCache.create());
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
            rangeGetter.attachGetObjectsContext(ctx);
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

    private boolean readEndOffset(long streamId, long endOffset) {
        boolean readEndOffset = endOffset == -1L;
        OptionalLong optionalLong = streamEndOffset(streamId);
        if (optionalLong.isPresent()) {
            readEndOffset = readEndOffset || optionalLong.getAsLong() == endOffset;
        }

        return readEndOffset;
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

        ctx.readEndOffset = readEndOffset(streamId, endOffset);

        // floor value < 0 means that all stream objects' ranges are greater than startOffset
        int streamObjectIndex = Math.max(0, stream.floorStreamObjectIndex(startOffset));

        final List<S3StreamObject> streamObjects = stream.getStreamObjects();

        int lastRangeIndex = -1;
        int streamSetObjectIndex = 0;
        fillObjects(ctx, stream, objects, lastRangeIndex, streamObjectIndex, streamObjects, streamSetObjectIndex,
            null, 0, null);
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
        int loadedStreamSetObjectIndex,
        NodeS3StreamSetObjectMetadataImage node
    ) {
        exec(() -> fillObjects0(ctx, stream, objects, lastRangeIndex, streamObjectIndex, streamObjects,
            streamSetObjectIndex, streamSetObjects, loadedStreamSetObjectIndex, node), ctx.cf, LOGGER, "fillObjects");
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
        int loadedStreamSetObjectIndex,
        NodeS3StreamSetObjectMetadataImage node
    ) {
        ctx.record(objects.size(), lastRangeIndex, streamObjectIndex, streamSetObjectIndex, loadedStreamSetObjectIndex, node);
        long nextStartOffset = ctx.nextStartOffset;
        boolean firstTimeSearchInSSO = true;
        int finalStartSearchIndex = streamSetObjectIndex;
        for (; ; ) {
            int roundStartObjectSize = objects.size();

            // try to find consistent stream objects
            for (; streamObjectIndex < streamObjects.size(); streamObjectIndex++) {
                ctx.checkSOCount++;
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
                ctx.searchNodeCount++;
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
                final long nodeId = range.nodeId();
                startSearchIndexCf.whenComplete((index, ex) -> {
                    if (ex != null) {
                        if (!(FutureUtil.cause(ex) instanceof ObjectNotExistException)) {
                            LOGGER.error("Failed to get start search index", ex);
                        }
                        index = 0;
                    }
                    // load stream set object index
                    int finalIndex = index;
                    if (finalIndex != 0) {
                        ctx.ssoIndexHelpCount++;
                    } else {
                        ctx.ssoNoIndex++;
                    }
                    long startTimeNs = System.nanoTime();
                    loadStreamSetObjectInfo(ctx, nodeId, finalStreamSetObjects, index, ctx.loadStreamSetObjectInfo / 5 + 5)
                        .thenAccept(finalEnsureLoadedIndexExclusive -> {
                            ctx.nextStartOffset = finalNextStartOffset;
                            ctx.waitLoadSSOCostMs.add(TimerUtil.timeElapsedSince(startTimeNs, TimeUnit.NANOSECONDS));
                            fillObjects(ctx, stream, objects, finalLastRangeIndex, finalStreamObjectIndex, streamObjects, finalIndex, finalStreamSetObjects, finalEnsureLoadedIndexExclusive, finalNode);
                        }).exceptionally(exception -> {
                            ctx.cf.completeExceptionally(exception);
                            return null;
                        });
                });
                return;
            }

            for (; streamSetObjectIndex < loadedStreamSetObjectIndex; streamSetObjectIndex++) {
                ctx.checkSSOCount++;
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

            if (streamSetObjectIndex == loadedStreamSetObjectIndex && loadedStreamSetObjectIndex < streamSetObjects.size()) {
                loadMoreStreamSetObjects(ctx, stream, objects, nextStartOffset, lastRangeIndex, streamSetObjects,
                    streamObjectIndex, loadedStreamSetObjectIndex, streamObjects, node);
                return;
            }

            if (streamSetObjectIndex >= streamSetObjects.size() || objects.size() == roundStartObjectSize) {
                // move to the next range
                // This can ensure that we can break the loop.
                streamSetObjects = null;
            }
        }
    }

    private void loadMoreStreamSetObjects(GetObjectsContext ctx,
                                          S3StreamMetadataImage stream,
                                          List<S3ObjectMetadata> objects,
                                          long nextStartOffset,
                                          int lastRangeIndex,
                                          List<S3StreamSetObject> streamSetObjects,
                                          int streamObjectIndex,
                                          int loadedStreamSetObjectIndex,
                                          List<S3StreamObject> streamObjects,
                                          NodeS3StreamSetObjectMetadataImage node) {
        // check if index can fast skip
        CompletableFuture<Integer> startSearchIndexCf = getStartSearchIndex(node, nextStartOffset, ctx);

        startSearchIndexCf.whenComplete((index, ex) -> {
            if (ex != null) {
                if (!(FutureUtil.cause(ex) instanceof ObjectNotExistException)) {
                    LOGGER.error("Failed to get start search index", ex);
                }
                index = 0;
            }

            if (index > loadedStreamSetObjectIndex) {
                ctx.fastSkipIndexCount++;
            }

            // load stream set object index
            int finalIndex = Math.max(index, loadedStreamSetObjectIndex);
            long startTimeNs = System.nanoTime();
            loadStreamSetObjectInfo(ctx, node.getNodeId(), streamSetObjects, finalIndex, ctx.loadStreamSetObjectInfo / 5 + 5)
                .thenAccept(loadedIndexExclusive -> {
                    ctx.nextStartOffset = nextStartOffset;
                    ctx.waitLoadSSOCostMs.add(TimerUtil.timeElapsedSince(startTimeNs, TimeUnit.NANOSECONDS));
                    fillObjects(ctx, stream, objects, lastRangeIndex, streamObjectIndex, streamObjects,
                        finalIndex, streamSetObjects, loadedIndexExclusive, node);
                }).exceptionally(exception -> {
                    ctx.cf.completeExceptionally(exception);
                    return null;
                });
        });
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

        // search in sparse index
        ctx.isFromSparseIndex = true;
        return getStartStreamSetObjectId(node.getNodeId(), startOffset, ctx)
            .thenApply(objectId -> {
                int startIndex = -1;
                if (objectId >= 0) {
                    startIndex = findStartSearchIndex(objectId, node.orderList());
                    if (startIndex < 0) {
                        StreamSetObjectRangeIndex.getInstance().invalid(node.getNodeId(), ctx.streamId, startOffset, objectId);
                    } else {
                        MetadataStats.getInstance().getRangeIndexHitCountStats().add(MetricsLevel.INFO, 1);
                    }
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
        if (StreamSetObjectRangeIndex.isEnabled()) {
            return StreamSetObjectRangeIndex.getInstance().searchObjectId(nodeId, ctx.streamId, startOffset);
        }

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
     * @return async ensure loaded stream set object index (exclusive)
     */
    private CompletableFuture<Integer> loadStreamSetObjectInfo(GetObjectsContext ctx,
                                                               long nodeId,
                                                               List<S3StreamSetObject> streamSetObjects,
                                                               int startSearchIndex,
                                                               int maxWaitInfoNum
    ) {
        return exec(() -> {
            ctx.loadStreamSetObjectInfo++;
            final int streamSetObjectsSize = streamSetObjects.size();
            List<CompletableFuture<Void>> loadIndexCfList = new LinkedList<>();

            int currentWaitNum = maxWaitInfoNum;
            int lastLoadedIndex = startSearchIndex;

            for (int i = startSearchIndex; i < streamSetObjectsSize; i++) {
                S3StreamSetObject streamSetObject = streamSetObjects.get(i);
                if (streamSetObject.ranges().length != 0) {
                    continue;
                }
                if (ctx.object2range.containsKey(streamSetObject.objectId())) {
                    continue;
                }

                CompletableFuture<Void> cf = ctx.rangeGetter
                    .find(streamSetObject.objectId(), ctx.streamId, nodeId, streamSetObject.orderId())
                    .thenAccept(range -> ctx.object2range.put(streamSetObject.objectId(), range));

                if (!cf.isDone() && currentWaitNum > 0) {
                    ctx.waitSSOLoadCount++;
                    currentWaitNum--;
                }

                loadIndexCfList.add(cf);

                if (currentWaitNum > 0) {
                    lastLoadedIndex = i;
                } else {
                    break;
                }
            }

            if (loadIndexCfList.isEmpty()) {
                // not need to load any index
                return CompletableFuture.completedFuture(streamSetObjectsSize);
            }

            if (ctx.readEndOffset) {
                preLoadEndOffsetStreamSetObject(ctx, nodeId, streamSetObjects);
            } else {
                preLoadMiddleOffsetStreamSetObject(ctx, nodeId, streamSetObjects, lastLoadedIndex, maxWaitInfoNum);
            }

            int finalLastLoadedIndex = lastLoadedIndex;
            return CompletableFuture.allOf(loadIndexCfList.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    return finalLastLoadedIndex + 1;
                });
        }, LOGGER, "loadStreamSetObjectInfo");
    }

    private void preLoadMiddleOffsetStreamSetObject(GetObjectsContext ctx,
                                                    long nodeId,
                                                    List<S3StreamSetObject> streamSetObjects,
                                                    int lastLoadedIndex,
                                                    int maxWaitInfoNum) {
        int streamSetObjectsSize = streamSetObjects.size();
        int remainingObjects = streamSetObjectsSize - lastLoadedIndex;
        if (!ctx.readEndOffset && ctx.maxPreLoadSSORound < 5 && remainingObjects > maxWaitInfoNum) {
            double preLoadStep = (double) remainingObjects / (5 - ctx.maxPreLoadSSORound);

            List<Integer> preLoadPos = new ArrayList<>();

            for (int i = 1; i <= 4; i++) {
                int loadIndex = lastLoadedIndex + (int) (preLoadStep * i);

                if (loadIndex <= lastLoadedIndex || loadIndex >= streamSetObjectsSize) {
                    continue;
                }

                preLoadPos.add(loadIndex);
            }

            preLoadStreamSetObject(ctx, nodeId, streamSetObjects, preLoadPos);

            ctx.maxPreLoadSSORound++;
        }
    }

    private void preLoadStreamSetObject(GetObjectsContext ctx, long nodeId, List<S3StreamSetObject> streamSetObjects, List<Integer> indexProvider) {
        indexProvider.forEach(index -> {
            S3StreamSetObject streamSetObject = streamSetObjects.get(index);
            if (streamSetObject.ranges().length != 0) {
                return;
            }
            if (ctx.object2range.containsKey(streamSetObject.objectId())) {
                return;
            }

            ctx.preLoadSSOCount++;
            ctx.rangeGetter
                .find(streamSetObject.objectId(), ctx.streamId, nodeId, streamSetObject.orderId())
                .thenAccept(range -> {
                    ctx.object2range.put(streamSetObject.objectId(), range);
                    PreLoadHistory preLoadHistory = new PreLoadHistory(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ctx.startTime), nodeId, index, range.orElse(null));
                    ctx.recordPreload(preLoadHistory);
                });
        });
    }

    private void preLoadEndOffsetStreamSetObject(GetObjectsContext ctx, long nodeId, List<S3StreamSetObject> streamSetObjects) {
        int lastLoadedCount = ctx.lastObjectLoaded(nodeId);
        int streamSetObjectsSize = streamSetObjects.size();
        if (ctx.readEndOffset && lastLoadedCount < 5) {
            ctx.incLastObjectLoaded(nodeId);

            List<Integer> preLoadPos = new ArrayList<>();
            for (int i = streamSetObjectsSize - 5 * lastLoadedCount; i < streamSetObjectsSize - 5 && i >= 0; i++) {
                preLoadPos.add(i);
            }

            preLoadStreamSetObject(ctx, nodeId, streamSetObjects, preLoadPos);
        }
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

    public static class GetObjectDebugInfo {
        private final long nextStartOffset;
        private final int objectSize;
        private final int lastRangeIndex;
        private final int streamObjectIndex;
        private final int streamSetObjectIndex;
        private final int loadedStreamSetObjectIndex;
        private final int ssoSize;
        private final int nodeId;
        private final long id;

        public GetObjectDebugInfo(long id, long nextStartOffset,
                                  int objectSize,
                                  int lastRangeIndex,
                                  int streamObjectIndex,
                                  int streamSetObjectIndex,
                                  int loadedStreamSetObjectIndex,
                                  int ssoSize,
                                  int nodeId) {
            this.id = id;
            this.nextStartOffset = nextStartOffset;
            this.objectSize = objectSize;
            this.lastRangeIndex = lastRangeIndex;
            this.streamObjectIndex = streamObjectIndex;
            this.streamSetObjectIndex = streamSetObjectIndex;
            this.loadedStreamSetObjectIndex = loadedStreamSetObjectIndex;
            this.ssoSize = ssoSize;
            this.nodeId = nodeId;
        }

        @Override
        public String toString() {
            return "GetObjectDebugInfo{" +
                "id=" + id +
                ", nextStartOffset=" + nextStartOffset +
                ", objectSize=" + objectSize +
                ", lastRangeIndex=" + lastRangeIndex +
                ", streamObjectIndex=" + streamObjectIndex +
                ", streamSetObjectIndex=" + streamSetObjectIndex +
                ", loadedStreamSetObjectIndex=" + loadedStreamSetObjectIndex +
                ", ssoSize=" + ssoSize +
                ", nodeId=" + nodeId +
                '}';
        }
    }

    public static class PreLoadHistory {
        long nodeId;
        long index;
        StreamOffsetRange streamOffsetRange;
        long id;

        public PreLoadHistory(long id, long nodeId, long index, StreamOffsetRange streamOffsetRange) {
            this.id = id;
            this.nodeId = nodeId;
            this.index = index;
            this.streamOffsetRange = streamOffsetRange;
        }

        @Override
        public String toString() {
            return "PreLoadHistory{" +
                "id=" + id +
                ", nodeId=" + nodeId +
                ", index=" + index +
                ", streamOffsetRange=" + streamOffsetRange +
                '}';
        }
    }

    public static class GetObjectsContext {
        final long streamId;
        public List<RangeMetadata> range;
        boolean readEndOffset;
        long startOffset;
        long nextStartOffset;
        long endOffset;
        int limit;
        boolean isFromSparseIndex;
        int maxPreLoadSSORound = 1;
        RangeGetter rangeGetter;
        LocalStreamRangeIndexCache indexCache;

        CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
        Map<Long, Optional<StreamOffsetRange>> object2range = new ConcurrentHashMap<>();
        List<GetObjectDebugInfo> debugContext = new ArrayList<>();
        final ConcurrentLinkedQueue<PreLoadHistory> preloadHistory = new ConcurrentLinkedQueue<>();

        public int checkSOCount;
        public int checkSSOCount;

        public int searchNodeCount;
        public int searchSSOStreamOffsetRangeCount;
        public LongAdder searchSSORangeEmpty = new LongAdder();
        public int bloomFilterSkipSSOCount;

        public int ssoIndexHelpCount;
        public int ssoNoIndex;

        public int waitSSOLoadCount;
        public LongAdder waitLoadSSOCostMs = new LongAdder();
        public int preLoadSSOCount;

        public int fastSkipIndexCount;
        public int loadStreamSetObjectInfo;

        public String dumpStatistics() {
            return String.format("GetObjectsContext{streamId=%d, startOffset=%d, nextStartOffset=%d, endOffset=%d, limit=%d,%n" +
                "checkSOCount=%d, checkSSOCount=%d, searchNodeCount=%d, searchSSOStreamOffsetRangeCount=%d, searchSSORangeEmpty=%d,%n" +
                "bloomFilterSkipCount=%d, ssoNoIndex=%d, ssoIndexHelpCount=%d, waitSSOLoadCount=%d, waitLoadSSOCostMs=%d%n" +
                    "loadStreamSetObjectInfo=%d, preLoadSSOCount=%d, fastSkipIndexCount=%d}",
                streamId, startOffset, nextStartOffset, endOffset, limit,
                checkSOCount, checkSSOCount,
                searchNodeCount, searchSSOStreamOffsetRangeCount, searchSSORangeEmpty.sum(),
                bloomFilterSkipSSOCount, ssoNoIndex,
                ssoIndexHelpCount,
                waitSSOLoadCount,
                TimeUnit.MILLISECONDS.convert(waitLoadSSOCostMs.sum(), TimeUnit.NANOSECONDS),
                loadStreamSetObjectInfo,
                preLoadSSOCount,
                fastSkipIndexCount);
        }

        private final long startTime = System.nanoTime();

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
            cf.whenComplete((v, e) -> {
                long cost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

                if (cost > 500 || fastSkipIndexCount > 3) {
                    LOGGER.info("get objects streamId={}, startOffset={}, endOffset={}, limit={} cost={}ms readEndOffset={}, debugContext={} \n" +
                            "ranges={}\n" +
                            "preload={}\n" +
                            "Metrics: checkSOCount={}, checkSSOCount={}, searchNodeCount={}, searchSSORangeCount={}, \" +\n" +
                            "searchSSORangeEmpty={}, bloomFilterSkipSSOCount={}, ssoIndexHelpCount={}, \" +\n" +
                            "ssoNoIndex={}, waitSSOLoadCount={}, waitLoadSSOCostMs={}, preLoadSSOCount={}, \" +\n" +
                            "fastSkipIndexCount={}, loadStreamSetObjectInfo={}",
                        streamId, startOffset, endOffset, limit, cost, readEndOffset, debugContext,
                        range,
                        this.preloadHistory,
                        this.checkSOCount, this.checkSSOCount, this.searchNodeCount, this.searchSSOStreamOffsetRangeCount,
                        this.searchSSORangeEmpty.sum(), this.bloomFilterSkipSSOCount, this.ssoIndexHelpCount,
                        this.ssoNoIndex, this.waitSSOLoadCount, cost,
                        this.preLoadSSOCount, this.fastSkipIndexCount, this.loadStreamSetObjectInfo);
                }
            });
        }

        public List<GetObjectDebugInfo> getDebugContext() {
            return debugContext;
        }

        public void recordPreload(PreLoadHistory history) {
            this.preloadHistory.add(history);
        }

        public void recordRange(List<RangeMetadata> range) {
            this.range = range;
        }

        public void record(int objectSize, int lastRangeIndex, int streamObjectIndex, int streamSetObjectIndex, int loadedStreamSetObjectIndex, NodeS3StreamSetObjectMetadataImage node) {
            int nodeId = -1;
            int ssoSize = -1;
            if (node != null) {
                nodeId = node.getNodeId();
                ssoSize = node.orderList().size();
            }

            debugContext.add(new GetObjectDebugInfo(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - this.startTime), nextStartOffset,
                objectSize,
                lastRangeIndex,
                streamObjectIndex,
                streamSetObjectIndex,
                loadedStreamSetObjectIndex,
                ssoSize,
                nodeId));

            if (debugContext.size() > 500) {
                LOGGER.error("GetObjects may has endless loop: streamId={}, startOffset={}, endOffset={}, limit={}, debugContext={}",
                    streamId, startOffset, endOffset, limit, debugContext);
                Runtime.getRuntime().halt(1);
            }
        }

        private final ConcurrentHashMap<Long, Integer> lastObjectLoadedSet = new ConcurrentHashMap<>();

        public int lastObjectLoaded(long nodeId) {
            return lastObjectLoadedSet.getOrDefault(nodeId, 1);
        }

        public void incLastObjectLoaded(long nodeId) {
            this.lastObjectLoadedSet.put(nodeId, lastObjectLoadedSet.getOrDefault(nodeId, 1) + 1);
        }
    }

    public interface RangeGetter {
        void attachGetObjectsContext(GetObjectsContext ctx);
        CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId, long nodeId, long orderId);
        CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId);
    }

}
