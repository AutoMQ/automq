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
package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3ObjectLogger;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.compact.operator.DataBlockWriter;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.github.bucket4j.Bucket;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class CompactionManager {
    private final Logger logger;
    private final Logger s3ObjectLogger;
    private final ObjectManager objectManager;
    private final StreamManager streamManager;
    private final S3Operator s3Operator;
    private final CompactionAnalyzer compactionAnalyzer;
    private final ScheduledExecutorService compactScheduledExecutor;
    private final ScheduledExecutorService bucketCallbackScheduledExecutor;
    private final ExecutorService compactThreadPool;
    private final ExecutorService forceSplitThreadPool;
    private final CompactionUploader uploader;
    private final Config config;
    private final int maxObjectNumToCompact;
    private final int compactionInterval;
    private final int forceSplitObjectPeriod;
    private final int maxStreamNumPerStreamSetObject;
    private final int maxStreamObjectNumPerCommit;
    private final long networkBandwidth;
    private final boolean s3ObjectLogEnable;
    private final long compactionCacheSize;
    private Bucket compactionBucket = null;

    public CompactionManager(Config config, ObjectManager objectManager, StreamManager streamManager, S3Operator s3Operator) {
        String logPrefix = String.format("[CompactionManager id=%d] ", config.nodeId());
        this.logger = new LogContext(logPrefix).logger(CompactionManager.class);
        this.s3ObjectLogger = S3ObjectLogger.logger(logPrefix);
        this.config = config;
        this.objectManager = objectManager;
        this.streamManager = streamManager;
        this.s3Operator = s3Operator;
        this.compactionInterval = config.streamSetObjectCompactionInterval();
        this.forceSplitObjectPeriod = config.streamSetObjectCompactionForceSplitPeriod();
        this.maxObjectNumToCompact = config.streamSetObjectCompactionMaxObjectNum();
        this.s3ObjectLogEnable = config.objectLogEnable();
        this.networkBandwidth = config.networkBaselineBandwidth();
        this.uploader = new CompactionUploader(objectManager, s3Operator, config);
        this.compactionCacheSize = config.streamSetObjectCompactionCacheSize();
        long streamSplitSize = config.streamSetObjectCompactionStreamSplitSize();
        maxStreamNumPerStreamSetObject = config.maxStreamNumPerStreamSetObject();
        maxStreamObjectNumPerCommit = config.maxStreamObjectNumPerCommit();
        this.compactionAnalyzer = new CompactionAnalyzer(compactionCacheSize, streamSplitSize, maxStreamNumPerStreamSetObject,
                maxStreamObjectNumPerCommit, new LogContext(String.format("[CompactionAnalyzer id=%d] ", config.nodeId())));
        this.compactScheduledExecutor = Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("schedule-compact-executor-%d", true), logger);
        this.bucketCallbackScheduledExecutor = Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("s3-data-block-reader-bucket-cb-%d", true), logger);
        this.compactThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("object-compaction-manager"));
        this.forceSplitThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("force-split-executor"));
        this.logger.info("Compaction manager initialized with config: compactionInterval: {} min, compactionCacheSize: {} bytes, " +
                        "streamSplitSize: {} bytes, forceSplitObjectPeriod: {} min, maxObjectNumToCompact: {}, maxStreamNumInStreamSet: {}, maxStreamObjectNum: {}",
                compactionInterval, compactionCacheSize, streamSplitSize, forceSplitObjectPeriod, maxObjectNumToCompact, maxStreamNumPerStreamSetObject, maxStreamObjectNumPerCommit);
    }

    public void start() {
        scheduleNextCompaction((long) this.compactionInterval * 60 * 1000);
    }

    private void scheduleNextCompaction(long delayMillis) {
        logger.info("Next Compaction started in {} ms", delayMillis);
        this.compactScheduledExecutor.schedule(() -> {
            TimerUtil timerUtil = new TimerUtil();
            try {
                logger.info("Compaction started");
                this.compact()
                        .thenAccept(result -> logger.info("Compaction complete, total cost {} ms", timerUtil.elapsedAs(TimeUnit.MILLISECONDS)))
                        .exceptionally(ex -> {
                            logger.error("Compaction failed, cost {} ms, ", timerUtil.elapsedAs(TimeUnit.MILLISECONDS), ex);
                            return null;
                        })
                        .join();
            } catch (Exception ex) {
                logger.error("Error while compacting objects ", ex);
            }
            long nextDelay = Math.max(0, (long) this.compactionInterval * 60 * 1000 - timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
            scheduleNextCompaction(nextDelay);
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.compactScheduledExecutor.shutdown();
        this.bucketCallbackScheduledExecutor.shutdown();
        this.uploader.stop();
    }

    private CompletableFuture<Void> compact() {
        return this.objectManager.getServerObjects().thenComposeAsync(objectMetadataList -> {
            List<Long> streamIds = objectMetadataList.stream().flatMap(e -> e.getOffsetRanges().stream())
                    .map(StreamOffsetRange::getStreamId).distinct().toList();
            return this.streamManager.getStreams(streamIds).thenAcceptAsync(streamMetadataList ->
                    this.compact(streamMetadataList, objectMetadataList), compactThreadPool);
        }, compactThreadPool);
    }

    private void compact(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectMetadataList) {
        logger.info("Get {} stream set objects from metadata", objectMetadataList.size());
        if (objectMetadataList.isEmpty()) {
            return;
        }
        Map<Boolean, List<S3ObjectMetadata>> objectMetadataFilterMap = convertS3Objects(objectMetadataList);
        List<S3ObjectMetadata> objectsToForceSplit = objectMetadataFilterMap.get(true);
        List<S3ObjectMetadata> objectsToCompact = objectMetadataFilterMap.get(false);

        long totalSize = objectsToForceSplit.stream().mapToLong(S3ObjectMetadata::objectSize).sum();
        totalSize += objectsToCompact.stream().mapToLong(S3ObjectMetadata::objectSize).sum();
        long expectReadBytesPerSec = totalSize / compactionInterval / 60;
        compactionBucket = Bucket.builder().addLimit(limit -> limit
                        .capacity(expectReadBytesPerSec)
                        .refillIntervally(expectReadBytesPerSec, Duration.ofSeconds(1))).build();
        logger.info("Throttle compaction read to {} bytes/s, expect to complete in no less than {}min",
                expectReadBytesPerSec, compactionInterval);

        if (!objectsToForceSplit.isEmpty()) {
            // split stream set objects to seperated stream objects
            forceSplitObjects(streamMetadataList, objectsToForceSplit);
        }
        // compact stream set objects
        compactObjects(streamMetadataList, objectsToCompact);
    }

    private void forceSplitObjects(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectsToForceSplit) {
        logger.info("Force split {} stream set objects", objectsToForceSplit.size());
        TimerUtil timerUtil = new TimerUtil();
        for (int i = 0; i < objectsToForceSplit.size(); i++) {
            timerUtil.reset();
            S3ObjectMetadata objectToForceSplit = objectsToForceSplit.get(i);
            logger.info("Force split progress {}/{}, splitting object {}, object size {}", i + 1, objectsToForceSplit.size(),
                    objectToForceSplit.objectId(), objectToForceSplit.objectSize());
            CommitStreamSetObjectRequest request = buildSplitRequest(streamMetadataList, objectToForceSplit);
            if (request == null) {
                continue;
            }
            logger.info("Build force split request for object {} complete, generated {} stream objects, time cost: {} ms, start committing objects",
                    objectToForceSplit.objectId(), request.getStreamObjects().size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
            timerUtil.reset();
            objectManager.commitStreamSetObject(request)
                    .thenAccept(resp -> {
                        logger.info("Commit force split request succeed, time cost: {} ms", timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                        if (s3ObjectLogEnable) {
                            s3ObjectLogger.trace("[Compact] {}", request);
                        }
                    })
                    .exceptionally(ex -> {
                        logger.error("Commit force split request failed, ex: ", ex);
                        return null;
                    })
                    .join();
        }
    }

    private void compactObjects(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectsToCompact) {
        if (objectsToCompact.isEmpty()) {
            return;
        }
        // sort by S3 object data time in descending order
        objectsToCompact.sort((o1, o2) -> Long.compare(o2.dataTimeInMs(), o1.dataTimeInMs()));
        if (maxObjectNumToCompact < objectsToCompact.size()) {
            // compact latest S3 objects first when number of objects to compact exceeds maxObjectNumToCompact
            objectsToCompact = objectsToCompact.subList(0, maxObjectNumToCompact);
        }
        logger.info("Compact {} stream set objects", objectsToCompact.size());
        TimerUtil timerUtil = new TimerUtil();
        CommitStreamSetObjectRequest request = buildCompactRequest(streamMetadataList, objectsToCompact);
        if (request == null) {
            return;
        }
        if (request.getCompactedObjectIds().isEmpty()) {
            logger.info("No stream set objects to compact");
            return;
        }
        logger.info("Build compact request for {} stream set objects complete, stream set object id: {}, stresam set object size: {}, stream object num: {}, time cost: {}, start committing objects",
                request.getCompactedObjectIds().size(), request.getObjectId(), request.getObjectSize(), request.getStreamObjects().size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
        timerUtil.reset();
        objectManager.commitStreamSetObject(request)
                .thenAccept(resp -> {
                    logger.info("Commit compact request succeed, time cost: {} ms", timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                    if (s3ObjectLogEnable) {
                        s3ObjectLogger.trace("[Compact] {}", request);
                    }
                })
                .exceptionally(ex -> {
                    logger.error("Commit compact request failed, ex: ", ex);
                    return null;
                })
                .join();
    }

    private void logCompactionPlans(List<CompactionPlan> compactionPlans, Set<Long> excludedObjectIds) {
        if (compactionPlans.isEmpty()) {
            logger.info("No compaction plans to execute");
            return;
        }
        long streamObjectNum = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.SPLIT)
                        .count())
                .sum();
        long streamSetObjectSize = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.COMPACT)
                        .mapToLong(CompactedObject::size)
                        .sum())
                .sum();
        int streamSetObjectNum = streamSetObjectSize > 0 ? 1 : 0;
        logger.info("Compaction plans: expect to generate {} Stream Object, {} stream set object with size {} in {} iterations, objects excluded: {}",
                streamObjectNum, streamSetObjectNum, streamSetObjectSize, compactionPlans.size(), excludedObjectIds);
    }

    public CompletableFuture<Void> forceSplitAll() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        //TODO: deal with metadata delay
        this.compactScheduledExecutor.execute(() -> this.objectManager.getServerObjects().thenAcceptAsync(objectMetadataList -> {
            List<Long> streamIds = objectMetadataList.stream().flatMap(e -> e.getOffsetRanges().stream())
                    .map(StreamOffsetRange::getStreamId).distinct().toList();
            this.streamManager.getStreams(streamIds).thenAcceptAsync(streamMetadataList -> {
                if (objectMetadataList.isEmpty()) {
                    logger.info("No stream set objects to force split");
                    return;
                }
                forceSplitObjects(streamMetadataList, objectMetadataList);
                cf.complete(null);
            }, forceSplitThreadPool);
        }, forceSplitThreadPool).exceptionally(ex -> {
            logger.error("Error while force split all stream set objects ", ex);
            cf.completeExceptionally(ex);
            return null;
        }));

        return cf;
    }

    /**
     * Split specified stream set object into stream objects.
     *
     * @param streamMetadataList metadata of opened streams
     * @param objectMetadata stream set object to split
     * @return List of CompletableFuture of StreamObject
     */
    private Collection<CompletableFuture<StreamObject>> splitStreamSetObject(List<StreamMetadata> streamMetadataList, S3ObjectMetadata objectMetadata) {
        if (objectMetadata == null) {
            return new ArrayList<>();
        }

        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList,
                Collections.singletonList(objectMetadata), s3Operator);
        if (streamDataBlocksMap.isEmpty()) {
            // object not exist, metadata is out of date
            logger.warn("Object {} not exist, metadata is out of date", objectMetadata.objectId());
            return new ArrayList<>();
        }
        List<StreamDataBlock> streamDataBlocks = streamDataBlocksMap.get(objectMetadata.objectId());
        if (streamDataBlocks.isEmpty()) {
            // object is empty, metadata is out of date
            logger.warn("Object {} is empty, metadata is out of date", objectMetadata.objectId());
            return new ArrayList<>();
        }

        List<Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>>> groupedDataBlocks = new ArrayList<>();
        List<List<StreamDataBlock>> groupedStreamDataBlocks = CompactionUtils.groupStreamDataBlocks(streamDataBlocks);
        for (List<StreamDataBlock> group : groupedStreamDataBlocks) {
            groupedDataBlocks.add(new ImmutablePair<>(group, new CompletableFuture<>()));
        }
        logger.info("Force split object {}, expect to generate {} stream objects", objectMetadata.objectId(), groupedDataBlocks.size());

        int index = 0;
        while (index < groupedDataBlocks.size()) {
            List<Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>>> batchGroup = new ArrayList<>();
            long readSize = 0;
            while (index < groupedDataBlocks.size()) {
                Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>> group = groupedDataBlocks.get(index);
                List<StreamDataBlock> groupedStreamDataBlock = group.getLeft();
                long size = groupedStreamDataBlock.get(groupedStreamDataBlock.size() - 1).getBlockEndPosition() -
                        groupedStreamDataBlock.get(0).getBlockStartPosition();
                if (readSize + size > compactionCacheSize) {
                    break;
                }
                readSize += size;
                batchGroup.add(group);
                index++;
            }
            if (batchGroup.isEmpty()) {
                logger.error("Force split object failed, not be able to read any data block, maybe compactionCacheSize is too small");
                return new ArrayList<>();
            }
            // prepare N stream objects at one time
            objectManager.prepareObject(batchGroup.size(), TimeUnit.MINUTES.toMillis(CompactionConstants.S3_OBJECT_TTL_MINUTES))
                    .thenComposeAsync(objectId -> {
                        List<StreamDataBlock> blocksToRead = batchGroup.stream().flatMap(p -> p.getLeft().stream()).toList();
                        DataBlockReader reader = new DataBlockReader(objectMetadata, s3Operator, compactionBucket, bucketCallbackScheduledExecutor);
                        // batch read
                        reader.readBlocks(blocksToRead, Math.min(CompactionConstants.S3_OBJECT_MAX_READ_BATCH, networkBandwidth));

                        List<CompletableFuture<Void>> cfs = new ArrayList<>();
                        for (Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>> pair : batchGroup) {
                            List<StreamDataBlock> blocks = pair.getLeft();
                            DataBlockWriter writer = new DataBlockWriter(objectId, s3Operator, config.objectPartSize());
                            for (StreamDataBlock block : blocks) {
                                writer.write(block);
                            }
                            long finalObjectId = objectId;
                            cfs.add(writer.close().thenAccept(v -> {
                                StreamObject streamObject = new StreamObject();
                                streamObject.setObjectId(finalObjectId);
                                streamObject.setStreamId(blocks.get(0).getStreamId());
                                streamObject.setStartOffset(blocks.get(0).getStartOffset());
                                streamObject.setEndOffset(blocks.get(blocks.size() - 1).getEndOffset());
                                streamObject.setObjectSize(writer.size());
                                pair.getValue().complete(streamObject);
                            }));
                            objectId++;
                        }
                        return CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0]));
                    }, forceSplitThreadPool)
                    .exceptionally(ex -> {
                        //TODO: clean up buffer
                        logger.error("Force split object failed", ex);
                        for (Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>> pair : groupedDataBlocks) {
                            pair.getValue().completeExceptionally(ex);
                        }
                        return null;
                    }).join();
        }

        return groupedDataBlocks.stream().map(Pair::getValue).collect(Collectors.toList());
    }

    CommitStreamSetObjectRequest buildSplitRequest(List<StreamMetadata> streamMetadataList, S3ObjectMetadata objectToSplit) {
        Collection<CompletableFuture<StreamObject>> cfs = splitStreamSetObject(streamMetadataList, objectToSplit);
        if (cfs.isEmpty()) {
            logger.error("Force split object {} failed, no stream object generated", objectToSplit.objectId());
            return null;
        }

        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        request.setObjectId(-1L);

        // wait for all force split objects to complete
        cfs.stream().map(e -> {
            try {
                return e.join();
            } catch (Exception ignored) {
                return null;
            }
        }).filter(Objects::nonNull).forEach(request::addStreamObject);

        request.setCompactedObjectIds(Collections.singletonList(objectToSplit.objectId()));
        if (!sanityCheckCompactionResult(streamMetadataList, Collections.singletonList(objectToSplit), request)) {
            logger.error("Sanity check failed, force split result is illegal");
            return null;
        }

        return request;
    }

    CommitStreamSetObjectRequest buildCompactRequest(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectsToCompact) {
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        Set<Long> compactedObjectIds = new HashSet<>();
        logger.info("{} stream set objects as compact candidates, total compaction size: {}",
                objectsToCompact.size(), objectsToCompact.stream().mapToLong(S3ObjectMetadata::objectSize).sum());
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, objectsToCompact, s3Operator);
        long now = System.currentTimeMillis();
        Set<Long> excludedObjectIds = new HashSet<>();
        List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, excludedObjectIds);
        logger.info("Analyze compaction plans complete, cost {}ms", System.currentTimeMillis() - now);
        logCompactionPlans(compactionPlans, excludedObjectIds);
        objectsToCompact = objectsToCompact.stream().filter(e -> !excludedObjectIds.contains(e.objectId())).collect(Collectors.toList());
        executeCompactionPlans(request, compactionPlans, objectsToCompact);
        compactionPlans.forEach(c -> c.streamDataBlocksMap().values().forEach(v -> v.forEach(b -> compactedObjectIds.add(b.getObjectId()))));

        request.setCompactedObjectIds(new ArrayList<>(compactedObjectIds));
        List<S3ObjectMetadata> compactedObjectMetadata = objectsToCompact.stream()
                .filter(e -> compactedObjectIds.contains(e.objectId())).toList();
        if (!sanityCheckCompactionResult(streamMetadataList, compactedObjectMetadata, request)) {
            logger.error("Sanity check failed, compaction result is illegal");
            return null;
        }

        return request;
    }

    boolean sanityCheckCompactionResult(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> compactedObjects,
                                        CommitStreamSetObjectRequest request) {
        Map<Long, StreamMetadata> streamMetadataMap = streamMetadataList.stream()
                .collect(Collectors.toMap(StreamMetadata::getStreamId, e -> e));
        Map<Long, S3ObjectMetadata> objectMetadataMap = compactedObjects.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));

        List<StreamOffsetRange> compactedStreamOffsetRanges = new ArrayList<>();
        request.getStreamRanges().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        request.getStreamObjects().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        Map<Long, List<StreamOffsetRange>> sortedStreamOffsetRanges = compactedStreamOffsetRanges.stream()
                .collect(Collectors.groupingBy(StreamOffsetRange::getStreamId));
        sortedStreamOffsetRanges.replaceAll((k, v) -> sortAndMerge(v));
        for (long objectId : request.getCompactedObjectIds()) {
            S3ObjectMetadata metadata = objectMetadataMap.get(objectId);
            for (StreamOffsetRange streamOffsetRange : metadata.getOffsetRanges()) {
                if (!streamMetadataMap.containsKey(streamOffsetRange.getStreamId()) ||
                        streamOffsetRange.getEndOffset() <= streamMetadataMap.get(streamOffsetRange.getStreamId()).getStartOffset()) {
                    // skip stream offset range that has been trimmed
                    continue;
                }
                if (!sortedStreamOffsetRanges.containsKey(streamOffsetRange.getStreamId())) {
                    logger.error("Sanity check failed, stream {} is missing after compact", streamOffsetRange.getStreamId());
                    return false;
                }
                boolean contained = false;
                for (StreamOffsetRange compactedStreamOffsetRange : sortedStreamOffsetRanges.get(streamOffsetRange.getStreamId())) {
                    if (streamOffsetRange.getStartOffset() >= compactedStreamOffsetRange.getStartOffset()
                            && streamOffsetRange.getEndOffset() <= compactedStreamOffsetRange.getEndOffset()) {
                        contained = true;
                        break;
                    }
                }
                if (!contained) {
                    logger.error("Sanity check failed, object {} offset range {} is missing after compact", objectId, streamOffsetRange);
                    return false;
                }
            }
        }

        return true;
    }

    private List<StreamOffsetRange> sortAndMerge(List<StreamOffsetRange> streamOffsetRangeList) {
        if (streamOffsetRangeList.size() < 2) {
            return streamOffsetRangeList;
        }
        long streamId = streamOffsetRangeList.get(0).getStreamId();
        Collections.sort(streamOffsetRangeList);
        List<StreamOffsetRange> mergedList = new ArrayList<>();
        long start  = -1L;
        long end = -1L;
        for (int i = 0; i < streamOffsetRangeList.size() - 1; i++) {
            StreamOffsetRange curr = streamOffsetRangeList.get(i);
            StreamOffsetRange next = streamOffsetRangeList.get(i + 1);
            if (start == -1) {
                start = curr.getStartOffset();
                end = curr.getEndOffset();
            }
            if (curr.getEndOffset() < next.getStartOffset()) {
                mergedList.add(new StreamOffsetRange(curr.getStreamId(), start, end));
                start = next.getStartOffset();
            }
            end = next.getEndOffset();
        }
        mergedList.add(new StreamOffsetRange(streamId, start, end));

        return mergedList;
    }

    Map<Boolean, List<S3ObjectMetadata>> convertS3Objects(List<S3ObjectMetadata> streamSetObjectMetadata) {
        return new HashMap<>(streamSetObjectMetadata.stream()
                .collect(Collectors.partitioningBy(e -> (System.currentTimeMillis() - e.dataTimeInMs())
                        >= TimeUnit.MINUTES.toMillis(this.forceSplitObjectPeriod))));
    }

    void executeCompactionPlans(CommitStreamSetObjectRequest request, List<CompactionPlan> compactionPlans, List<S3ObjectMetadata> s3ObjectMetadata)
            throws IllegalArgumentException {
        if (compactionPlans.isEmpty()) {
            return;
        }
        Map<Long, S3ObjectMetadata> s3ObjectMetadataMap = s3ObjectMetadata.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        List<StreamDataBlock> sortedStreamDataBlocks = new ArrayList<>();
        for (int i = 0; i < compactionPlans.size(); i++) {
            // iterate over each compaction plan
            CompactionPlan compactionPlan = compactionPlans.get(i);
            long totalSize = compactionPlan.streamDataBlocksMap().values().stream().flatMap(List::stream)
                    .mapToLong(StreamDataBlock::getBlockSize).sum();
            logger.info("Compaction progress {}/{}, read from {} stream set objects, total size: {}", i + 1, compactionPlans.size(),
                    compactionPlan.streamDataBlocksMap().size(), totalSize);
            for (Map.Entry<Long, List<StreamDataBlock>> streamDataBlocEntry : compactionPlan.streamDataBlocksMap().entrySet()) {
                S3ObjectMetadata metadata = s3ObjectMetadataMap.get(streamDataBlocEntry.getKey());
                List<StreamDataBlock> streamDataBlocks = streamDataBlocEntry.getValue();
                DataBlockReader reader = new DataBlockReader(metadata, s3Operator, compactionBucket, bucketCallbackScheduledExecutor);
                reader.readBlocks(streamDataBlocks, Math.min(CompactionConstants.S3_OBJECT_MAX_READ_BATCH, networkBandwidth));
            }
            List<CompletableFuture<StreamObject>> streamObjectCFList = new ArrayList<>();
            CompletableFuture<Void> streamSetObjectCF = null;
            for (CompactedObject compactedObject : compactionPlan.compactedObjects()) {
                if (compactedObject.type() == CompactionType.COMPACT) {
                    sortedStreamDataBlocks.addAll(compactedObject.streamDataBlocks());
                    streamSetObjectCF = uploader.chainWriteStreamSetObject(streamSetObjectCF, compactedObject);
                } else {
                    streamObjectCFList.add(uploader.writeStreamObject(compactedObject));
                }
            }

            // wait for all stream objects and stream set object part to be uploaded
            try {
                if (streamSetObjectCF != null) {
                    // wait for all writes done
                    streamSetObjectCF.thenAccept(v -> uploader.forceUploadStreamSetObject()).join();
                }
                streamObjectCFList.stream().map(CompletableFuture::join).forEach(request::addStreamObject);
            } catch (Exception ex) {
                //TODO: clean up buffer
                logger.error("Error while uploading compaction objects", ex);
                uploader.reset();
                throw new IllegalArgumentException("Error while uploading compaction objects", ex);
            }
        }
        List<ObjectStreamRange> objectStreamRanges = CompactionUtils.buildObjectStreamRange(sortedStreamDataBlocks);
        objectStreamRanges.forEach(request::addStreamRange);
        request.setObjectId(uploader.getStreamSetObjectId());
        // set stream set object id to be the first object id of compacted objects
        request.setOrderId(s3ObjectMetadata.get(0).objectId());
        request.setObjectSize(uploader.complete());
        uploader.reset();
    }
}
