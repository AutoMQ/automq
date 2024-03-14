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
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.LogContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
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
import org.slf4j.Logger;

public class CompactionManager {
    private final Logger logger;
    private final Logger s3ObjectLogger;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final CompactionAnalyzer compactionAnalyzer;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private final CompactionUploader uploader;
    private final Config kafkaConfig;
    private final long compactionCacheSize;
    private final double executionScoreThreshold;
    private final long streamSplitSize;
    private final int maxObjectNumToCompact;
    private final int compactionInterval;
    private final int forceSplitObjectPeriod;
    private final boolean s3ObjectLogEnable;
    private final TokenBucketThrottle networkInThrottle;

    public CompactionManager(Config config, ObjectManager objectManager) {
        this(config, objectManager, new DefaultS3Operator(config.s3Endpont(), config.s3Region(),
            config.s3Bucket(), config.s3ForcePathStyle(), config.s3AccessKey(), config.s3SecretKey()));
    }

    public CompactionManager(Config config, ObjectManager objectManager, S3Operator s3Operator) {
        String logPrefix = String.format("[CompactionManager id=%d] ", config.brokerId());
        this.logger = new LogContext(logPrefix).logger(CompactionManager.class);
        this.s3ObjectLogger = S3ObjectLogger.logger(logPrefix);
        this.kafkaConfig = config;
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.compactionInterval = config.s3ObjectCompactionInterval();
        this.compactionCacheSize = config.s3ObjectCompactionCacheSize();
        this.executionScoreThreshold = config.s3ObjectCompactionExecutionScoreThreshold();
        this.streamSplitSize = config.s3ObjectCompactionStreamSplitSize();
        this.forceSplitObjectPeriod = config.s3ObjectCompactionForceSplitPeriod();
        this.maxObjectNumToCompact = config.s3ObjectCompactionMaxObjectNum();
        this.s3ObjectLogEnable = config.s3ObjectLogEnable();
        this.networkInThrottle = new TokenBucketThrottle(config.s3ObjectCompactionNWInBandwidth());
        this.uploader = new CompactionUploader(objectManager, s3Operator, config);
        this.compactionAnalyzer = new CompactionAnalyzer(compactionCacheSize, executionScoreThreshold, streamSplitSize,
                new LogContext(String.format("[CompactionAnalyzer id=%d] ", config.brokerId())));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("object-compaction-manager"));
        this.executorService = Executors.newFixedThreadPool(8, new DefaultThreadFactory("force-split-executor"));
        this.logger.info("Compaction manager initialized with config: compactionInterval: {} min, compactionCacheSize: {} bytes, " +
                        "executionScoreThreshold: {}, streamSplitSize: {} bytes, forceSplitObjectPeriod: {} min, maxObjectNumToCompact: {}",
                compactionInterval, compactionCacheSize, executionScoreThreshold, streamSplitSize, forceSplitObjectPeriod, maxObjectNumToCompact);
    }

    public void start() {
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                logger.info("Compaction started");
                long start = System.currentTimeMillis();
                this.compact()
                        .thenAccept(result -> logger.info("Compaction complete, total cost {} ms, result {}",
                                System.currentTimeMillis() - start, result))
                        .exceptionally(ex -> {
                            logger.error("Compaction failed, cost {} ms, ", System.currentTimeMillis() - start, ex);
                            return null;
                        });
            } catch (Exception ex) {
                logger.error("Error while compacting objects ", ex);
            }
        }, 1, this.compactionInterval, TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.networkInThrottle.stop();
        this.uploader.stop();
    }

    public CompletableFuture<CompactResult> compact() {
        return this.objectManager.getServerObjects().thenCompose(s3ObjectMetadata -> {
            logger.info("Get {} WAL objects from metadata", s3ObjectMetadata.size());

            long start = System.currentTimeMillis();
            CommitWALObjectRequest request = buildCompactRequest(s3ObjectMetadata);

            if (request == null) {
                return CompletableFuture.completedFuture(CompactResult.FAILED);
            }

            if (request.getCompactedObjectIds().isEmpty()) {
                logger.info("No need to compact");
                return CompletableFuture.completedFuture(CompactResult.SKIPPED);
            }

            logger.info("Build compact request complete, time cost: {} ms, start committing objects", System.currentTimeMillis() - start);
            return objectManager.commitWALObject(request).thenApply(resp -> {
                logger.info("Commit compact request succeed, {} objects compacted, WAL object id: {}, size: {}, stream object num: {}, time cost: {} ms",
                        request.getCompactedObjectIds().size(), request.getObjectId(), request.getObjectSize(), request.getStreamObjects().size(), System.currentTimeMillis() - start);
                if (s3ObjectLogEnable) {
                    s3ObjectLogger.trace("[Compact] {}", request);
                }
                return CompactResult.SUCCESS;
            });
        });
    }

    private void logCompactionPlans(List<CompactionPlan> compactionPlans) {
        if (compactionPlans.isEmpty()) {
            logger.info("No compaction plans to execute");
            return;
        }
        long streamObjectNum = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.SPLIT)
                        .count())
                .sum();
        long walObjectSize = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.COMPACT)
                        .mapToLong(CompactedObject::size)
                        .sum())
                .sum();
        logger.info("Compaction plans: expect to generate {} StreamObject, 1 WAL object with size {} in {} iterations",
                streamObjectNum, walObjectSize, compactionPlans.size());
    }

    public CompletableFuture<Void> forceSplitAll() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        //TODO: deal with metadata delay
        this.scheduledExecutorService.execute(() -> this.objectManager.getServerObjects().thenAcceptAsync(objects -> {
            List<CompletableFuture<StreamObject>> cfList = splitWALObjects(objects);
            List<StreamObject> streamObjects = cfList.stream().map(e -> {
                try {
                    return e.join();
                } catch (Exception ex) {
                    logger.error("Error while force split object ", ex);
                }
                return null;
            }).collect(Collectors.toList());
            if (streamObjects.stream().anyMatch(Objects::isNull)) {
                logger.error("Force split WAL objects failed");
                cf.completeExceptionally(new RuntimeException("Force split WAL objects failed"));
                return;
            }
            CommitWALObjectRequest request = new CommitWALObjectRequest();
            streamObjects.forEach(request::addStreamObject);
            request.setCompactedObjectIds(objects.stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
            objectManager.commitWALObject(request).thenAccept(resp -> {
                logger.info("Force split {} WAL objects succeed, produce {} stream objects", objects.size(), streamObjects.size());
                if (s3ObjectLogEnable) {
                    s3ObjectLogger.trace("[ForceSplit] {}", request);
                }
                cf.complete(null);
            }).exceptionally(ex -> {
                logger.error("Force split all WAL objects failed", ex);
                cf.completeExceptionally(ex);
                return null;
            });
        }, executorService));

        return cf;
    }


    List<CompletableFuture<StreamObject>> splitWALObjects(List<S3ObjectMetadata> objectMetadataList) {
        if (objectMetadataList.isEmpty()) {
            return new ArrayList<>();
        }

        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(objectMetadataList, s3Operator);
        List<CompletableFuture<StreamObject>> splitFutureList = new ArrayList<>();
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlocksMap.entrySet()) {
            List<StreamDataBlock> streamDataBlocks = entry.getValue();
            for (StreamDataBlock streamDataBlock : streamDataBlocks) {
                CompletableFuture<StreamObject> streamObjectCf = new CompletableFuture<>();
                splitFutureList.add(streamObjectCf);
                objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30))
                        .thenAcceptAsync(objectId -> {
                            logger.debug("Split {} to {}", streamDataBlock, objectId);
                            DataBlockWriter writer = new DataBlockWriter(objectId, s3Operator, kafkaConfig.s3ObjectPartSize());
                            writer.copyWrite(streamDataBlock);
                            writer.close().thenAccept(v -> {
                                StreamObject streamObject = new StreamObject();
                                streamObject.setObjectId(objectId);
                                streamObject.setStreamId(streamDataBlock.getStreamId());
                                streamObject.setStartOffset(streamDataBlock.getStartOffset());
                                streamObject.setEndOffset(streamDataBlock.getEndOffset());
                                streamObject.setObjectSize(writer.size());
                                streamObjectCf.complete(streamObject);
                            });
                        }, executorService).exceptionally(ex -> {
                            logger.error("Prepare object failed", ex);
                            streamObjectCf.completeExceptionally(ex);
                            return null;
                        });
            }
        }
        return splitFutureList;
    }

    CommitWALObjectRequest buildCompactRequest(List<S3ObjectMetadata> s3ObjectMetadata) {
        Map<Boolean, List<S3ObjectMetadata>> objectMetadataFilterMap = filterS3Objects(s3ObjectMetadata);
        List<S3ObjectMetadata> objectsToSplit = objectMetadataFilterMap.get(true);
        List<S3ObjectMetadata> objectsToCompact = objectMetadataFilterMap.get(false);
        // force split objects that exists for too long
        logger.info("{} WAL objects to be force split, total split size {}", objectsToSplit.size(),
                objectMetadataFilterMap.get(true).stream().mapToLong(S3ObjectMetadata::objectSize).sum());
        List<CompletableFuture<StreamObject>> forceSplitCfs = splitWALObjects(objectsToSplit);

        CommitWALObjectRequest request = new CommitWALObjectRequest();
        List<CompactionPlan> compactionPlans = new ArrayList<>();
        try {
            logger.info("{} WAL objects as compact candidates, total compaction size: {}",
                    objectsToCompact.size(), objectsToCompact.stream().mapToLong(S3ObjectMetadata::objectSize).sum());
            Map<Long, List<StreamDataBlock>> streamDataBlockMap = CompactionUtils.blockWaitObjectIndices(objectsToCompact, s3Operator);
            compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap);
            logCompactionPlans(compactionPlans);
            compactWALObjects(request, compactionPlans, s3ObjectMetadata);
        } catch (Exception e) {
            logger.error("Error while compacting objects ", e);
        }

        forceSplitCfs.stream().map(e -> {
            try {
                return e.join();
            } catch (Exception ex) {
                logger.error("Force split StreamObject failed ", ex);
                return null;
            }
        }).filter(Objects::nonNull).forEach(request::addStreamObject);

        Set<Long> compactedObjectIds = new HashSet<>();
        objectMetadataFilterMap.get(true).forEach(e -> compactedObjectIds.add(e.objectId()));
        compactionPlans.forEach(c -> c.streamDataBlocksMap().values().forEach(v -> v.forEach(b -> compactedObjectIds.add(b.getObjectId()))));
        request.setCompactedObjectIds(new ArrayList<>(compactedObjectIds));

        if (!sanityCheckCompactionResult(objectsToSplit, objectsToCompact, request)) {
            logger.error("Sanity check failed, compaction result is illegal");
            return null;
        }

        return request;
    }

    boolean sanityCheckCompactionResult(List<S3ObjectMetadata> objectsToSplit, List<S3ObjectMetadata> objectsToCompact,
                                        CommitWALObjectRequest request) {

        Map<Long, S3ObjectMetadata> objectMetadataMap = objectsToCompact.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        objectMetadataMap.putAll(objectsToSplit.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e)));

        List<StreamOffsetRange> compactedStreamOffsetRanges = new ArrayList<>();
        request.getStreamRanges().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        request.getStreamObjects().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        Map<Long, List<StreamOffsetRange>> sortedStreamOffsetRanges = compactedStreamOffsetRanges.stream()
                .collect(Collectors.groupingBy(StreamOffsetRange::getStreamId));
        sortedStreamOffsetRanges.replaceAll((k, v) -> sortAndMerge(v));
        for (long objectId : request.getCompactedObjectIds()) {
            S3ObjectMetadata metadata = objectMetadataMap.get(objectId);
            for (StreamOffsetRange streamOffsetRange : metadata.getOffsetRanges()) {
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

    Map<Boolean, List<S3ObjectMetadata>> filterS3Objects(List<S3ObjectMetadata> s3WALObjectMetadata) {
        Map<Boolean, List<S3ObjectMetadata>> objectMetadataFilterMap = new HashMap<>(s3WALObjectMetadata.stream()
                .collect(Collectors.partitioningBy(e -> (System.currentTimeMillis() - e.dataTimeInMs())
                        >= TimeUnit.MINUTES.toMillis(this.forceSplitObjectPeriod))));

        objectMetadataFilterMap.replaceAll((k, v) -> {
            if (v.size() > maxObjectNumToCompact) {
                return new ArrayList<>(v.subList(0, maxObjectNumToCompact));
            }
            return v;
        });
        return objectMetadataFilterMap;
    }

    void compactWALObjects(CommitWALObjectRequest request, List<CompactionPlan> compactionPlans, List<S3ObjectMetadata> s3ObjectMetadata)
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
            logger.info("Compaction progress {}/{}, read from {} WALs, total size: {}", i + 1, compactionPlans.size(),
                    compactionPlan.streamDataBlocksMap().size(), totalSize);
            for (Map.Entry<Long, List<StreamDataBlock>> streamDataBlocEntry : compactionPlan.streamDataBlocksMap().entrySet()) {
                S3ObjectMetadata metadata = s3ObjectMetadataMap.get(streamDataBlocEntry.getKey());
                List<StreamDataBlock> streamDataBlocks = streamDataBlocEntry.getValue();
                DataBlockReader reader = new DataBlockReader(metadata, s3Operator);
                reader.readBlocks(streamDataBlocks, networkInThrottle);
            }
            List<CompletableFuture<StreamObject>> streamObjectCFList = new ArrayList<>();
            CompletableFuture<Void> walObjectCF = null;
            for (CompactedObject compactedObject : compactionPlan.compactedObjects()) {
                if (compactedObject.type() == CompactionType.COMPACT) {
                    sortedStreamDataBlocks.addAll(compactedObject.streamDataBlocks());
                    walObjectCF = uploader.chainWriteWALObject(walObjectCF, compactedObject);
                } else {
                    streamObjectCFList.add(uploader.writeStreamObject(compactedObject));
                }
            }

            // wait for all stream objects and wal object part to be uploaded
            try {
                if (walObjectCF != null) {
                    // wait for all writes done
                    walObjectCF.thenAccept(v -> uploader.forceUploadWAL()).join();
                }
                streamObjectCFList.stream().map(CompletableFuture::join).forEach(request::addStreamObject);
            } catch (Exception ex) {
                logger.error("Error while uploading compaction objects", ex);
                uploader.reset();
                throw new IllegalArgumentException("Error while uploading compaction objects", ex);
            }
        }
        List<ObjectStreamRange> objectStreamRanges = CompactionUtils.buildObjectStreamRange(sortedStreamDataBlocks);
        objectStreamRanges.forEach(request::addStreamRange);
        request.setObjectId(uploader.getWALObjectId());
        // set wal object id to be the first object id of compacted objects
        request.setOrderId(s3ObjectMetadata.get(0).objectId());
        request.setObjectSize(uploader.completeWAL());
        uploader.reset();
    }
}
