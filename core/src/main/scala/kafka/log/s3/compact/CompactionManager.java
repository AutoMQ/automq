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
package kafka.log.s3.compact;

import io.netty.util.concurrent.DefaultThreadFactory;
import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.compact.operator.DataBlockReader;
import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.S3Operator;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CompactionManager {
    private final Logger logger;
    private final ObjectManager objectManager;
    private final StreamMetadataManager streamMetadataManager;
    private final S3Operator s3Operator;
    private final CompactionAnalyzer compactionAnalyzer;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private final CompactionUploader uploader;
    private final long compactionCacheSize;
    private final double executionScoreThreshold;
    private final long streamSplitSize;
    private final int compactionInterval;
    private final int forceSplitObjectPeriod;
    private final TokenBucketThrottle networkInThrottle;

    public CompactionManager(KafkaConfig config, ObjectManager objectManager, StreamMetadataManager streamMetadataManager, S3Operator s3Operator) {
        this.logger = new LogContext(String.format("[CompactionManager id=%d] ", config.brokerId())).logger(CompactionManager.class);
        this.objectManager = objectManager;
        this.streamMetadataManager = streamMetadataManager;
        this.s3Operator = s3Operator;
        this.compactionInterval = config.s3ObjectCompactionInterval();
        this.compactionCacheSize = config.s3ObjectCompactionCacheSize();
        this.executionScoreThreshold = config.s3ObjectCompactionExecutionScoreThreshold();
        this.streamSplitSize = config.s3ObjectCompactionStreamSplitSize();
        this.forceSplitObjectPeriod = config.s3ObjectCompactionForceSplitPeriod();
        this.networkInThrottle = new TokenBucketThrottle(config.s3ObjectCompactionNWInBandwidth());
        this.uploader = new CompactionUploader(objectManager, s3Operator, config);
        this.compactionAnalyzer = new CompactionAnalyzer(compactionCacheSize, executionScoreThreshold, streamSplitSize,
                s3Operator, new LogContext(String.format("[CompactionAnalyzer id=%d] ", config.brokerId())));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("object-compaction-manager"));
        this.executorService = Executors.newFixedThreadPool(8, new DefaultThreadFactory("force-split-executor"));
        this.logger.info("Compaction manager initialized with config: compactionInterval: {} min, compactionCacheSize: {} bytes, " +
                        "executionScoreThreshold: {}, streamSplitSize: {} bytes, forceSplitObjectPeriod: {} min",
                compactionInterval, compactionCacheSize, executionScoreThreshold, streamSplitSize, forceSplitObjectPeriod);
    }

    public void start() {
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            logger.info("Compaction started");
            long start = System.currentTimeMillis();
            this.compact()
                    .thenAccept(result -> logger.info("Compaction complete, total cost {} ms, result {}",
                            System.currentTimeMillis() - start, result))
                    .exceptionally(ex -> {
                        logger.error("Compaction failed, cost {} ms, ", System.currentTimeMillis() - start, ex);
                        return null;
                    });
        }, 1, this.compactionInterval, TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.networkInThrottle.stop();
        this.uploader.stop();
    }

    public CompletableFuture<CompactResult> compact() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        logger.info("Get {} WAL objects from metadata", s3ObjectMetadata.size());
        Map<Boolean, List<S3WALObjectMetadata>> objectMetadataFilterMap = s3ObjectMetadata.stream()
                .collect(Collectors.partitioningBy(e -> (System.currentTimeMillis() - e.getWalObject().dataTimeInMs())
                        >= TimeUnit.MINUTES.toMillis(this.forceSplitObjectPeriod)));
        // force split objects that exists for too long
        logger.info("{} WAL objects need force split", objectMetadataFilterMap.get(true).size());
        long splitStart = System.currentTimeMillis();
        splitWALObjects(objectMetadataFilterMap.get(true)).thenAccept(v ->
                logger.info("Force split {} objects, time cost: {} ms", objectMetadataFilterMap.get(true).size(), System.currentTimeMillis() - splitStart));

        try {
            logger.info("{} WAL objects as compact candidates", objectMetadataFilterMap.get(false).size());
            long compactionStart = System.currentTimeMillis();
            List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(objectMetadataFilterMap.get(false));
            logger.info("Analyze compaction plans complete, {} plans generated", compactionPlans.size());
            if (compactionPlans.isEmpty()) {
                return CompletableFuture.completedFuture(CompactResult.SKIPPED);
            }
            CommitWALObjectRequest request = buildCompactRequest(compactionPlans, s3ObjectMetadata);
            logger.info("Build compact request complete, time cost: {} ms, start committing objects", System.currentTimeMillis() - compactionStart);
            return objectManager.commitWALObject(request).thenApply(nil -> {
                logger.info("Commit compact request succeed, WAL object id: {}, size: {}, stream object num: {}, time cost: {} ms",
                        request.getObjectId(), request.getObjectSize(), request.getStreamObjects().size(), System.currentTimeMillis() - compactionStart);
                return CompactResult.SUCCESS;
            });
        } catch (Exception e) {
            logger.error("Error while compaction objects", e);
            return CompletableFuture.failedFuture(e);
        }

    }

    public CompletableFuture<Void> forceSplitAll() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        //TODO: deal with metadata delay
        this.scheduledExecutorService.execute(() -> splitWALObjects(this.streamMetadataManager.getWALObjects())
                .thenAccept(v -> cf.complete(null))
                .exceptionally(ex -> {
                    cf.completeExceptionally(ex);
                    return null;
                })
        );
        return cf;
    }

    CompletableFuture<Void> splitWALObjects(List<S3WALObjectMetadata> objectMetadataList) {
        if (objectMetadataList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(objectMetadataList, s3Operator);
        List<CompletableFuture<Void>> splitFutureList = new ArrayList<>();
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlocksMap.entrySet()) {
            List<StreamDataBlock> streamDataBlocks = entry.getValue();
            for (StreamDataBlock streamDataBlock : streamDataBlocks) {
                splitFutureList.add(objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30))
                        .thenAcceptAsync(objectId -> {
                            logger.info("Split {} to {}", streamDataBlock, objectId);
                            //TODO: implement this
                        }, executorService).exceptionally(ex -> {
                            logger.error("Prepare object failed", ex);
                            return null;
                        })
                );
            }
        }
        return CompletableFuture.allOf(splitFutureList.toArray(new CompletableFuture[0]));
    }

    CommitWALObjectRequest buildCompactRequest(List<CompactionPlan> compactionPlans, List<S3WALObjectMetadata> s3ObjectMetadata)
            throws IllegalArgumentException {
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        Map<Long, S3WALObjectMetadata> s3ObjectMetadataMap = s3ObjectMetadata.stream()
                .collect(Collectors.toMap(e -> e.getWalObject().objectId(), e -> e));
        for (CompactionPlan compactionPlan : compactionPlans) {
            // iterate over each compaction plan
            for (Map.Entry<Long, List<StreamDataBlock>> streamDataBlocEntry : compactionPlan.streamDataBlocksMap().entrySet()) {
                S3ObjectMetadata metadata = s3ObjectMetadataMap.get(streamDataBlocEntry.getKey()).getObjectMetadata();
                List<StreamDataBlock> streamDataBlocks = streamDataBlocEntry.getValue();
                List<DataBlockReader.DataBlockIndex> blockIndices = CompactionUtils.buildBlockIndicesFromStreamDataBlock(streamDataBlocks);
                networkInThrottle.throttle(streamDataBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum());
                DataBlockReader reader = new DataBlockReader(metadata, s3Operator);
                reader.readBlocks(blockIndices).thenAccept(dataBlocks -> {
                    for (int i = 0; i < blockIndices.size(); i++) {
                        StreamDataBlock streamDataBlock = streamDataBlocks.get(i);
                        streamDataBlock.getDataCf().complete(dataBlocks.get(i).buffer());
                    }
                }).exceptionally(ex -> {
                    logger.error("read on invalid object {}, ex ", metadata.key(), ex);
                    for (int i = 0; i < blockIndices.size(); i++) {
                        StreamDataBlock streamDataBlock = streamDataBlocks.get(i);
                        streamDataBlock.getDataCf().completeExceptionally(ex);
                    }
                    return null;
                });
            }
            List<CompletableFuture<StreamObject>> streamObjectCFList = new ArrayList<>();
            CompletableFuture<CompletableFuture<Void>> walObjectCF = null;
            List<ObjectStreamRange> objectStreamRanges = new ArrayList<>();
            for (CompactedObject compactedObject : compactionPlan.compactedObjects()) {
                if (compactedObject.type() == CompactionType.COMPACT) {
                    objectStreamRanges = CompactionUtils.buildObjectStreamRange(compactedObject);
                    walObjectCF = uploader.writeWALObject(compactedObject);
                } else {
                    streamObjectCFList.add(uploader.writeStreamObject(compactedObject));
                }
            }
            // wait for all stream objects and wal object part to be uploaded
            try {
                if (walObjectCF != null) {
                    // wait for all blocks to be uploaded or added to waiting list
                    CompletableFuture<Void> writeObjectCF = walObjectCF.join();
                    // force upload all blocks still in waiting list
                    uploader.forceUploadWAL();
                    // wait for all blocks to be uploaded
                    writeObjectCF.join();
                    objectStreamRanges.forEach(request::addStreamRange);
                }
                streamObjectCFList.stream().map(CompletableFuture::join).forEach(request::addStreamObject);
            } catch (Exception ex) {
                logger.error("Error while uploading compaction objects", ex);
                uploader.reset();
                throw new IllegalArgumentException("Error while uploading compaction objects", ex);
            }
        }
        request.setObjectId(uploader.getWALObjectId());
        // set wal object id to be the first object id of compacted objects
        request.setOrderId(s3ObjectMetadata.get(0).getObjectMetadata().objectId());
        request.setCompactedObjectIds(s3ObjectMetadata.stream().map(s -> s.getObjectMetadata().objectId()).collect(Collectors.toList()));
        request.setObjectSize(uploader.completeWAL());
        uploader.reset();
        return request;
    }
}
