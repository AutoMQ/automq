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
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CompactionManager {
    private final static Logger LOGGER = LoggerFactory.getLogger(CompactionManager.class);
    private final ObjectManager objectManager;
    private final StreamMetadataManager streamMetadataManager;
    private final S3Operator s3Operator;
    private final CompactionAnalyzer compactionAnalyzer;
    private final ScheduledExecutorService executorService;
    private final CompactionUploader uploader;
    private final long compactionCacheSize;
    private final double executionScoreThreshold;
    private final long streamSplitSize;
    private final TokenBucketThrottle networkInThrottle;

    public CompactionManager(KafkaConfig config, ObjectManager objectManager, StreamMetadataManager streamMetadataManager, S3Operator s3Operator) {
        this.objectManager = objectManager;
        this.streamMetadataManager = streamMetadataManager;
        this.s3Operator = s3Operator;
        this.compactionCacheSize = config.s3ObjectCompactionCacheSize();
        this.executionScoreThreshold = config.s3ObjectCompactionExecutionScoreThreshold();
        this.streamSplitSize = config.s3ObjectCompactionStreamSplitSize();
        this.networkInThrottle = new TokenBucketThrottle(config.s3ObjectCompactionNWInBandwidth());
        this.uploader = new CompactionUploader(objectManager, s3Operator, config);
        this.compactionAnalyzer = new CompactionAnalyzer(compactionCacheSize, executionScoreThreshold, streamSplitSize, s3Operator);
        this.executorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("object-compaction-manager"));
        this.executorService.scheduleWithFixedDelay(this::compact, 0, 600, TimeUnit.SECONDS);
    }

    public void shutdown() {
        this.executorService.shutdown();
        this.networkInThrottle.stop();
        this.uploader.stop();
    }

    public CompletableFuture<CompactResult> compact() {
        List<S3WALObjectMetadata> s3ObjectMetadata = this.streamMetadataManager.getWALObjects();
        try {
            Map<Long, S3WALObjectMetadata> s3ObjectMetadataMap = s3ObjectMetadata.stream()
                    .collect(Collectors.toMap(e -> e.getWalObject().objectId(), e -> e));
            List<CompactionPlan> compactionPlans = this.compactionAnalyzer.analyze(s3ObjectMetadata);
            if (compactionPlans.isEmpty()) {
                return CompletableFuture.completedFuture(CompactResult.SKIPPED);
            }
            CommitWALObjectRequest request = new CommitWALObjectRequest();
            for (CompactionPlan compactionPlan : compactionPlans) {
                List<CompletableFuture<StreamObject>> streamObjectFutureList = new ArrayList<>();
                List<CompletableFuture<List<ObjectStreamRange>>> walObjStreamRangeFutureList = new ArrayList<>();
                // iterate over each compaction plan
                for (Map.Entry<Long, List<StreamDataBlock>> streamDataBlocEntry : compactionPlan.streamDataBlocksMap().entrySet()) {
                    S3ObjectMetadata metadata = s3ObjectMetadataMap.get(streamDataBlocEntry.getKey()).getObjectMetadata();
                    List<StreamDataBlock> streamDataBlocks = streamDataBlocEntry.getValue();
                    List<DataBlockReader.DataBlockIndex> blockIndices = buildBlockIndicesFromStreamDataBlock(streamDataBlocks);
                    networkInThrottle.throttle(streamDataBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum());
                    DataBlockReader reader = new DataBlockReader(metadata, s3Operator);
                    reader.readBlocks(blockIndices).thenAccept(dataBlocks -> {
                        for (int i = 0; i < blockIndices.size(); i++) {
                            StreamDataBlock streamDataBlock = streamDataBlocks.get(i);
                            streamDataBlock.getDataCf().complete(dataBlocks.get(i).buffer());
                        }
                    }).exceptionally(ex -> {
                        LOGGER.error("read on invalid object {}, ex ", metadata.key(), ex);
                        for (int i = 0; i < blockIndices.size(); i++) {
                            StreamDataBlock streamDataBlock = streamDataBlocks.get(i);
                            streamDataBlock.getDataCf().completeExceptionally(ex);
                        }
                        return null;
                    });
                }
                for (CompactedObject compactedObject : compactionPlan.compactedObjects()) {
                    if (compactedObject.type() == CompactionType.COMPACT) {
                        walObjStreamRangeFutureList.add(uploader.writeWALObject(compactedObject));
                    } else {
                        streamObjectFutureList.add(uploader.writeStreamObject(compactedObject));
                    }
                }
                // wait for all stream objects and wal object parts to be uploaded
                try {
                    walObjStreamRangeFutureList.stream().map(CompletableFuture::join).forEach(e -> e.forEach(request::addStreamRange));
                    streamObjectFutureList.stream().map(CompletableFuture::join).forEach(request::addStreamObject);
                } catch (Exception ex) {
                    LOGGER.error("Error while uploading compaction objects", ex);
                    uploader.reset();
                    return CompletableFuture.failedFuture(ex);
                }
                compactionPlan.streamDataBlocksMap().values().forEach(e -> e.forEach(StreamDataBlock::free));
            }
            request.setObjectId(uploader.getWALObjectId());
            // set wal object id to be the first object id of compacted objects
            request.setOrderId(s3ObjectMetadata.get(0).getObjectMetadata().getObjectId());
            request.setCompactedObjectIds(s3ObjectMetadata.stream().map(s -> s.getObjectMetadata().getObjectId()).collect(Collectors.toList()));
            uploader.getWalObjectWriter().close().thenAccept(nil -> request.setObjectSize(uploader.getWalObjectWriter().size())).join();
            uploader.reset();
            return objectManager.commitWALObject(request).thenApply(nil -> {
                LOGGER.info("Compaction success, WAL object id: {}, size: {}, stream object num: {}",
                        request.getObjectId(), request.getObjectSize(), request.getStreamObjects().size());
                return CompactResult.SUCCESS;
            });
        } catch (Exception e) {
            LOGGER.error("Error while analyzing compaction objects", e);
            return CompletableFuture.failedFuture(e);
        }

    }

    private List<DataBlockReader.DataBlockIndex> buildBlockIndicesFromStreamDataBlock(List<StreamDataBlock> streamDataBlocks) {
        List<DataBlockReader.DataBlockIndex> blockIndices = new ArrayList<>();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            blockIndices.add(new DataBlockReader.DataBlockIndex(streamDataBlock.getBlockId(), streamDataBlock.getBlockPosition(),
                    streamDataBlock.getBlockSize(), streamDataBlock.getRecordCount()));
        }
        return blockIndices;
    }

}
