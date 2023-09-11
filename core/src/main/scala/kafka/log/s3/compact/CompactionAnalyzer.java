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

import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.CompactedObjectBuilder;
import kafka.log.s3.compact.objects.CompactionType;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.compact.operator.DataBlockReader;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.S3WALObjectMetadata;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CompactionAnalyzer {
    private final static Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CompactionAnalyzer.class);
    private final long compactionCacheSize;
    private final double executionScoreThreshold;
    private final long streamSplitSize;
    private final S3Operator s3Operator;

    public CompactionAnalyzer(long compactionCacheSize, double executionScoreThreshold, long streamSplitSize, S3Operator s3Operator) {
        this.compactionCacheSize = compactionCacheSize;
        this.executionScoreThreshold = executionScoreThreshold;
        this.streamSplitSize = streamSplitSize;
        this.s3Operator = s3Operator;
    }

    public List<CompactionPlan> analyze(List<S3WALObjectMetadata> objectMetadataList) {
        List<CompactionPlan> compactionPlans = new ArrayList<>();
        try {
            List<CompactedObjectBuilder> compactedObjectBuilders = buildCompactedObjects(objectMetadataList);
            List<CompactedObject> compactedObjects = new ArrayList<>();
            CompactedObjectBuilder compactedWALObjectBuilder = null;
            long totalSize = 0L;
            for (int i = 0; i < compactedObjectBuilders.size(); ) {
                CompactedObjectBuilder compactedObjectBuilder = compactedObjectBuilders.get(i);
                if (totalSize + compactedObjectBuilder.totalBlockSize() > compactionCacheSize) {
                    if (shouldSplitObject(compactedObjectBuilder)) {
                        // split object to fit into cache
                        int endOffset = 0;
                        for (int j = 0; j < compactedObjectBuilder.streamDataBlocks().size(); j++) {
                            if (totalSize + compactedObjectBuilder.streamDataBlocks().get(j).getBlockSize() > compactionCacheSize) {
                                endOffset = j;
                                break;
                            }
                        }
                        if (endOffset != 0) {
                            CompactedObjectBuilder builder = compactedObjectBuilder.split(0, endOffset);
                            compactedWALObjectBuilder = addOrMergeCompactedObject(builder, compactedObjects, compactedWALObjectBuilder);
                        }
                    }
                    compactionPlans.add(generateCompactionPlan(compactedObjects, compactedWALObjectBuilder));
                    compactedObjects.clear();
                    compactedWALObjectBuilder = null;
                    totalSize = 0;
                } else {
                    // object fits into cache size
                    compactedWALObjectBuilder = addOrMergeCompactedObject(compactedObjectBuilder, compactedObjects, compactedWALObjectBuilder);
                    totalSize += compactedObjectBuilder.totalBlockSize();
                    i++;
                }

            }
            if (!compactedObjects.isEmpty()) {
                compactionPlans.add(generateCompactionPlan(compactedObjects, compactedWALObjectBuilder));
            }
            return compactionPlans;
        } catch (Exception e) {
            LOGGER.error("Error while analyzing compaction plan", e);
        }
        return compactionPlans;
    }

    private CompactedObjectBuilder addOrMergeCompactedObject(CompactedObjectBuilder compactedObjectBuilder,
                                                             List<CompactedObject> compactedObjects,
                                                             CompactedObjectBuilder compactedWALObjectBuilder) {
        if (compactedObjectBuilder.type() == CompactionType.SPLIT) {
            compactedObjects.add(compactedObjectBuilder.build());
        } else {
            if (compactedWALObjectBuilder == null) {
                compactedWALObjectBuilder = new CompactedObjectBuilder();
            }
            compactedWALObjectBuilder.merge(compactedObjectBuilder);
        }
        return compactedWALObjectBuilder;
    }

    private boolean shouldSplitObject(CompactedObjectBuilder compactedObjectBuilder) {
        //TODO: split object depends on available cache size and current object size
        //TODO: use multipart upload to upload split stream object
        return true;
    }

    private CompactionPlan generateCompactionPlan(List<CompactedObject> compactedObjects, CompactedObjectBuilder compactedWALObject) {
        if (compactedWALObject != null) {
            compactedObjects.add(compactedWALObject.build());
        }
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = new HashMap<>();
        for (CompactedObject compactedObject : compactedObjects) {
            for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
                streamDataBlockMap.computeIfAbsent(streamDataBlock.getObjectId(), k -> new ArrayList<>()).add(streamDataBlock);
            }
        }
        for (List<StreamDataBlock> dataBlocks : streamDataBlockMap.values()) {
            dataBlocks.sort(StreamDataBlock.BLOCK_POSITION_COMPARATOR);
        }

        return new CompactionPlan(new ArrayList<>(compactedObjects), streamDataBlockMap);
    }

    public List<CompactedObjectBuilder> buildCompactedObjects(List<S3WALObjectMetadata> objects) {
        List<StreamDataBlock> streamDataBlocks = blockWaitObjectIndices(objects);
        if (!shouldCompact(streamDataBlocks)) {
            return new ArrayList<>();
        }
        return compactObjects(sortStreamRangePositions(streamDataBlocks));
    }

    List<StreamDataBlock> blockWaitObjectIndices(List<S3WALObjectMetadata> objectMetadataList) {
        Map<Long, S3WALObject> s3WALObjectMap = objectMetadataList.stream()
                .collect(Collectors.toMap(e -> e.getWalObject().objectId(), S3WALObjectMetadata::getWalObject));
        List<CompletableFuture<List<StreamDataBlock>>> objectStreamRangePositionFutures = new ArrayList<>();
        for (S3WALObjectMetadata walObjectMetadata : objectMetadataList) {
            DataBlockReader dataBlockReader = new DataBlockReader(walObjectMetadata.getObjectMetadata(), s3Operator);
            dataBlockReader.parseDataBlockIndex();
            objectStreamRangePositionFutures.add(dataBlockReader.getDataBlockIndex());
        }
        return objectStreamRangePositionFutures.stream().flatMap(f -> {
            try {
                List<StreamDataBlock> streamDataBlocks = f.join();
                List<StreamDataBlock> validStreamDataBlocks = new ArrayList<>();
                S3WALObject s3WALObject = s3WALObjectMap.get(streamDataBlocks.get(0).getObjectId());
                // filter out invalid stream data blocks in case metadata is inconsistent with S3 index block
                for (StreamDataBlock streamDataBlock : streamDataBlocks) {
                    if (s3WALObject.intersect(streamDataBlock.getStreamId(), streamDataBlock.getStartOffset(), streamDataBlock.getEndOffset())) {
                        validStreamDataBlocks.add(streamDataBlock);
                    }
                }
                return validStreamDataBlocks.stream();
            } catch (Exception ex) {
                // continue compaction without invalid object
                LOGGER.error("Read on invalid object ", ex);
                return null;
            }
        }).collect(Collectors.toList());
    }

    private List<CompactedObjectBuilder> compactObjects(List<StreamDataBlock> streamDataBlocks) {
        List<CompactedObjectBuilder> compactedObjectBuilders = new ArrayList<>();
        CompactedObjectBuilder builder = new CompactedObjectBuilder();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            if (builder.lastStreamId() == -1L) {
                // init state
                builder.addStreamDataBlock(streamDataBlock);
            } else if (builder.lastStreamId() == streamDataBlock.getStreamId()) {
                // data range from same stream
                if (streamDataBlock.getStartOffset() > builder.lastOffset()) {
                    // data range is not continuous, split current object as StreamObject
                    builder = splitObject(builder, compactedObjectBuilders);
                    builder.addStreamDataBlock(streamDataBlock);
                } else if (streamDataBlock.getStartOffset() == builder.lastOffset()) {
                    builder.addStreamDataBlock(streamDataBlock);
                } else {
                    // should not go there
                    LOGGER.error("FATAL ERROR: illegal stream range position, last offset: {}, curr: {}",
                            builder.lastOffset(), streamDataBlock);
                    return new ArrayList<>();
                }
            } else {
                builder = splitAndAddBlock(builder, streamDataBlock, compactedObjectBuilders);
            }
        }
        compactedObjectBuilders.add(builder);
        return compactedObjectBuilders;
    }

    boolean shouldCompact(List<StreamDataBlock> streamDataBlocks) {
        // do compact if there is any stream with data placed in more than one WAL objects
        Map<Long, Integer> streamIdToDistinctObjectMap = streamDataBlocks.stream()
                .collect(Collectors.groupingBy(StreamDataBlock::getStreamId, Collectors.mapping(StreamDataBlock::getObjectId, Collectors.toSet())))
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        return streamIdToDistinctObjectMap.values().stream().filter(v -> v > 1).count() > 1;
    }

    private CompactedObjectBuilder splitAndAddBlock(CompactedObjectBuilder builder,
                                                    StreamDataBlock streamDataBlock,
                                                    List<CompactedObjectBuilder> compactedObjectBuilders) {
        if (builder.currStreamBlockSize() > streamSplitSize) {
            builder = splitObject(builder, compactedObjectBuilders);
        }
        builder.addStreamDataBlock(streamDataBlock);
        return builder;
    }

    private CompactedObjectBuilder splitObject(CompactedObjectBuilder builder,
                                               List<CompactedObjectBuilder> compactedObjectBuilders) {
        CompactedObjectBuilder splitBuilder = builder.splitCurrentStream();
        splitBuilder.setType(CompactionType.SPLIT);
        if (builder.totalBlockSize() != 0) {
            compactedObjectBuilders.add(builder);
        }
        compactedObjectBuilders.add(splitBuilder);
        builder = new CompactedObjectBuilder();
        return builder;
    }

    List<StreamDataBlock> sortStreamRangePositions(List<StreamDataBlock> streamDataBlocks) {
        //TODO: use merge sort
        Map<Long, List<StreamDataBlock>> sortedStreamObjectMap = new TreeMap<>();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            sortedStreamObjectMap.computeIfAbsent(streamDataBlock.getStreamId(), k -> new ArrayList<>()).add(streamDataBlock);
        }
        return sortedStreamObjectMap.values().stream().flatMap(list -> {
            list.sort(StreamDataBlock.STREAM_OFFSET_COMPARATOR);
            return list.stream();
        }).collect(Collectors.toList());
    }

}
