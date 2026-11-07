/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.utils.LogContext;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactionAnalyzer {
    private final Logger logger;
    private final long compactionCacheSize;
    private final long streamSplitSize;
    private final int maxStreamNumInStreamSet;
    private final int maxStreamObjectNum;

    public CompactionAnalyzer(long compactionCacheSize, long streamSplitSize, int maxStreamNumInStreamSet,
        int maxStreamObjectNum) {
        this(compactionCacheSize, streamSplitSize, maxStreamNumInStreamSet, maxStreamObjectNum, new LogContext("[CompactionAnalyzer]"));
    }

    public CompactionAnalyzer(long compactionCacheSize, long streamSplitSize,
        int maxStreamNumInStreamSet, int maxStreamObjectNum, LogContext logContext) {
        this.logger = logContext.logger(CompactionAnalyzer.class);
        this.compactionCacheSize = compactionCacheSize;
        this.streamSplitSize = streamSplitSize;
        this.maxStreamNumInStreamSet = maxStreamNumInStreamSet;
        this.maxStreamObjectNum = maxStreamObjectNum;
    }

    public List<CompactionPlan> analyze(Map<Long, List<StreamDataBlock>> streamDataBlockMap,
        Set<Long> excludedObjectIds) {
        if (streamDataBlockMap.isEmpty()) {
            return Collections.emptyList();
        }
        streamDataBlockMap = filterBlocksToCompact(streamDataBlockMap);
        this.logger.info("{} stream set objects to compact after filter", streamDataBlockMap.size());
        if (streamDataBlockMap.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<CompactedObjectBuilder> compactedObjectBuilders = groupObjectWithLimits(streamDataBlockMap, excludedObjectIds);
            return generatePlanWithCacheLimit(compactedObjectBuilders);
        } catch (Exception e) {
            logger.error("Error while analyzing compaction plan", e);
        }
        return Collections.emptyList();
    }

    /**
     * Group stream data blocks into different compaction type ({@link CompactionType#COMPACT} & {@link CompactionType#SPLIT})
     * with compaction limitation ({@code maxStreamObjectNum} and {@code maxStreamNumInStreamSet}).
     *
     * @param streamDataBlockMap stream data blocks map, key: object id, value: stream data blocks
     * @param excludedObjectIds  objects that are excluded from compaction because of compaction limitation
     * @return list of {@link CompactedObjectBuilder}
     */
    List<CompactedObjectBuilder> groupObjectWithLimits(Map<Long, List<StreamDataBlock>> streamDataBlockMap,
        Set<Long> excludedObjectIds) {
        List<StreamDataBlock> sortedStreamDataBlocks = CompactionUtils.sortStreamRangePositions(streamDataBlockMap);
        List<CompactedObjectBuilder> compactedObjectBuilders = new ArrayList<>();
        CompactionStats stats = null;
        int streamNumInStreamSet = -1;
        int streamObjectNum = -1;
        do {
            final Set<Long> objectsToRemove = new HashSet<>();
            if (stats != null) {
                if (streamObjectNum > maxStreamObjectNum) {
                    logger.warn("Stream object num {} exceeds limit {}, try to reduce number of objects to compact", streamObjectNum, maxStreamObjectNum);
                    addObjectsToRemove(CompactionType.SPLIT, compactedObjectBuilders, stats, objectsToRemove);
                } else {
                    logger.warn("Stream number {} exceeds limit {}, try to reduce number of objects to compact", streamNumInStreamSet, maxStreamNumInStreamSet);
                    addObjectsToRemove(CompactionType.COMPACT, compactedObjectBuilders, stats, objectsToRemove);
                }
                if (objectsToRemove.isEmpty()) {
                    logger.error("Unable to derive objects to exclude, compaction failed");
                    return new ArrayList<>();
                }
            }
            if (!objectsToRemove.isEmpty()) {
                logger.info("Excluded objects {} for compaction", objectsToRemove);
                excludedObjectIds.addAll(objectsToRemove);
            }
            sortedStreamDataBlocks.removeIf(e -> objectsToRemove.contains(e.getObjectId()));
            objectsToRemove.forEach(streamDataBlockMap::remove);
            streamDataBlockMap = filterBlocksToCompact(streamDataBlockMap);
            if (streamDataBlockMap.isEmpty()) {
                logger.warn("No viable objects to compact after exclusion");
                return new ArrayList<>();
            }
            compactedObjectBuilders = compactObjects(sortedStreamDataBlocks);
            stats = CompactionStats.of(compactedObjectBuilders);
            streamNumInStreamSet = stats.getStreamRecord().streamNumInStreamSet();
            streamObjectNum = stats.getStreamRecord().streamObjectNum();
            logger.info("Current stream num in stream set: {}, max: {}, stream object num: {}, max: {}", streamNumInStreamSet, maxStreamNumInStreamSet, streamObjectNum, maxStreamObjectNum);
        }
        while (streamNumInStreamSet > maxStreamNumInStreamSet || streamObjectNum > maxStreamObjectNum);

        return compactedObjectBuilders;
    }

    /**
     * Find objects to exclude from compaction.
     *
     * @param compactionType          {@link CompactionType#COMPACT} means to exclude objects to reduce stream number in stream set object;
     *                                {@link CompactionType#SPLIT} means to exclude objects to reduce stream object number
     * @param compactedObjectBuilders all compacted object builders
     * @param stats                   compaction stats
     * @param objectsToRemove         objects to remove
     */
    private void addObjectsToRemove(CompactionType compactionType, List<CompactedObjectBuilder> compactedObjectBuilders,
        CompactionStats stats, Set<Long> objectsToRemove) {
        List<CompactedObjectBuilder> sortedCompactedObjectIndexList = new ArrayList<>();
        for (CompactedObjectBuilder compactedObjectBuilder : compactedObjectBuilders) {
            // find all compacted objects of the same type
            if (compactedObjectBuilder.type() == compactionType) {
                sortedCompactedObjectIndexList.add(compactedObjectBuilder);
            }
        }
        if (compactionType == CompactionType.SPLIT) {
            // try to find out one stream object to remove
            sortedCompactedObjectIndexList.sort(new StreamObjectComparator(stats.getS3ObjectToCompactedObjectNumMap()));
            // remove compacted object with the highest priority
            CompactedObjectBuilder compactedObjectToRemove = sortedCompactedObjectIndexList.get(0);
            // add all objects in the compacted object
            objectsToRemove.addAll(compactedObjectToRemove.streamDataBlocks().stream()
                .map(StreamDataBlock::getObjectId)
                .collect(Collectors.toSet()));
        } else {
            // try to find out one stream to remove
            // key: stream id, value: id of all objects that contains the stream
            Map<Long, Set<Long>> streamObjectIdsMap = new HashMap<>();
            // key: object id, value: id of all streams from the object, used to describe the dispersion of streams in the object
            Map<Long, Set<Long>> objectStreamIdsMap = new HashMap<>();
            for (CompactedObjectBuilder compactedObjectBuilder : sortedCompactedObjectIndexList) {
                for (StreamDataBlock streamDataBlock : compactedObjectBuilder.streamDataBlocks()) {
                    Set<Long> objectIds = streamObjectIdsMap.computeIfAbsent(streamDataBlock.getStreamId(), k -> new HashSet<>());
                    objectIds.add(streamDataBlock.getObjectId());
                    Set<Long> streamIds = objectStreamIdsMap.computeIfAbsent(streamDataBlock.getObjectId(), k -> new HashSet<>());
                    streamIds.add(streamDataBlock.getStreamId());
                }
            }
            List<Pair<Long, Integer>> sortedStreamObjectStatsList = new ArrayList<>();
            for (Map.Entry<Long, Set<Long>> entry : streamObjectIdsMap.entrySet()) {
                long streamId = entry.getKey();
                Set<Long> objectIds = entry.getValue();
                int objectStreamNum = 0;
                for (long objectId : objectIds) {
                    objectStreamNum += objectStreamIdsMap.get(objectId).size();
                }
                sortedStreamObjectStatsList.add(new ImmutablePair<>(streamId, objectStreamNum));
            }
            sortedStreamObjectStatsList.sort(Comparator.comparingInt(Pair::getRight));
            // remove stream with minimum object dispersion
            objectsToRemove.addAll(streamObjectIdsMap.get(sortedStreamObjectStatsList.get(0).getKey()));
        }
    }

    /**
     * Generate compaction plan with cache size limit.
     *
     * @param compactedObjectBuilders compacted object builders
     * @return list of {@link CompactionPlan} with each plan's memory consumption is less than {@code compactionCacheSize}
     */
    List<CompactionPlan> generatePlanWithCacheLimit(List<CompactedObjectBuilder> compactedObjectBuilders) {
        List<CompactionPlan> compactionPlans = new ArrayList<>();
        List<CompactedObject> compactedObjects = new ArrayList<>();
        CompactedObjectBuilder compactedStreamSetObjectBuilder = null;
        long totalSize = 0L;
        int compactionOrder = 0;
        for (int i = 0; i < compactedObjectBuilders.size(); ) {
            CompactedObjectBuilder compactedObjectBuilder = compactedObjectBuilders.get(i);
            if (totalSize + compactedObjectBuilder.totalBlockSize() > compactionCacheSize) {
                if (shouldSplitObject(compactedObjectBuilder)) {
                    // split object to fit into cache
                    int endOffset = 0;
                    long tmpSize = totalSize;
                    for (int j = 0; j < compactedObjectBuilder.streamDataBlocks().size(); j++) {
                        tmpSize += compactedObjectBuilder.streamDataBlocks().get(j).getBlockSize();
                        if (tmpSize > compactionCacheSize) {
                            endOffset = j;
                            break;
                        }
                    }
                    if (endOffset != 0) {
                        CompactedObjectBuilder builder = compactedObjectBuilder.split(0, endOffset);
                        compactedStreamSetObjectBuilder = addOrMergeCompactedObject(builder, compactedObjects, compactedStreamSetObjectBuilder);
                    }
                }
                compactionPlans.add(generateCompactionPlan(compactionOrder++, compactedObjects, compactedStreamSetObjectBuilder));
                compactedObjects.clear();
                compactedStreamSetObjectBuilder = null;
                totalSize = 0;
            } else {
                // object fits into cache size
                compactedStreamSetObjectBuilder = addOrMergeCompactedObject(compactedObjectBuilder, compactedObjects, compactedStreamSetObjectBuilder);
                totalSize += compactedObjectBuilder.totalBlockSize();
                i++;
            }

        }
        if (!compactedObjects.isEmpty() || compactedStreamSetObjectBuilder != null) {
            compactionPlans.add(generateCompactionPlan(compactionOrder, compactedObjects, compactedStreamSetObjectBuilder));
        }
        return compactionPlans;
    }

    private CompactedObjectBuilder addOrMergeCompactedObject(CompactedObjectBuilder compactedObjectBuilder,
        List<CompactedObject> compactedObjects,
        CompactedObjectBuilder compactedStreamSetObjectBuilder) {
        if (compactedObjectBuilder.type() == CompactionType.SPLIT) {
            compactedObjects.add(compactedObjectBuilder.build());
        } else {
            if (compactedStreamSetObjectBuilder == null) {
                compactedStreamSetObjectBuilder = new CompactedObjectBuilder();
            }
            compactedStreamSetObjectBuilder.merge(compactedObjectBuilder);
        }
        return compactedStreamSetObjectBuilder;
    }

    private boolean shouldSplitObject(CompactedObjectBuilder compactedObjectBuilder) {
        //TODO: split object depends on available cache size and current object size
        //TODO: use multipart upload to upload split stream object
        return true;
    }

    private CompactionPlan generateCompactionPlan(int order, List<CompactedObject> compactedObjects,
        CompactedObjectBuilder compactedStreamSetObject) {
        if (compactedStreamSetObject != null) {
            compactedObjects.add(compactedStreamSetObject.build());
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

        return new CompactionPlan(order, new ArrayList<>(compactedObjects), streamDataBlockMap);
    }

    /**
     * Iterate through stream data blocks and group them into different {@link CompactionType}.
     *
     * @param streamDataBlocks stream data blocks
     * @return list of {@link CompactedObjectBuilder}
     */
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
                    logger.error("FATAL ERROR: illegal stream range position, last offset: {}, curr: {}",
                        builder.lastOffset(), streamDataBlock);
                    return new ArrayList<>();
                }
            } else {
                builder = splitAndAddBlock(builder, streamDataBlock, compactedObjectBuilders);
            }
        }
        if (builder.currStreamBlockSize() > streamSplitSize) {
            splitObject(builder, compactedObjectBuilders);
        } else {
            compactedObjectBuilders.add(builder);
        }
        return compactedObjectBuilders;
    }

    /**
     * Filter out objects that have at least one stream data block which can be compacted with other objects.
     *
     * @param streamDataBlocksMap stream data blocks map, key: object id, value: stream data blocks
     * @return filtered stream data blocks map
     */
    Map<Long, List<StreamDataBlock>> filterBlocksToCompact(Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        // group stream data blocks by stream id, key: stream id, value: ids of objects that contains this stream
        Map<Long, Set<Long>> streamToObjectIds = streamDataBlocksMap.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.groupingBy(StreamDataBlock::getStreamId, Collectors.mapping(StreamDataBlock::getObjectId, Collectors.toSet())));
        Set<Long> objectIdsToCompact = streamToObjectIds
            .entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .flatMap(e -> e.getValue().stream())
            .collect(Collectors.toSet());
        return streamDataBlocksMap.entrySet().stream()
            .filter(e -> objectIdsToCompact.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

    private abstract static class AbstractCompactedObjectComparator implements Comparator<CompactedObjectBuilder> {
        protected final Map<Long, Integer> objectStatsMap;

        public AbstractCompactedObjectComparator(Map<Long, Integer> objectStatsMap) {
            this.objectStatsMap = objectStatsMap;
        }

        protected int compareCompactedObject(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            return Integer.compare(CompactionUtils.getTotalObjectStats(o1, objectStatsMap),
                CompactionUtils.getTotalObjectStats(o2, objectStatsMap));
        }
    }

    private static class CompactObjectComparator extends AbstractCompactedObjectComparator {
        public CompactObjectComparator(Map<Long, Integer> objectStatsMap) {
            super(objectStatsMap);
        }

        @Override
        public int compare(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            int compare = Integer.compare(o1.totalStreamNum(), o2.totalStreamNum());
            if (compare == 0) {
                return compareCompactedObject(o1, o2);
            }
            return compare;
        }
    }

    private static class StreamObjectComparator extends AbstractCompactedObjectComparator {
        public StreamObjectComparator(Map<Long, Integer> objectStatsMap) {
            super(objectStatsMap);
        }

        @Override
        public int compare(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            int compare = Integer.compare(o1.streamDataBlocks().size(), o2.streamDataBlocks().size());
            if (compare == 0) {
                return compareCompactedObject(o1, o2);
            }
            return compare;
        }
    }

}
