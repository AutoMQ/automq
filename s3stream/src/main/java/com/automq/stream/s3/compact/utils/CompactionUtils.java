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

package com.automq.stream.s3.compact.utils;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.compact.operator.DataBlockWriter;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.S3Operator;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class CompactionUtils {
    public static List<ObjectStreamRange> buildObjectStreamRange(List<StreamDataBlock> streamDataBlocks) {
        List<ObjectStreamRange> objectStreamRanges = new ArrayList<>();
        ObjectStreamRange currObjectStreamRange = null;
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            if (currObjectStreamRange == null) {
                currObjectStreamRange = new ObjectStreamRange(streamDataBlock.getStreamId(), -1L,
                    streamDataBlock.getStartOffset(), streamDataBlock.getEndOffset(), streamDataBlock.getBlockSize());
            } else {
                if (currObjectStreamRange.getStreamId() == streamDataBlock.getStreamId()) {
                    currObjectStreamRange.setEndOffset(streamDataBlock.getEndOffset());
                    currObjectStreamRange.setSize(currObjectStreamRange.getSize() + streamDataBlock.getBlockSize());
                } else {
                    objectStreamRanges.add(currObjectStreamRange);
                    currObjectStreamRange = new ObjectStreamRange(streamDataBlock.getStreamId(), -1L,
                        streamDataBlock.getStartOffset(), streamDataBlock.getEndOffset(), streamDataBlock.getBlockSize());
                }
            }
        }
        if (currObjectStreamRange != null) {
            objectStreamRanges.add(currObjectStreamRange);
        }
        return objectStreamRanges;
    }

    public static Map<Long, List<StreamDataBlock>> blockWaitObjectIndices(List<StreamMetadata> streamMetadataList,
        List<S3ObjectMetadata> objectMetadataList,
        S3Operator s3Operator) {
        return blockWaitObjectIndices(streamMetadataList, objectMetadataList, s3Operator, null);
    }

    public static Map<Long, List<StreamDataBlock>> blockWaitObjectIndices(List<StreamMetadata> streamMetadataList,
        List<S3ObjectMetadata> objectMetadataList,
        S3Operator s3Operator,
        Logger logger) {
        Map<Long, StreamMetadata> streamMetadataMap = streamMetadataList.stream()
            .collect(Collectors.toMap(StreamMetadata::streamId, s -> s));
        Map<Long, CompletableFuture<List<StreamDataBlock>>> objectStreamRangePositionFutures = new HashMap<>();
        for (S3ObjectMetadata objectMetadata : objectMetadataList) {
            DataBlockReader dataBlockReader = new DataBlockReader(objectMetadata, s3Operator);
            dataBlockReader.parseDataBlockIndex();
            objectStreamRangePositionFutures.put(objectMetadata.objectId(), dataBlockReader.getDataBlockIndex());
        }
        return objectStreamRangePositionFutures.entrySet().stream()
            .map(f -> {
                try {
                    List<StreamDataBlock> streamDataBlocks = f.getValue().join();
                    List<StreamDataBlock> validStreamDataBlocks = new ArrayList<>();
                    // filter out invalid stream data blocks in case metadata is inconsistent with S3 index block
                    for (StreamDataBlock streamDataBlock : streamDataBlocks) {
                        if (!streamMetadataMap.containsKey(streamDataBlock.getStreamId())) {
                            // non-exist stream
                            continue;
                        }
                        if (streamDataBlock.getEndOffset() <= streamMetadataMap.get(streamDataBlock.getStreamId()).startOffset()) {
                            // trimmed stream data block
                            continue;
                        }
                        validStreamDataBlocks.add(streamDataBlock);
                    }
                    return new AbstractMap.SimpleEntry<>(f.getKey(), validStreamDataBlocks);
                } catch (Exception ex) {
                    // continue compaction without invalid object
                    if (logger != null) {
                        logger.warn("failed to get data block index for object {}", f.getKey(), ex);
                    }
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    /**
     * Sort stream data blocks by stream id and start offset.
     *
     * @param streamDataBlocksMap streamDataBlocksMap stream data blocks map, key: object id, value: stream data blocks
     * @return                   sorted stream data blocks
     */
    public static List<StreamDataBlock> sortStreamRangePositions(Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        //TODO: use merge sort
        Map<Long, List<StreamDataBlock>> sortedStreamObjectMap = new TreeMap<>();
        for (List<StreamDataBlock> streamDataBlocks : streamDataBlocksMap.values()) {
            streamDataBlocks.forEach(e -> sortedStreamObjectMap.computeIfAbsent(e.getStreamId(), k -> new ArrayList<>()).add(e));
        }
        return sortedStreamObjectMap.values().stream().flatMap(list -> {
            list.sort(StreamDataBlock.STREAM_OFFSET_COMPARATOR);
            return list.stream();
        }).collect(Collectors.toList());
    }

    /**
     * Group stream data blocks by certain conditions.
     *
     * @param streamDataBlocks stream data blocks to be grouped
     * @param predicate        the predicate to check whether a stream data block should be grouped with the previous one
     * @return                 grouped stream data blocks
     */
    public static List<List<StreamDataBlock>> groupStreamDataBlocks(List<StreamDataBlock> streamDataBlocks,
        Predicate<StreamDataBlock> predicate) {
        List<List<StreamDataBlock>> groupedStreamDataBlocks = new ArrayList<>();
        List<StreamDataBlock> currGroup = new ArrayList<>();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            if (predicate.test(streamDataBlock)) {
                currGroup.add(streamDataBlock);
            } else if (!currGroup.isEmpty()) {
                groupedStreamDataBlocks.add(currGroup);
                currGroup = new ArrayList<>();
                currGroup.add(streamDataBlock);
            }
        }
        if (!currGroup.isEmpty()) {
            groupedStreamDataBlocks.add(currGroup);
        }
        return groupedStreamDataBlocks;
    }

    public static List<ObjectStreamRange> buildObjectStreamRangeFromGroup(List<List<StreamDataBlock>> streamDataBlockGroup) {
        List<ObjectStreamRange> objectStreamRanges = new ArrayList<>();

        for (List<StreamDataBlock> streamDataBlocks : streamDataBlockGroup) {
            if (streamDataBlocks.isEmpty()) {
                continue;
            }
            objectStreamRanges.add(new ObjectStreamRange(
                streamDataBlocks.get(0).getStreamId(),
                -1L,
                streamDataBlocks.get(0).getStartOffset(),
                streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset(),
                streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum()));
        }

        return objectStreamRanges;
    }

    public static List<DataBlockIndex> buildDataBlockIndicesFromGroup(List<List<StreamDataBlock>> streamDataBlockGroup) {
        List<DataBlockIndex> dataBlockIndices = new ArrayList<>();

        long blockStartPosition = 0;
        for (List<StreamDataBlock> streamDataBlocks : streamDataBlockGroup) {
            if (streamDataBlocks.isEmpty()) {
                continue;
            }
            dataBlockIndices.add(new DataBlockIndex(
                streamDataBlocks.get(0).getStreamId(),
                streamDataBlocks.get(0).getStartOffset(),
                (int) (streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset() - streamDataBlocks.get(0).getStartOffset()),
                streamDataBlocks.stream().map(StreamDataBlock::dataBlockIndex).mapToInt(DataBlockIndex::recordCount).sum(),
                blockStartPosition,
                streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum()));
            blockStartPosition += streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum();
        }

        return dataBlockIndices;
    }

    public static int getTotalObjectStats(CompactedObjectBuilder o, Map<Long, Integer> objectStatsMap) {
        int totalCompactedObjects = 0;
        for (Long objectId : o.uniqueObjectIds()) {
            totalCompactedObjects += objectStatsMap.get(objectId);
        }
        return totalCompactedObjects;
    }

    public static CompletableFuture<Void> chainWriteDataBlock(DataBlockWriter dataBlockWriter,
        List<StreamDataBlock> streamDataBlocks, ExecutorService executorService) {
        CompletableFuture<Void> cf = null;
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            if (cf == null) {
                cf = streamDataBlock.getDataCf().thenAcceptAsync(data -> dataBlockWriter.write(streamDataBlock), executorService);
            } else {
                cf = cf.thenCompose(nil -> streamDataBlock.getDataCf().thenAcceptAsync(data -> dataBlockWriter.write(streamDataBlock), executorService));
            }
        }
        return cf;
    }
}
