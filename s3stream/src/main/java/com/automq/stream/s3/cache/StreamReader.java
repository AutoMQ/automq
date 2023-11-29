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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.Threads;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReader.class);
    public static final Integer MAX_OBJECT_READER_SIZE = 100 * 1024 * 1024; // 100MB;
    private static final Integer READ_OBJECT_INDEX_STEP = 2;
    private final S3Operator s3Operator;
    private final ObjectManager objectManager;
    private final ObjectReaderLRUCache objectReaders;
    private final DataBlockReadAccumulator dataBlockReadAccumulator;
    private final BlockCache blockCache;
    private final ExecutorService streamReaderExecutor = Threads.newFixedThreadPoolWithMonitor(
            2,
            "s3-stream-reader",
            false,
            LOGGER);
    private final ExecutorService backgroundExecutor = Threads.newFixedThreadPoolWithMonitor(
            2,
            "s3-stream-reader-background",
            true,
            LOGGER);

    public StreamReader(S3Operator operator, ObjectManager objectManager, BlockCache blockCache) {
        this.s3Operator = operator;
        this.objectManager = objectManager;
        this.objectReaders = new ObjectReaderLRUCache(MAX_OBJECT_READER_SIZE);
        this.dataBlockReadAccumulator = new DataBlockReadAccumulator();
        this.blockCache = blockCache;
    }

    public CompletableFuture<List<StreamRecordBatch>> readAhead(long streamId, long startOffset, long endOffset, int maxBytes, ReadAheadAgent agent, boolean isAsync) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] read ahead, stream={}, {}-{}, maxBytes={}, isAsync={}", streamId, startOffset, endOffset, maxBytes, isAsync);
        }
        ReadContext context = new ReadContext(startOffset, maxBytes);
        TimerUtil timer = new TimerUtil();
        return getDataBlockIndices(streamId, endOffset, context).thenComposeAsync(v -> {
            if (context.streamDataBlocksPair.isEmpty()) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] stream={}, {}-{}, read data block indices cost: {} ms", streamId, startOffset, endOffset,
                        timer.elapsedAs(TimeUnit.MILLISECONDS));
            }
            // concurrently read all data blocks
            //TODO: acquire quota single data block, so that they can be release on put block cache complete
            List<CompletableFuture<Void>> cfList = new ArrayList<>();
            Map<String, List<StreamRecordBatch>> recordsMap = new ConcurrentHashMap<>();
            List<String> sortedDataBlockKeyList = new ArrayList<>();
            boolean firstDataBlock = true;
            int totalSize = 0;
            for (Pair<Long, List<StreamDataBlock>> entry : context.streamDataBlocksPair) {
                long objectId = entry.getKey();
                ObjectReader objectReader = context.objectReaderMap.get(objectId);
                for (StreamDataBlock streamDataBlock : entry.getValue()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] stream={}, {}-{}, read data block {} from {} [{}, {}), size={}",
                                streamId, startOffset, endOffset, streamDataBlock.getBlockId(), objectReader.objectKey(),
                                streamDataBlock.getBlockStartPosition(), streamDataBlock.getBlockEndPosition(), streamDataBlock.getBlockSize());
                    }
                    totalSize += streamDataBlock.getBlockSize();
                    boolean finalFirstDataBlock = firstDataBlock;
                    String dataBlockKey = objectId + "-" + streamDataBlock.getBlockId();
                    sortedDataBlockKeyList.add(dataBlockKey);
                    cfList.add(dataBlockReadAccumulator.readDataBlock(objectReader, streamDataBlock.dataBlockIndex())
                            .thenAcceptAsync(dataBlock -> {
                                if (dataBlock.records().isEmpty()) {
                                    return;
                                }

                                if (!isAsync) {
                                    // retain records to be returned
                                    dataBlock.records().forEach(StreamRecordBatch::retain);
                                    recordsMap.put(dataBlockKey, dataBlock.records());
                                }

                                // retain records to be put into block cache
                                dataBlock.records().forEach(StreamRecordBatch::retain);
                                if (isAsync && finalFirstDataBlock) {
                                    long firstOffset = dataBlock.records().get(0).getBaseOffset();
                                    blockCache.put(streamId, firstOffset, context.lastOffset, dataBlock.records());
                                } else {
                                    blockCache.put(streamId, dataBlock.records());
                                }
                                dataBlock.release();
                            }, backgroundExecutor));
                    if (firstDataBlock) {
                        firstDataBlock = false;
                    }
                }
            }

            int finalTotalSize = totalSize;
            return CompletableFuture.allOf(cfList.toArray(CompletableFuture[]::new)).thenApply(vv -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] read data from s3 complete, stream={}, {}-{}, cost: {} ms", streamId, startOffset, endOffset,
                             timer.elapsedAs(TimeUnit.MILLISECONDS));
                }
                // release all ObjectReaders
                for (Map.Entry<Long, ObjectReader> entry : context.objectReaderMap.entrySet()) {
                    entry.getValue().release();
                }
                context.objectReaderMap.clear();

                List<StreamRecordBatch> recordsToReturn = new LinkedList<>();
                if (!isAsync) {
                    List<StreamRecordBatch> totalRecords = new ArrayList<>();
                    for (String dataBlockKey : sortedDataBlockKeyList) {
                        List<StreamRecordBatch> recordList = recordsMap.get(dataBlockKey);
                        if (recordList != null) {
                            totalRecords.addAll(recordList);
                        }
                    }
                    // collect records to return
                    int remainBytes = maxBytes;
                    for (StreamRecordBatch record : totalRecords) {
                        if (remainBytes <= 0 || record.getLastOffset() <= startOffset || record.getBaseOffset() >= endOffset) {
                            // release record that won't be returned
                            record.release();
                            continue;
                        }
                        recordsToReturn.add(record);
                        remainBytes -= record.size();
                    }
                    long lastReadOffset = recordsToReturn.isEmpty() ? totalRecords.get(0).getBaseOffset()
                            : recordsToReturn.get(recordsToReturn.size() - 1).getLastOffset();
                    blockCache.setReadAheadRecord(streamId, lastReadOffset, context.lastOffset);
                }
                agent.updateReadAheadResult(context.lastOffset, finalTotalSize);
                return recordsToReturn;
            });
        }, streamReaderExecutor);
    }

    private CompletableFuture<Void> getDataBlockIndices(long streamId, long endOffset, ReadContext context) {
        CompletableFuture<Boolean /* empty objects */> getObjectsCf = CompletableFuture.completedFuture(false);
        if (context.objectIndex >= context.objects.size()) {
            getObjectsCf = objectManager
                    .getObjects(streamId, context.nextStartOffset, endOffset, READ_OBJECT_INDEX_STEP)
                    .orTimeout(1, TimeUnit.MINUTES)
                    .thenApply(objects -> {
                        context.objects = objects;
                        context.objectIndex = 0;
                        if (context.objects.isEmpty()) {
                            if (endOffset == -1L) { // background read ahead
                                return true;
                            } else {
                                LOGGER.error("[BUG] fail to read, expect objects not empty, streamId={}, startOffset={}, endOffset={}",
                                        streamId, context.nextStartOffset, endOffset);
                                throw new IllegalStateException("fail to read, expect objects not empty");
                            }
                        }
                        return false;
                    });
        }
        CompletableFuture<ObjectReader.FindIndexResult> findIndexCf = getObjectsCf.thenComposeAsync(emptyObjects -> {
            if (emptyObjects) {
                return CompletableFuture.completedFuture(null);
            }
            S3ObjectMetadata objectMetadata = context.objects.get(context.objectIndex);
            ObjectReader reader = getObjectReader(objectMetadata);
            context.objectReaderMap.put(objectMetadata.objectId(), reader);
            return reader.find(streamId, context.nextStartOffset, endOffset, context.nextMaxBytes);
        }, streamReaderExecutor);
        return findIndexCf.thenComposeAsync(findIndexResult -> {
            if (findIndexResult == null) {
                return CompletableFuture.completedFuture(null);
            }
            List<StreamDataBlock> streamDataBlocks = findIndexResult.streamDataBlocks();
            if (streamDataBlocks.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            S3ObjectMetadata objectMetadata = context.objects.get(context.objectIndex);
            long lastOffset = streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset();
            context.lastOffset = Math.max(lastOffset, context.lastOffset);
            context.streamDataBlocksPair.add(new ImmutablePair<>(objectMetadata.objectId(), streamDataBlocks));
            if (findIndexResult.isFulfilled()) {
                return CompletableFuture.completedFuture(null);
            }
            context.nextStartOffset = findIndexResult.nextStartOffset();
            context.nextMaxBytes = findIndexResult.nextMaxBytes();
            context.objectIndex++;
            return getDataBlockIndices(streamId, endOffset, context);
        }, streamReaderExecutor);
    }

    private ObjectReader getObjectReader(S3ObjectMetadata metadata) {
        synchronized (objectReaders) {
            ObjectReader objectReader = objectReaders.get(metadata.objectId());
            if (objectReader == null) {
                objectReader = new ObjectReader(metadata, s3Operator);
                objectReaders.put(metadata.objectId(), objectReader);
            }
            return objectReader.retain();
        }
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        List<Pair<Long, List<StreamDataBlock>>> streamDataBlocksPair;
        Map<Long, ObjectReader> objectReaderMap;
        int objectIndex;
        long nextStartOffset;
        int nextMaxBytes;
        long lastOffset;

        public ReadContext(long startOffset, int maxBytes) {
            this.objects = new LinkedList<>();
            this.objectIndex = 0;
            this.streamDataBlocksPair = new LinkedList<>();
            this.objectReaderMap = new HashMap<>();
            this.nextStartOffset = startOffset;
            this.nextMaxBytes = maxBytes;
        }

    }
}
