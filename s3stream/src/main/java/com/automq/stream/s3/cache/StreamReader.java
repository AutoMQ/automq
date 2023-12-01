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
import java.util.UUID;
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
    private final Map<DefaultS3BlockCache.ReadAheadTaskKey, CompletableFuture<Void>> inflightReadAheadTaskMap;
    private final InflightReadThrottle inflightReadThrottle;
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

    public StreamReader(S3Operator operator, ObjectManager objectManager, BlockCache blockCache,
                        Map<DefaultS3BlockCache.ReadAheadTaskKey, CompletableFuture<Void>> inflightReadAheadTaskMap,
                        InflightReadThrottle inflightReadThrottle) {
        this.s3Operator = operator;
        this.objectManager = objectManager;
        this.objectReaders = new ObjectReaderLRUCache(MAX_OBJECT_READER_SIZE);
        this.dataBlockReadAccumulator = new DataBlockReadAccumulator();
        this.blockCache = blockCache;
        this.inflightReadAheadTaskMap = inflightReadAheadTaskMap;
        this.inflightReadThrottle = inflightReadThrottle;
    }

    public void shutdown() {
        streamReaderExecutor.shutdown();
        backgroundExecutor.shutdown();
    }

    public CompletableFuture<List<StreamRecordBatch>> syncReadAhead(long streamId, long startOffset, long endOffset,
                                                                    int maxBytes, ReadAheadAgent agent, UUID uuid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] sync read ahead, stream={}, {}-{}, maxBytes={}", streamId, startOffset, endOffset, maxBytes);
        }
        ReadContext context = new ReadContext(startOffset, maxBytes);
        TimerUtil timer = new TimerUtil();
        DefaultS3BlockCache.ReadAheadTaskKey readAheadTaskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset);
        // put a placeholder task at start offset to prevent next cache miss request spawn duplicated read ahead task
        inflightReadAheadTaskMap.putIfAbsent(readAheadTaskKey, new CompletableFuture<>());
        return getDataBlockIndices(streamId, endOffset, context).thenComposeAsync(v -> {
            if (context.streamDataBlocksPair.isEmpty()) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] stream={}, {}-{}, read data block indices cost: {} ms", streamId, startOffset, endOffset,
                        timer.elapsedAs(TimeUnit.MILLISECONDS));
            }

            List<CompletableFuture<Void>> cfList = new ArrayList<>();
            Map<String, List<StreamRecordBatch>> recordsMap = new ConcurrentHashMap<>();
            List<String> sortedDataBlockKeyList = new ArrayList<>();

            // collect all data blocks to read from S3
            List<Pair<ObjectReader, StreamDataBlock>> streamDataBlocksToRead = collectStreamDataBlocksToRead(streamId, context);

            // reserve all data blocks to read
            List<DataBlockReadAccumulator.ReserveResult> reserveResults = dataBlockReadAccumulator.reserveDataBlock(streamDataBlocksToRead);
            int totalReserveSize = reserveResults.stream().mapToInt(DataBlockReadAccumulator.ReserveResult::reserveSize).sum();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] sync ra acquire size: {}, uuid={}, stream={}, {}-{}, {}",
                        totalReserveSize, uuid, streamId, startOffset, endOffset, maxBytes);
            }

            CompletableFuture<Void> throttleCf = inflightReadThrottle.acquire(uuid, totalReserveSize);
            return throttleCf.thenComposeAsync(nil -> {
                // concurrently read all data blocks
                for (int i = 0; i < streamDataBlocksToRead.size(); i++) {
                    Pair<ObjectReader, StreamDataBlock> pair = streamDataBlocksToRead.get(i);
                    ObjectReader objectReader = pair.getLeft();
                    StreamDataBlock streamDataBlock = pair.getRight();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] sync ra, stream={}, {}-{}, read data block {} from {} [{}, {}), size={}",
                                streamId, startOffset, endOffset, streamDataBlock.getBlockId(), objectReader.objectKey(),
                                streamDataBlock.getBlockStartPosition(), streamDataBlock.getBlockEndPosition(), streamDataBlock.getBlockSize());
                    }
                    String dataBlockKey = streamDataBlock.getObjectId() + "-" + streamDataBlock.getBlockId();
                    sortedDataBlockKeyList.add(dataBlockKey);
                    DataBlockReadAccumulator.ReserveResult reserveResult = reserveResults.get(i);
                    DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
                    cfList.add(reserveResult.cf().thenAcceptAsync(dataBlock -> {
                        if (dataBlock.records().isEmpty()) {
                            return;
                        }

                        // retain records to be returned
                        dataBlock.records().forEach(StreamRecordBatch::retain);
                        recordsMap.put(dataBlockKey, dataBlock.records());

                        // retain records to be put into block cache
                        dataBlock.records().forEach(StreamRecordBatch::retain);
                        blockCache.put(streamId, dataBlock.records());
                        dataBlock.release();

                        // complete and remove inflight read ahead task
                        CompletableFuture<Void> inflightReadAheadTask = inflightReadAheadTaskMap.remove(taskKey);
                        if (inflightReadAheadTask != null) {
                            inflightReadAheadTask.complete(null);
                        }
                    }, backgroundExecutor));
                    if (reserveResult.reserveSize() > 0) {
                        dataBlockReadAccumulator.readDataBlock(objectReader, streamDataBlock.dataBlockIndex());
                    }
                }
                return CompletableFuture.allOf(cfList.toArray(CompletableFuture[]::new)).thenApply(vv -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] sync read ahead complete, stream={}, {}-{}, maxBytes: {}, " +
                                        "result: {}-{}, {}, cost: {} ms", streamId, startOffset, endOffset, maxBytes,
                                startOffset, context.lastOffset, context.totalReadSize, timer.elapsedAs(TimeUnit.MILLISECONDS));
                    }
                    context.releaseReader();

                    List<StreamRecordBatch> recordsToReturn = new LinkedList<>();
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
                    agent.updateReadAheadResult(context.lastOffset, context.totalReadSize);
                    return recordsToReturn;
                });
            }, streamReaderExecutor);
        }, streamReaderExecutor);
    }

    public void asyncReadAhead(long streamId, long startOffset, long endOffset, int maxBytes, ReadAheadAgent agent) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] async read ahead, stream={}, {}-{}, maxBytes={}", streamId, startOffset, endOffset, maxBytes);
        }
        ReadContext context = new ReadContext(startOffset, maxBytes);
        TimerUtil timer = new TimerUtil();
        DefaultS3BlockCache.ReadAheadTaskKey readAheadTaskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset);
        // put a placeholder task at start offset to prevent next cache miss request spawn duplicated read ahead task
        inflightReadAheadTaskMap.putIfAbsent(readAheadTaskKey, new CompletableFuture<>());
        getDataBlockIndices(streamId, endOffset, context).thenAcceptAsync(v -> {
            if (context.streamDataBlocksPair.isEmpty()) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] stream={}, {}-{}, read data block indices cost: {} ms", streamId, startOffset, endOffset,
                        timer.elapsedAs(TimeUnit.MILLISECONDS));
            }

            List<CompletableFuture<Void>> cfList = new ArrayList<>();
            // collect all data blocks to read from S3
            List<Pair<ObjectReader, StreamDataBlock>> streamDataBlocksToRead = collectStreamDataBlocksToRead(streamId, context);

            // concurrently read all data blocks
            for (int i = 0; i < streamDataBlocksToRead.size(); i++) {
                Pair<ObjectReader, StreamDataBlock> pair = streamDataBlocksToRead.get(i);
                ObjectReader objectReader = pair.getLeft();
                StreamDataBlock streamDataBlock = pair.getRight();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] async ra, stream={}, {}-{}, read data block {} from {} [{}, {}), size={}",
                            streamId, startOffset, endOffset, streamDataBlock.getBlockId(), objectReader.objectKey(),
                            streamDataBlock.getBlockStartPosition(), streamDataBlock.getBlockEndPosition(), streamDataBlock.getBlockSize());
                }
                UUID uuid = UUID.randomUUID();
                DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
                DataBlockReadAccumulator.ReserveResult reserveResult = dataBlockReadAccumulator.reserveDataBlock(List.of(pair)).get(0);
                int readIndex = i;
                cfList.add(reserveResult.cf().thenAcceptAsync(dataBlock -> {
                    if (dataBlock.records().isEmpty()) {
                        return;
                    }

                    // retain records to be put into block cache
                    dataBlock.records().forEach(StreamRecordBatch::retain);
                    if (readIndex == 0) {
                        long firstOffset = dataBlock.records().get(0).getBaseOffset();
                        blockCache.put(streamId, firstOffset, context.lastOffset, dataBlock.records());
                    } else {
                        blockCache.put(streamId, dataBlock.records());
                    }
                    dataBlock.release();
                    inflightReadThrottle.release(uuid);

                    // complete and remove inflight read ahead task
                    CompletableFuture<Void> inflightReadAheadTask = inflightReadAheadTaskMap.remove(taskKey);
                    if (inflightReadAheadTask != null) {
                        inflightReadAheadTask.complete(null);
                    }
                }, backgroundExecutor));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] async ra acquire size: {}, uuid={}, stream={}, {}-{}, {}",
                            reserveResult.reserveSize(), uuid, streamId, startOffset, endOffset, maxBytes);
                }
                if (reserveResult.reserveSize() > 0) {
                    inflightReadThrottle.acquire(uuid, reserveResult.reserveSize()).thenAcceptAsync(nil -> {
                        // read data block
                        dataBlockReadAccumulator.readDataBlock(objectReader, streamDataBlock.dataBlockIndex());
                    }, streamReaderExecutor);
                }
            }
            CompletableFuture.allOf(cfList.toArray(CompletableFuture[]::new)).thenAccept(vv -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] sync read ahead complete, stream={}, {}-{}, maxBytes: {}, " +
                                    "result: {}-{}, {}, cost: {} ms", streamId, startOffset, endOffset, maxBytes,
                            startOffset, context.lastOffset, context.totalReadSize, timer.elapsedAs(TimeUnit.MILLISECONDS));
                }
                context.releaseReader();
                agent.updateReadAheadResult(context.lastOffset, context.totalReadSize);
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
            context.totalReadSize += streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum();
            if (findIndexResult.isFulfilled()) {
                return CompletableFuture.completedFuture(null);
            }
            context.nextStartOffset = findIndexResult.nextStartOffset();
            context.nextMaxBytes = findIndexResult.nextMaxBytes();
            context.objectIndex++;
            return getDataBlockIndices(streamId, endOffset, context);
        }, streamReaderExecutor);
    }

    private List<Pair<ObjectReader, StreamDataBlock>> collectStreamDataBlocksToRead(long streamId, ReadContext context) {
        List<Pair<ObjectReader, StreamDataBlock>> result = new ArrayList<>();
        for (Pair<Long, List<StreamDataBlock>> entry : context.streamDataBlocksPair) {
            long objectId = entry.getKey();
            ObjectReader objectReader = context.objectReaderMap.get(objectId);
            for (StreamDataBlock streamDataBlock : entry.getValue()) {
                result.add(Pair.of(objectReader, streamDataBlock));
                DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
                inflightReadAheadTaskMap.putIfAbsent(taskKey, new CompletableFuture<>());
            }
        }
        return result;
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
        int totalReadSize;
        long lastOffset;

        public ReadContext(long startOffset, int maxBytes) {
            this.objects = new LinkedList<>();
            this.objectIndex = 0;
            this.streamDataBlocksPair = new LinkedList<>();
            this.objectReaderMap = new HashMap<>();
            this.nextStartOffset = startOffset;
            this.nextMaxBytes = maxBytes;
        }

        public void releaseReader() {
            for (Map.Entry<Long, ObjectReader> entry : objectReaderMap.entrySet()) {
                entry.getValue().release();
            }
            objectReaderMap.clear();
        }

    }
}
