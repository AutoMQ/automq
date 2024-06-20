/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.trace.TraceUtils;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.Threads;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamReader {
    public static final Integer MAX_OBJECT_READER_SIZE = 100 * 1024 * 1024; // 100MB;
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReader.class);
    private static final Integer READ_OBJECT_INDEX_STEP = 2;
    private final S3Operator s3Operator;
    private final ObjectManager objectManager;
    private final ObjectReaderLRUCache objectReaders;
    private final DataBlockReadAccumulator dataBlockReadAccumulator;
    private final BlockCache blockCache;
    private final Map<DefaultS3BlockCache.ReadAheadTaskKey, DefaultS3BlockCache.ReadAheadTaskContext> inflightReadAheadTaskMap;
    private final InflightReadThrottle inflightReadThrottle;
    private final ExecutorService streamReaderExecutor = Threads.newFixedThreadPoolWithMonitor(
        2,
        "s3-stream-reader",
        false,
        LOGGER);
    private final ExecutorService getIndicesExecutor = Threads.newFixedThreadPoolWithMonitor(
            2,
            "s3-stream-get-indices",
            false,
            LOGGER);
    private final ExecutorService backgroundExecutor = Threads.newFixedThreadPoolWithMonitor(
        2,
        "s3-stream-reader-background",
        true,
        LOGGER);
    private final ExecutorService errorHandlerExecutor = Threads.newFixedThreadPoolWithMonitor(
        1,
        "s3-stream-reader-error-handler",
        true,
        LOGGER);

    public StreamReader(S3Operator operator, ObjectManager objectManager, BlockCache blockCache,
        Map<DefaultS3BlockCache.ReadAheadTaskKey, DefaultS3BlockCache.ReadAheadTaskContext> inflightReadAheadTaskMap,
        InflightReadThrottle inflightReadThrottle) {
        this.s3Operator = operator;
        this.objectManager = objectManager;
        this.objectReaders = new ObjectReaderLRUCache(MAX_OBJECT_READER_SIZE);
        this.dataBlockReadAccumulator = new DataBlockReadAccumulator();
        this.blockCache = blockCache;
        this.inflightReadAheadTaskMap = Objects.requireNonNull(inflightReadAheadTaskMap);
        this.inflightReadThrottle = Objects.requireNonNull(inflightReadThrottle);
    }

    // for test
    public StreamReader(S3Operator operator, ObjectManager objectManager, BlockCache blockCache,
        ObjectReaderLRUCache objectReaders,
        DataBlockReadAccumulator dataBlockReadAccumulator,
        Map<DefaultS3BlockCache.ReadAheadTaskKey, DefaultS3BlockCache.ReadAheadTaskContext> inflightReadAheadTaskMap,
        InflightReadThrottle inflightReadThrottle) {
        this.s3Operator = operator;
        this.objectManager = objectManager;
        this.objectReaders = objectReaders;
        this.dataBlockReadAccumulator = dataBlockReadAccumulator;
        this.blockCache = blockCache;
        this.inflightReadAheadTaskMap = Objects.requireNonNull(inflightReadAheadTaskMap);
        this.inflightReadThrottle = Objects.requireNonNull(inflightReadThrottle);
    }

    public void shutdown() {
        streamReaderExecutor.shutdown();
        backgroundExecutor.shutdown();
        errorHandlerExecutor.shutdown();
    }

    @WithSpan
    public CompletableFuture<List<StreamRecordBatch>> syncReadAhead(TraceContext traceContext,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes, ReadAheadAgent agent, UUID uuid) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] sync read ahead, stream={}, {}-{}, maxBytes={}", streamId, startOffset, endOffset, maxBytes);
        }
        ReadContext context = new ReadContext(startOffset, maxBytes);
        TimerUtil timer = new TimerUtil();
        DefaultS3BlockCache.ReadAheadTaskKey readAheadTaskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset);
        // put a placeholder task at start offset to prevent next cache miss request spawn duplicated read ahead task
        DefaultS3BlockCache.ReadAheadTaskContext readAheadTaskContext = new DefaultS3BlockCache.ReadAheadTaskContext(new CompletableFuture<>(),
            DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_DATA_INDEX);
        if (inflightReadAheadTaskMap.putIfAbsent(readAheadTaskKey, readAheadTaskContext) == null) {
            context.taskKeySet.add(readAheadTaskKey);
        }
        return getDataBlockIndices(traceContext, streamId, endOffset, context)
            .thenComposeAsync(v -> handleSyncReadAhead(traceContext, streamId, startOffset, endOffset, maxBytes, agent, uuid, context), streamReaderExecutor)
            .whenComplete((nil, ex) -> {
                for (DefaultS3BlockCache.ReadAheadTaskKey key : context.taskKeySet) {
                    completeInflightTask0(key, ex);
                }
                context.taskKeySet.clear();
                long totalTime = timer.elapsedAndResetAs(TimeUnit.NANOSECONDS);
                StorageOperationStats.getInstance().readAheadTimeStats(true).record(totalTime);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] sync read ahead complete, stream={}, {}-{}, maxBytes: {}, " +
                                    "result: {}-{}, {}, cost: {} ms", streamId, startOffset, endOffset, maxBytes,
                            startOffset, context.lastOffset, context.totalReadSize, totalTime);
                }
            });
    }

    @WithSpan
    CompletableFuture<List<StreamRecordBatch>> handleSyncReadAhead(TraceContext traceContext, long streamId,
                                                                   long startOffset, long endOffset, int maxBytes,
                                                                   ReadAheadAgent agent, UUID uuid, ReadContext context) {
        if (context.streamDataBlocksPair.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        List<CompletableFuture<List<StreamRecordBatch>>> cfList = new ArrayList<>();
        Map<String, List<StreamRecordBatch>> recordsMap = new ConcurrentHashMap<>();
        List<String> sortedDataBlockKeyList = new ArrayList<>();

        // collect all data blocks to read from S3
        List<Pair<ObjectReader, StreamDataBlock>> streamDataBlocksToRead = collectStreamDataBlocksToRead(context);

        // reserve all data blocks to read
        List<DataBlockReadAccumulator.ReserveResult> reserveResults = dataBlockReadAccumulator.reserveDataBlock(streamDataBlocksToRead);
        int totalReserveSize = reserveResults.stream().mapToInt(DataBlockReadAccumulator.ReserveResult::reserveSize).sum();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] sync ra acquire size: {}, uuid={}, stream={}, {}-{}, {}",
                totalReserveSize, uuid, streamId, startOffset, endOffset, maxBytes);
        }

        TimerUtil throttleTimer = new TimerUtil();
        CompletableFuture<Void> throttleCf = inflightReadThrottle.acquire(traceContext, uuid, totalReserveSize);
        return throttleCf.thenComposeAsync(nil -> {
            // concurrently read all data blocks
            StorageOperationStats.getInstance().readAheadGetIndicesTimeStats.record(throttleTimer.elapsedAndResetAs(TimeUnit.NANOSECONDS));
            for (int i = 0; i < streamDataBlocksToRead.size(); i++) {
                TimerUtil timerReadS3 = new TimerUtil();
                Pair<ObjectReader, StreamDataBlock> pair = streamDataBlocksToRead.get(i);
                ObjectReader objectReader = pair.getLeft();
                StreamDataBlock streamDataBlock = pair.getRight();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] sync ra, stream={}, {}-{}, read data block {} from {} [{}, {}), size={}",
                        streamId, startOffset, endOffset, streamDataBlock, objectReader.objectKey(),
                        streamDataBlock.getBlockStartPosition(), streamDataBlock.getBlockEndPosition(), streamDataBlock.getBlockSize());
                }
                String dataBlockKey = streamDataBlock.getObjectId() + "-" + streamDataBlock.getBlockStartPosition();
                sortedDataBlockKeyList.add(dataBlockKey);
                DataBlockReadAccumulator.ReserveResult reserveResult = reserveResults.get(i);
                DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
                if (context.taskKeySet.contains(taskKey)) {
                    setInflightReadAheadStatus(taskKey, DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA);
                }
                boolean isNotAlignedFirstBlock = i == 0 && startOffset != streamDataBlock.getStartOffset();
                if (isNotAlignedFirstBlock && context.taskKeySet.contains(taskKey)) {
                    setInflightReadAheadStatus(new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset),
                        DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA);
                }
                try {
                    CompletableFuture<List<StreamRecordBatch>> cf = TraceUtils.runWithSpanAsync(new TraceContext(traceContext), Attributes.empty(), "StreamReader::readDataBlock",
                        () -> reserveResult.cf().thenApplyAsync(dataBlock -> {
                            StorageOperationStats.getInstance().readAheadReadS3TimeStats.record(timerReadS3.elapsedAndResetAs(TimeUnit.NANOSECONDS));
                            if (dataBlock.records().isEmpty()) {
                                return new ArrayList<StreamRecordBatch>();
                            }
                            // retain records to be returned
                            dataBlock.records().forEach(StreamRecordBatch::retain);
                            recordsMap.put(dataBlockKey, dataBlock.records());

                            // retain records to be put into block cache
                            dataBlock.records().forEach(StreamRecordBatch::retain);
                            blockCache.put(streamId, dataBlock.records());
                            dataBlock.release();

                            StorageOperationStats.getInstance().readAheadPutBlockCacheTimeStats.record(timerReadS3.elapsedAndResetAs(TimeUnit.NANOSECONDS));
                            return dataBlock.records();
                        }, backgroundExecutor).whenComplete((ret, ex) -> {
                            if (ex != null) {
                                LOGGER.error("[S3BlockCache] sync ra fail to read data block, stream={}, {}-{}, data block: {}",
                                    streamId, startOffset, endOffset, streamDataBlock, ex);
                            }
                            completeInflightTask(context, taskKey, ex);
                            if (isNotAlignedFirstBlock) {
                                // in case of first data block and startOffset is not aligned with start of data block
                                completeInflightTask(context, new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset), ex);
                            }
                        }));
                    cfList.add(cf);
                } catch (Throwable e) {
                    throw new IllegalArgumentException(e);
                }
                if (reserveResult.reserveSize() > 0) {
                    dataBlockReadAccumulator.readDataBlock(objectReader, streamDataBlock.dataBlockIndex());
                }
            }
            return CompletableFuture.allOf(cfList.toArray(CompletableFuture[]::new)).thenApply(vv -> {
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
                StorageOperationStats.getInstance().readAheadSizeStats(true).record(context.totalReadSize);
                return recordsToReturn;
            }).whenComplete((ret, ex) -> {
                if (ex != null) {
                    LOGGER.error("[S3BlockCache] sync read ahead fail, stream={}, {}-{}, maxBytes: {}",
                        streamId, startOffset, endOffset, maxBytes, ex);
                    errorHandlerExecutor.execute(() -> cleanUpOnCompletion(cfList));
                }
                context.releaseReader();
            });
        }, streamReaderExecutor);
    }

    private void cleanUpOnCompletion(List<CompletableFuture<List<StreamRecordBatch>>> cfList) {
        cfList.forEach(cf -> cf.whenComplete((ret, ex) -> {
            if (ex == null) {
                // release records that won't be returned
                ret.forEach(StreamRecordBatch::release);
            }
        }));
    }

    public void asyncReadAhead(long streamId, long startOffset, long endOffset, int maxBytes, ReadAheadAgent agent) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[S3BlockCache] async read ahead, stream={}, {}-{}, maxBytes={}", streamId, startOffset, endOffset, maxBytes);
        }
        ReadContext context = new ReadContext(startOffset, maxBytes);
        TimerUtil timer = new TimerUtil();
        DefaultS3BlockCache.ReadAheadTaskKey readAheadTaskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset);
        // put a placeholder task at start offset to prevent next cache miss request spawn duplicated read ahead task
        DefaultS3BlockCache.ReadAheadTaskContext readAheadTaskContext = new DefaultS3BlockCache.ReadAheadTaskContext(new CompletableFuture<>(),
            DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_DATA_INDEX);
        inflightReadAheadTaskMap.putIfAbsent(readAheadTaskKey, readAheadTaskContext);
        context.taskKeySet.add(readAheadTaskKey);
        getDataBlockIndices(TraceContext.DEFAULT, streamId, endOffset, context)
            .thenComposeAsync(v -> handleAsyncReadAhead(streamId, startOffset, endOffset, maxBytes, agent, context), streamReaderExecutor)
            .whenComplete((nil, ex) -> {
                for (DefaultS3BlockCache.ReadAheadTaskKey key : context.taskKeySet) {
                    completeInflightTask0(key, ex);
                }
                context.taskKeySet.clear();
                long totalTime = timer.elapsedAs(TimeUnit.NANOSECONDS);
                StorageOperationStats.getInstance().readAheadTimeStats(false).record(totalTime);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[S3BlockCache] async ra complete, stream={}, {}-{}, maxBytes: {}, " +
                                    "result: {}-{}, {}, cost: {} ms", streamId, startOffset, endOffset, maxBytes,
                            startOffset, context.lastOffset, context.totalReadSize, totalTime);
                }
            });
    }

    CompletableFuture<Void> handleAsyncReadAhead(long streamId, long startOffset, long endOffset, int maxBytes,
                                                 ReadAheadAgent agent, ReadContext context) {
        if (context.streamDataBlocksPair.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<CompletableFuture<Void>> cfList = new ArrayList<>();
        // collect all data blocks to read from S3
        List<Pair<ObjectReader, StreamDataBlock>> streamDataBlocksToRead = collectStreamDataBlocksToRead(context);

        // concurrently read all data blocks
        for (int i = 0; i < streamDataBlocksToRead.size(); i++) {
            TimerUtil timerRead = new TimerUtil();
            Pair<ObjectReader, StreamDataBlock> pair = streamDataBlocksToRead.get(i);
            ObjectReader objectReader = pair.getLeft();
            StreamDataBlock streamDataBlock = pair.getRight();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] async ra, stream={}, {}-{}, read data block {} from {} [{}, {}), size={}",
                    streamId, startOffset, endOffset, streamDataBlock, objectReader.objectKey(),
                    streamDataBlock.getBlockStartPosition(), streamDataBlock.getBlockEndPosition(), streamDataBlock.getBlockSize());
            }
            UUID uuid = UUID.randomUUID();
            DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
            DataBlockReadAccumulator.ReserveResult reserveResult = dataBlockReadAccumulator.reserveDataBlock(List.of(pair)).get(0);
            int readIndex = i;
            boolean isNotAlignedFirstBlock = i == 0 && startOffset != streamDataBlock.getStartOffset();
            CompletableFuture<Void> cf = reserveResult.cf().thenAcceptAsync(dataBlock -> {
                StorageOperationStats.getInstance().readAheadReadS3TimeStats.record(timerRead.elapsedAndResetAs(TimeUnit.NANOSECONDS));
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
                StorageOperationStats.getInstance().readAheadPutBlockCacheTimeStats.record(timerRead.elapsedAndResetAs(TimeUnit.NANOSECONDS));
            }, backgroundExecutor).whenComplete((ret, ex) -> {
                if (ex != null) {
                    LOGGER.error("[S3BlockCache] async ra fail to read data block, stream={}, {}-{}, data block: {}",
                        streamId, startOffset, endOffset, streamDataBlock, ex);
                }
                inflightReadThrottle.release(uuid);
                completeInflightTask(context, taskKey, ex);
                if (isNotAlignedFirstBlock) {
                    // in case of first data block and startOffset is not aligned with start of data block
                    completeInflightTask(context, new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset), ex);
                }
            });
            cfList.add(cf);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[S3BlockCache] async ra acquire size: {}, uuid={}, stream={}, {}-{}, {}",
                    reserveResult.reserveSize(), uuid, streamId, startOffset, endOffset, maxBytes);
            }
            if (reserveResult.reserveSize() > 0) {
                inflightReadThrottle.acquire(TraceContext.DEFAULT, uuid, reserveResult.reserveSize()).thenAcceptAsync(nil -> {
                    StorageOperationStats.getInstance().readAheadThrottleTimeStats.record(timerRead.elapsedAndResetAs(TimeUnit.NANOSECONDS));
                    // read data block
                    if (context.taskKeySet.contains(taskKey)) {
                        setInflightReadAheadStatus(taskKey, DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA);
                    }
                    if (isNotAlignedFirstBlock && context.taskKeySet.contains(taskKey)) {
                        setInflightReadAheadStatus(new DefaultS3BlockCache.ReadAheadTaskKey(streamId, startOffset),
                            DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_FETCH_DATA);
                    }
                    dataBlockReadAccumulator.readDataBlock(objectReader, streamDataBlock.dataBlockIndex());
                }, streamReaderExecutor).exceptionally(ex -> {
                    cf.completeExceptionally(ex);
                    return null;
                });
            }
        }
        return CompletableFuture.allOf(cfList.toArray(CompletableFuture[]::new)).whenComplete((ret, ex) -> {
            if (ex != null) {
                LOGGER.error("[S3BlockCache] async ra failed, stream={}, {}-{}, maxBytes: {}, " +
                        "result: {}-{}, {}, ", streamId, startOffset, endOffset, maxBytes,
                    startOffset, context.lastOffset, context.totalReadSize, ex);
            }
            context.releaseReader();
            agent.updateReadAheadResult(context.lastOffset, context.totalReadSize);
            StorageOperationStats.getInstance().readAheadSizeStats(false).record(context.totalReadSize);
        });
    }

    @WithSpan
    CompletableFuture<Void> getDataBlockIndices(TraceContext traceContext, long streamId, long endOffset,
        ReadContext context) {
        traceContext.currentContext();
        CompletableFuture<Boolean /* empty objects */> getObjectsCf = CompletableFuture.completedFuture(false);
        if (context.objectIndex >= context.objects.size()) {
            TimerUtil objectTimer = new TimerUtil();
            getObjectsCf = objectManager
                .getObjects(streamId, context.nextStartOffset, endOffset, READ_OBJECT_INDEX_STEP)
                .thenApply(objects -> {
                    context.getObjectsTime += objectTimer.elapsedAndResetAs(TimeUnit.NANOSECONDS);
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
            long startTime = System.nanoTime();
            S3ObjectMetadata objectMetadata = context.objects.get(context.objectIndex);
            ObjectReader reader = getObjectReader(objectMetadata);
            context.objectReaderMap.put(objectMetadata.objectId(), reader);
            return reader.find(streamId, context.nextStartOffset, endOffset, context.nextMaxBytes)
                    .whenComplete((ret, ex) -> {
                        context.findIndexTime += TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS);
                    });
        }, getIndicesExecutor);

        return findIndexCf.thenComposeAsync(findIndexResult -> {
            if (findIndexResult == null) {
                return CompletableFuture.completedFuture(null);
            }
            List<StreamDataBlock> streamDataBlocks = findIndexResult.streamDataBlocks();
            if (streamDataBlocks.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            long startTime = System.nanoTime();

            for (StreamDataBlock streamDataBlock : streamDataBlocks) {
                DefaultS3BlockCache.ReadAheadTaskKey taskKey = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, streamDataBlock.getStartOffset());
                DefaultS3BlockCache.ReadAheadTaskContext readAheadContext = new DefaultS3BlockCache.ReadAheadTaskContext(new CompletableFuture<>(),
                    DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_THROTTLE);
                if (inflightReadAheadTaskMap.putIfAbsent(taskKey, readAheadContext) == null) {
                    context.taskKeySet.add(taskKey);
                }
                if (context.isFirstDataBlock && streamDataBlock.getStartOffset() != context.nextStartOffset) {
                    context.isFirstDataBlock = false;
                    DefaultS3BlockCache.ReadAheadTaskKey key = new DefaultS3BlockCache.ReadAheadTaskKey(streamId, context.nextStartOffset);
                    if (context.taskKeySet.contains(key)) {
                        inflightReadAheadTaskMap.computeIfPresent(key, (k, v) -> {
                            v.status = DefaultS3BlockCache.ReadBlockCacheStatus.WAIT_THROTTLE;
                            return v;
                        });
                    }
                }
            }

            S3ObjectMetadata objectMetadata = context.objects.get(context.objectIndex);
            long lastOffset = streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset();
            context.lastOffset = Math.max(lastOffset, context.lastOffset);
            context.streamDataBlocksPair.add(new ImmutablePair<>(objectMetadata.objectId(), streamDataBlocks));
            context.totalReadSize += streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum();
            context.nextMaxBytes = findIndexResult.nextMaxBytes();
            context.nextStartOffset = findIndexResult.nextStartOffset();
            context.objectIndex++;
            context.computeTime += TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS);
            if (findIndexResult.isFulfilled()) {
                StorageOperationStats.getInstance().readAheadGetIndicesTimeStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
                StorageOperationStats.getInstance().getIndicesTimeGetObjectStats.record(context.getObjectsTime);
                StorageOperationStats.getInstance().getIndicesTimeFindIndexStats.record(context.findIndexTime);
                StorageOperationStats.getInstance().getIndicesTimeComputeStats.record(context.computeTime);
                return CompletableFuture.completedFuture(null);
            }
            return getDataBlockIndices(traceContext, streamId, endOffset, context);
        }, getIndicesExecutor);
    }

    private void setInflightReadAheadStatus(DefaultS3BlockCache.ReadAheadTaskKey key,
        DefaultS3BlockCache.ReadBlockCacheStatus status) {
        inflightReadAheadTaskMap.computeIfPresent(key, (k, readAheadContext) -> {
            readAheadContext.status = status;
            return readAheadContext;
        });
    }

    private void completeInflightTask(ReadContext readContext, DefaultS3BlockCache.ReadAheadTaskKey key, Throwable ex) {
        if (!readContext.taskKeySet.contains(key)) {
            return;
        }
        completeInflightTask0(key, ex);
        readContext.taskKeySet.remove(key);
    }

    private void completeInflightTask0(DefaultS3BlockCache.ReadAheadTaskKey key, Throwable ex) {
        DefaultS3BlockCache.ReadAheadTaskContext context = inflightReadAheadTaskMap.remove(key);
        if (context != null) {
            if (ex != null) {
                context.cf.completeExceptionally(ex);
            } else {
                context.cf.complete(null);
            }
        }
    }

    private List<Pair<ObjectReader, StreamDataBlock>> collectStreamDataBlocksToRead(ReadContext context) {
        List<Pair<ObjectReader, StreamDataBlock>> result = new ArrayList<>();
        for (Pair<Long, List<StreamDataBlock>> entry : context.streamDataBlocksPair) {
            long objectId = entry.getKey();
            ObjectReader objectReader = context.objectReaderMap.get(objectId);
            for (StreamDataBlock streamDataBlock : entry.getValue()) {
                result.add(Pair.of(objectReader, streamDataBlock));
            }
        }
        return result;
    }

    ObjectReader getObjectReader(S3ObjectMetadata metadata) {
        synchronized (objectReaders) {
            ObjectReader objectReader = objectReaders.get(metadata.objectId());
            if (objectReader == null) {
                objectReader = ObjectReader.reader(metadata, s3Operator);
                objectReaders.put(metadata.objectId(), objectReader);
            }
            return objectReader.retain();
        }
    }

    static class ReadContext {
        List<S3ObjectMetadata> objects;
        List<Pair<Long, List<StreamDataBlock>>> streamDataBlocksPair;
        Map<Long, ObjectReader> objectReaderMap;
        Set<DefaultS3BlockCache.ReadAheadTaskKey> taskKeySet;
        boolean isFirstDataBlock = true;
        int objectIndex;
        long nextStartOffset;
        int nextMaxBytes;
        int totalReadSize;
        long lastOffset;
        TimerUtil timer;
        long getObjectsTime = 0;
        long findIndexTime = 0;
        long computeTime = 0;

        public ReadContext(long startOffset, int maxBytes) {
            this.objects = new LinkedList<>();
            this.objectIndex = 0;
            this.streamDataBlocksPair = new LinkedList<>();
            this.objectReaderMap = new ConcurrentHashMap<>();
            this.taskKeySet = ConcurrentHashMap.newKeySet();
            this.nextStartOffset = startOffset;
            this.nextMaxBytes = maxBytes;
            this.timer = new TimerUtil();
        }

        public void releaseReader() {
            for (Map.Entry<Long, ObjectReader> entry : objectReaderMap.entrySet()) {
                entry.getValue().release();
            }
            objectReaderMap.clear();
        }

    }
}
