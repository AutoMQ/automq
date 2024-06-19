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

package com.automq.stream.s3;

import com.automq.stream.api.exceptions.FastReadFailFastException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.FutureTicker;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.utils.FutureUtil.suppress;

public class S3Storage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Storage.class);
    private static final FastReadFailFastException FAST_READ_FAIL_FAST_EXCEPTION = new FastReadFailFastException();
    private static final int NUM_STREAM_CALLBACK_LOCKS = 128;
    private final long maxDeltaWALCacheSize;
    private final Config config;
    private final WriteAheadLog deltaWAL;
    /**
     * WAL log cache
     */
    private final LogCache deltaWALCache;
    /**
     * WAL out of order callback sequencer. {@link #streamCallbackLocks} will ensure the memory safety.
     */
    private final WALCallbackSequencer callbackSequencer = new WALCallbackSequencer();
    private final WALConfirmOffsetCalculator confirmOffsetCalculator = new WALConfirmOffsetCalculator();
    private final Queue<DeltaWALUploadTaskContext> walPrepareQueue = new LinkedList<>();
    private final Queue<DeltaWALUploadTaskContext> walCommitQueue = new LinkedList<>();
    private final List<DeltaWALUploadTaskContext> inflightWALUploadTasks = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService backgroundExecutor = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("s3-storage-background", true), LOGGER);
    private final ExecutorService uploadWALExecutor = Threads.newFixedThreadPoolWithMonitor(
        4, "s3-storage-upload-wal", true, LOGGER);
    /**
     * A ticker used for batching force upload WAL.
     *
     * @see #forceUpload
     */
    private final FutureTicker forceUploadTicker = new FutureTicker(500, TimeUnit.MILLISECONDS, backgroundExecutor);
    private final Queue<WalWriteRequest> backoffRecords = new LinkedBlockingQueue<>();
    private final ScheduledFuture<?> drainBackoffTask;
    private final StreamManager streamManager;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final S3BlockCache blockCache;
    /**
     * Stream callback locks. Used to ensure the stream callbacks will not be called concurrently.
     *
     * @see #handleAppendCallback
     */
    private final Lock[] streamCallbackLocks = IntStream.range(0, NUM_STREAM_CALLBACK_LOCKS).mapToObj(i -> new ReentrantLock()).toArray(Lock[]::new);
    private final HashedWheelTimer timeoutDetect = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("storage-timeout-detect", true), 1, TimeUnit.SECONDS, 100);
    private long lastLogTimestamp = 0L;
    private volatile double maxDataWriteRate = 0.0;

    public S3Storage(Config config, WriteAheadLog deltaWAL, StreamManager streamManager, ObjectManager objectManager,
        S3BlockCache blockCache, S3Operator s3Operator) {
        this.config = config;
        this.maxDeltaWALCacheSize = config.walCacheSize();
        this.deltaWAL = deltaWAL;
        this.blockCache = blockCache;
        this.deltaWALCache = new LogCache(config.walCacheSize(), config.walUploadThreshold(), config.maxStreamNumPerStreamSetObject());
        this.streamManager = streamManager;
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.drainBackoffTask = this.backgroundExecutor.scheduleWithFixedDelay(this::tryDrainBackoffRecords, 100, 100, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerInflightWALUploadTasksCountSupplier(this.inflightWALUploadTasks::size);
    }

    /**
     * Only for test.
     */
    static LogCache.LogCacheBlock recoverContinuousRecords(Iterator<WriteAheadLog.RecoverResult> it,
        List<StreamMetadata> openingStreams) {
        RecoverResult result = recoverContinuousRecords(it, openingStreams, LOGGER);
        result.firstException().ifPresent(e -> {
            throw e;
        });
        return result.cacheBlock;
    }

    /**
     * Recover continuous records in each stream from the WAL, and put them into the returned {@link LogCache.LogCacheBlock}.
     * It will filter out
     * <ul>
     *     <li>the records that are not in the opening streams</li>
     *     <li>the records that have been committed</li>
     *     <li>the records that are not continuous, which means, all records after the first discontinuous record</li>
     * </ul>
     * <p>
     * It throws {@link IllegalStateException} if the start offset of the first recovered record mismatches
     * the end offset of any opening stream, which indicates data loss.
     * <p>
     * If there are out of order records (which should never happen or there is a BUG), it will try to re-order them.
     * <p>
     * For example, if we recover following records from the WAL in a stream:
     * <pre>    1, 2, 3, 5, 4, 6, 10, 11</pre>
     * and the {@link StreamMetadata#endOffset()} of this stream is 3. Then the returned {@link LogCache.LogCacheBlock}
     * will contain records
     * <pre>    3, 4, 5, 6</pre>
     * Here,
     * <ul>
     *     <li>The record 1 and 2 are discarded because they have been committed (less than 3, the end offset of the stream)</li>
     *     <li>The record 10 and 11 are discarded because they are not continuous (10 is not 7, the next offset of 6)</li>
     *     <li>The record 5 and 4 are reordered because they are out of order, and we handle this bug here</li>
     * </ul>
     */
    static RecoverResult recoverContinuousRecords(Iterator<WriteAheadLog.RecoverResult> it,
        List<StreamMetadata> openingStreams, Logger logger) {
        Map<Long, Long> openingStreamEndOffsets = openingStreams.stream().collect(Collectors.toMap(StreamMetadata::streamId, StreamMetadata::endOffset));
        LogCache.LogCacheBlock cacheBlock = new LogCache.LogCacheBlock(1024L * 1024 * 1024);
        Map<Long, Long> streamNextOffsets = new HashMap<>();
        Map<Long, Queue<StreamRecordBatch>> streamDiscontinuousRecords = new HashMap<>();
        long logEndOffset = recoverContinuousRecords(it, openingStreamEndOffsets, streamNextOffsets, streamDiscontinuousRecords, cacheBlock, logger);
        // release all discontinuous records.
        streamDiscontinuousRecords.values().forEach(queue -> {
            if (queue.isEmpty()) {
                return;
            }
            logger.info("drop discontinuous records, records={}", queue);
            queue.forEach(StreamRecordBatch::release);
        });

        if (logEndOffset >= 0L) {
            cacheBlock.confirmOffset(logEndOffset);
        }

        RecoverResult result = new RecoverResult();
        cacheBlock.records().forEach((streamId, records) -> {
            if (!records.isEmpty()) {
                long startOffset = records.get(0).getBaseOffset();
                long expectedStartOffset = openingStreamEndOffsets.getOrDefault(streamId, startOffset);
                if (startOffset != expectedStartOffset) {
                    RuntimeException exception = new IllegalStateException(String.format("[BUG] WAL data may lost, streamId %d endOffset=%d from controller, " +
                        "but WAL recovered records startOffset=%s", streamId, expectedStartOffset, startOffset));
                    LOGGER.error("invalid stream records", exception);
                    result.invalidStreams.put(streamId, exception);
                }
            }
        });
        if (result.invalidStreams.isEmpty()) {
            result.cacheBlock = cacheBlock;
        } else {
            // re-new a cache block and put all valid records into it.
            LogCache.LogCacheBlock newCacheBlock = new LogCache.LogCacheBlock(1024L * 1024 * 1024);
            cacheBlock.records().forEach((streamId, records) -> {
                if (!result.invalidStreams.containsKey(streamId)) {
                    records.forEach(newCacheBlock::put);
                } else {
                    // release invalid records.
                    records.forEach(StreamRecordBatch::release);
                }
            });
            result.cacheBlock = newCacheBlock;
        }

        return result;
    }

    /**
     * Recover continuous records in each stream from the WAL, and put them into the returned {@link LogCache.LogCacheBlock}.
     *
     * @param it                         WAL recover iterator
     * @param openingStreamEndOffsets    the end offset of each opening stream
     * @param streamNextOffsets          the next offset of each stream (to be filled)
     * @param streamDiscontinuousRecords the out-of-order records of each stream (to be filled)
     * @param cacheBlock                 the cache block (to be filled)
     * @return the end offset of the last record recovered
     */
    private static long recoverContinuousRecords(Iterator<WriteAheadLog.RecoverResult> it,
        Map<Long, Long> openingStreamEndOffsets,
        Map<Long, Long> streamNextOffsets,
        Map<Long, Queue<StreamRecordBatch>> streamDiscontinuousRecords,
        LogCache.LogCacheBlock cacheBlock,
        Logger logger) {
        long logEndOffset = -1L;
        while (it.hasNext()) {
            WriteAheadLog.RecoverResult recoverResult = it.next();
            logEndOffset = recoverResult.recordOffset();
            ByteBuf recordBuf = recoverResult.record().duplicate();
            StreamRecordBatch streamRecordBatch = StreamRecordBatchCodec.decode(recordBuf);
            long streamId = streamRecordBatch.getStreamId();
            Long openingStreamEndOffset = openingStreamEndOffsets.get(streamId);
            if (openingStreamEndOffset == null) {
                // stream is already safe closed. so skip the stream records.
                recordBuf.release();
                continue;
            }
            if (streamRecordBatch.getBaseOffset() < openingStreamEndOffset) {
                // filter committed records.
                recordBuf.release();
                continue;
            }

            Long expectNextOffset = streamNextOffsets.get(streamId);
            Queue<StreamRecordBatch> discontinuousRecords = streamDiscontinuousRecords.get(streamId);
            if (expectNextOffset == null || expectNextOffset == streamRecordBatch.getBaseOffset()) {
                // continuous record, put it into cache.
                cacheBlock.put(streamRecordBatch);
                expectNextOffset = streamRecordBatch.getLastOffset();
                // check if there are some out of order records in the queue.
                if (discontinuousRecords != null) {
                    while (!discontinuousRecords.isEmpty()) {
                        StreamRecordBatch peek = discontinuousRecords.peek();
                        if (peek.getBaseOffset() == expectNextOffset) {
                            // should never happen, log it.
                            logger.error("[BUG] recover an out of order record, streamId={}, expectNextOffset={}, record={}", streamId, expectNextOffset, peek);
                            cacheBlock.put(peek);
                            discontinuousRecords.poll();
                            expectNextOffset = peek.getLastOffset();
                        } else {
                            break;
                        }
                    }
                }
                // update next offset.
                streamNextOffsets.put(streamRecordBatch.getStreamId(), expectNextOffset);
            } else {
                // unexpected record, put it into discontinuous records queue.
                if (discontinuousRecords == null) {
                    discontinuousRecords = new PriorityQueue<>(Comparator.comparingLong(StreamRecordBatch::getBaseOffset));
                    streamDiscontinuousRecords.put(streamId, discontinuousRecords);
                }
                discontinuousRecords.add(streamRecordBatch);
            }
        }
        return logEndOffset;
    }

    @Override
    public void startup() {
        try {
            LOGGER.info("S3Storage starting");
            recover();
            LOGGER.info("S3Storage start completed");
        } catch (Throwable e) {
            LOGGER.error("S3Storage start fail", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Upload WAL to S3 and close opening streams.
     */
    public void recover() throws Throwable {
        this.deltaWAL.start();
        recover0(this.deltaWAL, this.streamManager, this.objectManager, LOGGER);
    }

    /**
     * Be called by {@link Failover} to recover from crash.
     * Note: {@link WriteAheadLog#start()} should be called before this method.
     */
    public void recover(WriteAheadLog deltaWAL, StreamManager streamManager, ObjectManager objectManager,
        Logger logger) throws Throwable {
        recover0(deltaWAL, streamManager, objectManager, logger);
    }

    /**
     * Recover WAL, upload WAL to S3 and close opening streams.
     * Note: {@link WriteAheadLog#start()} should be called before this method.
     */
    void recover0(WriteAheadLog deltaWAL, StreamManager streamManager, ObjectManager objectManager,
        Logger logger) throws Throwable {
        List<StreamMetadata> streams = streamManager.getOpeningStreams().get();

        RecoverResult recoverResult = recoverContinuousRecords(deltaWAL.recover(), streams, logger);
        LogCache.LogCacheBlock cacheBlock = recoverResult.cacheBlock;

        Map<Long, Long> streamEndOffsets = new HashMap<>();
        cacheBlock.records().forEach((streamId, records) -> {
            if (!records.isEmpty()) {
                streamEndOffsets.put(streamId, records.get(records.size() - 1).getLastOffset());
            }
        });

        if (cacheBlock.size() != 0) {
            logger.info("try recover from crash, recover records bytes size {}", cacheBlock.size());
            DeltaWALUploadTask task = DeltaWALUploadTask.builder().config(config).streamRecordsMap(cacheBlock.records())
                .objectManager(objectManager).s3Operator(s3Operator).executor(uploadWALExecutor).build();
            task.prepare().thenCompose(nil -> task.upload()).thenCompose(nil -> task.commit()).get();
            cacheBlock.records().forEach((streamId, records) -> records.forEach(StreamRecordBatch::release));
        }
        deltaWAL.reset().get();
        for (StreamMetadata stream : streams) {
            long newEndOffset = streamEndOffsets.getOrDefault(stream.streamId(), stream.endOffset());
            logger.info("recover try close stream {} with new end offset {}", stream, newEndOffset);
        }
        CompletableFuture.allOf(
            streams
                .stream()
                .map(s -> streamManager.closeStream(s.streamId(), s.epoch()))
                .toArray(CompletableFuture[]::new)
        ).get();

        // fail it if there is any invalid stream.
        recoverResult.firstException().ifPresent(e -> {
            throw e;
        });
    }

    @Override
    public void shutdown() {
        drainBackoffTask.cancel(false);
        for (WalWriteRequest request : backoffRecords) {
            request.cf.completeExceptionally(new IOException("S3Storage is shutdown"));
        }
        deltaWAL.shutdownGracefully();
        backgroundExecutor.shutdown();
        try {
            if (backgroundExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn("await backgroundExecutor timeout 10s");
            }
        } catch (InterruptedException e) {
            backgroundExecutor.shutdownNow();
            LOGGER.warn("await backgroundExecutor close fail", e);
        }
    }

    @Override
    @WithSpan
    public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
        final long startTime = System.nanoTime();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        // encoded before append to free heap ByteBuf.
        streamRecord.encoded();
        WalWriteRequest writeRequest = new WalWriteRequest(streamRecord, -1L, cf, context);
        handleAppendRequest(writeRequest);
        append0(context, writeRequest, false);
        cf.whenComplete((nil, ex) -> {
            streamRecord.release();
            StorageOperationStats.getInstance().appendStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
        });
        return cf;
    }

    /**
     * Append record to WAL.
     *
     * @return backoff status.
     */
    public boolean append0(AppendContext context, WalWriteRequest request, boolean fromBackoff) {
        // TODO: storage status check, fast fail the request when storage closed.
        if (!fromBackoff && !backoffRecords.isEmpty()) {
            backoffRecords.offer(request);
            return true;
        }
        if (!tryAcquirePermit()) {
            if (!fromBackoff) {
                backoffRecords.offer(request);
            }
            StorageOperationStats.getInstance().appendLogCacheFullStats.record(0L);
            if (System.currentTimeMillis() - lastLogTimestamp > 1000L) {
                LOGGER.warn("[BACKOFF] log cache size {} is larger than {}", deltaWALCache.size(), maxDeltaWALCacheSize);
                lastLogTimestamp = System.currentTimeMillis();
            }
            return true;
        }
        WriteAheadLog.AppendResult appendResult;
        try {
            try {
                StreamRecordBatch streamRecord = request.record;
                streamRecord.retain();
                Lock lock = confirmOffsetCalculator.addLock();
                lock.lock();
                try {
                    appendResult = deltaWAL.append(new TraceContext(context), streamRecord.encoded());
                } finally {
                    lock.unlock();
                }
            } catch (WriteAheadLog.OverCapacityException e) {
                // the WAL write data align with block, 'WAL is full but LogCacheBlock is not full' may happen.
                confirmOffsetCalculator.update();
                forceUpload(LogCache.MATCH_ALL_STREAMS);
                if (!fromBackoff) {
                    backoffRecords.offer(request);
                }
                if (System.currentTimeMillis() - lastLogTimestamp > 1000L) {
                    LOGGER.warn("[BACKOFF] log over capacity", e);
                    lastLogTimestamp = System.currentTimeMillis();
                }
                return true;
            }
            request.offset = appendResult.recordOffset();
            confirmOffsetCalculator.add(request);
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED] append WAL fail", e);
            request.cf.completeExceptionally(e);
            return false;
        }
        appendResult.future().whenComplete((nil, ex) -> {
            if (ex != null) {
                // no exception should be thrown from the WAL
                LOGGER.error("[UNEXPECTED] append WAL fail, request {}", request, ex);
                return;
            }
            handleAppendCallback(request);
        });
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean tryAcquirePermit() {
        return deltaWALCache.size() < maxDeltaWALCacheSize;
    }

    private void tryDrainBackoffRecords() {
        try {
            for (; ; ) {
                WalWriteRequest request = backoffRecords.peek();
                if (request == null) {
                    break;
                }
                if (append0(request.context, request, true)) {
                    LOGGER.warn("try drain backoff record fail, still backoff");
                    break;
                }
                backoffRecords.poll();
            }
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED] tryDrainBackoffRecords fail", e);
        }
    }

    @Override
    @WithSpan
    public CompletableFuture<ReadDataBlock> read(FetchContext context,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        final long startTime = System.nanoTime();
        CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
        FutureUtil.propagate(read0(context, streamId, startOffset, endOffset, maxBytes), cf);
        cf.whenComplete((nil, ex) -> StorageOperationStats.getInstance().readStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS)));
        return cf;
    }

    @WithSpan
    private CompletableFuture<ReadDataBlock> read0(FetchContext context,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        List<StreamRecordBatch> logCacheRecords = deltaWALCache.get(context, streamId, startOffset, endOffset, maxBytes);
        if (!logCacheRecords.isEmpty() && logCacheRecords.get(0).getBaseOffset() <= startOffset) {
            return CompletableFuture.completedFuture(new ReadDataBlock(logCacheRecords, CacheAccessType.DELTA_WAL_CACHE_HIT));
        }
        if (context.readOptions().fastRead()) {
            // fast read fail fast when need read from block cache.
            logCacheRecords.forEach(StreamRecordBatch::release);
            logCacheRecords.clear();
            return CompletableFuture.failedFuture(FAST_READ_FAIL_FAST_EXCEPTION);
        }
        if (!logCacheRecords.isEmpty()) {
            endOffset = logCacheRecords.get(0).getBaseOffset();
        }
        long finalEndOffset = endOffset;
        Timeout timeout = timeoutDetect.newTimeout(t -> LOGGER.error("[POTENTIAL_BUG] read from block cache timeout, stream={}, [{},{}), maxBytes: {}", streamId, startOffset, finalEndOffset, maxBytes), 1, TimeUnit.MINUTES);
        return blockCache.read(context, streamId, startOffset, endOffset, maxBytes).thenApply(blockCacheRst -> {
            List<StreamRecordBatch> rst = new ArrayList<>(blockCacheRst.getRecords());
            int remainingBytesSize = maxBytes - rst.stream().mapToInt(StreamRecordBatch::size).sum();
            int readIndex = -1;
            for (int i = 0; i < logCacheRecords.size() && remainingBytesSize > 0; i++) {
                readIndex = i;
                StreamRecordBatch record = logCacheRecords.get(i);
                rst.add(record);
                remainingBytesSize -= record.size();
            }
            try {
                continuousCheck(rst);
            } catch (IllegalArgumentException e) {
                blockCacheRst.getRecords().forEach(StreamRecordBatch::release);
                throw e;
            }
            if (readIndex < logCacheRecords.size()) {
                // release unnecessary record
                logCacheRecords.subList(readIndex + 1, logCacheRecords.size()).forEach(StreamRecordBatch::release);
            }
            return new ReadDataBlock(rst, blockCacheRst.getCacheAccessType());
        }).whenComplete((rst, ex) -> {
            handleTimeout(timeout, streamId, startOffset, finalEndOffset, maxBytes);
            if (ex != null) {
                LOGGER.error("read from block cache failed, stream={}, {}-{}, maxBytes: {}",
                    streamId, startOffset, finalEndOffset, maxBytes, ex);
                logCacheRecords.forEach(StreamRecordBatch::release);
            }
        });
    }

    private void handleTimeout(Timeout timeout, long streamId, long startOffset, long finalEndOffset, int maxBytes) {
        if (timeout.isExpired()) {
            LOGGER.error("[POTENTIAL_BUG_RECOVERED] read from block cache completed, stream={}, [{},{}), maxBytes: {}",
                streamId, startOffset, finalEndOffset, maxBytes);
        } else {
            timeout.cancel();
        }
    }

    private void continuousCheck(List<StreamRecordBatch> records) {
        long expectStartOffset = -1L;
        for (StreamRecordBatch record : records) {
            if (expectStartOffset == -1L || record.getBaseOffset() == expectStartOffset) {
                expectStartOffset = record.getLastOffset();
            } else {
                throw new IllegalArgumentException(String.format("Continuous check failed, expect offset: %d," +
                    " actual: %d, records: %s", expectStartOffset, record.getBaseOffset(), records));
            }
        }
    }

    /**
     * Force upload stream WAL cache to S3. Use group upload to avoid generate too many S3 objects when broker shutdown.
     * {@code streamId} can be {@link LogCache#MATCH_ALL_STREAMS} to force upload all streams.
     */
    @Override
    public CompletableFuture<Void> forceUpload(long streamId) {
        final long startTime = System.nanoTime();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        // Wait for a while to group force upload tasks.
        forceUploadTicker.tick().whenComplete((nil, ex) -> {
            StorageOperationStats.getInstance().forceUploadWALAwaitStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
            uploadDeltaWAL(streamId, true);
            // Wait for all tasks contains streamId complete.
            FutureUtil.propagate(CompletableFuture.allOf(this.inflightWALUploadTasks.stream()
                .filter(it -> it.cache.containsStream(streamId))
                .map(it -> it.cf).toArray(CompletableFuture[]::new)), cf);
            if (LogCache.MATCH_ALL_STREAMS != streamId) {
                callbackSequencer.tryFree(streamId);
            }
        });
        cf.whenComplete((nil, ex) -> StorageOperationStats.getInstance().forceUploadWALCompleteStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS)));
        return cf;
    }

    private void handleAppendRequest(WalWriteRequest request) {
        callbackSequencer.before(request);
    }

    private void handleAppendCallback(WalWriteRequest request) {
        suppress(() -> handleAppendCallback0(request), LOGGER);
    }

    private void handleAppendCallback0(WalWriteRequest request) {
        final long startTime = System.nanoTime();
        List<WalWriteRequest> waitingAckRequests;
        Lock lock = getStreamCallbackLock(request.record.getStreamId());
        lock.lock();
        try {
            waitingAckRequests = callbackSequencer.after(request);
            waitingAckRequests.forEach(r -> r.record.retain());
            for (WalWriteRequest waitingAckRequest : waitingAckRequests) {
                boolean full = deltaWALCache.put(waitingAckRequest.record);
                waitingAckRequest.confirmed = true;
                if (full) {
                    // cache block is full, trigger WAL upload.
                    uploadDeltaWAL();
                }
            }
        } finally {
            lock.unlock();
        }
        for (WalWriteRequest waitingAckRequest : waitingAckRequests) {
            waitingAckRequest.cf.complete(null);
        }
        StorageOperationStats.getInstance().appendCallbackStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
    }

    private Lock getStreamCallbackLock(long streamId) {
        return streamCallbackLocks[(int) ((streamId & Long.MAX_VALUE) % NUM_STREAM_CALLBACK_LOCKS)];
    }

    @SuppressWarnings("UnusedReturnValue")
    CompletableFuture<Void> uploadDeltaWAL() {
        return uploadDeltaWAL(LogCache.MATCH_ALL_STREAMS, false);
    }

    CompletableFuture<Void> uploadDeltaWAL(long streamId, boolean force) {
        synchronized (deltaWALCache) {
            deltaWALCache.setConfirmOffset(confirmOffsetCalculator.get());
            Optional<LogCache.LogCacheBlock> blockOpt = deltaWALCache.archiveCurrentBlockIfContains(streamId);
            if (blockOpt.isPresent()) {
                LogCache.LogCacheBlock logCacheBlock = blockOpt.get();
                DeltaWALUploadTaskContext context = new DeltaWALUploadTaskContext(logCacheBlock);
                context.objectManager = this.objectManager;
                context.force = force;
                return uploadDeltaWAL(context);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    // only for test
    CompletableFuture<Void> uploadDeltaWAL(LogCache.LogCacheBlock logCacheBlock) {
        DeltaWALUploadTaskContext context = new DeltaWALUploadTaskContext(logCacheBlock);
        context.objectManager = this.objectManager;
        return uploadDeltaWAL(context);
    }

    /**
     * Upload cache block to S3. The earlier cache block will have smaller objectId and commit first.
     */
    CompletableFuture<Void> uploadDeltaWAL(DeltaWALUploadTaskContext context) {
        context.timer = new TimerUtil();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        context.cf = cf;
        inflightWALUploadTasks.add(context);
        backgroundExecutor.execute(() -> FutureUtil.exec(() -> uploadDeltaWAL0(context), cf, LOGGER, "uploadDeltaWAL"));
        cf.whenComplete((nil, ex) -> {
            StorageOperationStats.getInstance().uploadWALCompleteStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
            inflightWALUploadTasks.remove(context);
            if (ex != null) {
                LOGGER.error("upload delta WAL fail", ex);
            }
        });
        return cf;
    }

    private void uploadDeltaWAL0(DeltaWALUploadTaskContext context) {
        // calculate upload rate
        long elapsed = System.currentTimeMillis() - context.cache.createdTimestamp();
        double rate;
        if (context.force || elapsed <= 100L) {
            rate = Long.MAX_VALUE;
        } else {
            rate = context.cache.size() * 1000.0 / Math.min(5000L, elapsed);
            if (rate > maxDataWriteRate) {
                maxDataWriteRate = rate;
            }
            rate = maxDataWriteRate;
        }
        context.task = DeltaWALUploadTask.builder()
            .config(config)
            .streamRecordsMap(context.cache.records())
            .objectManager(objectManager)
            .s3Operator(s3Operator)
            .executor(uploadWALExecutor)
            .rate(rate)
            .build();
        boolean walObjectPrepareQueueEmpty = walPrepareQueue.isEmpty();
        walPrepareQueue.add(context);
        if (!walObjectPrepareQueueEmpty) {
            // there is another WAL upload task is preparing, just return.
            return;
        }
        prepareDeltaWALUpload(context);
    }

    private void prepareDeltaWALUpload(DeltaWALUploadTaskContext context) {
        context.task.prepare().thenAcceptAsync(nil -> {
            StorageOperationStats.getInstance().uploadWALPrepareStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
            // 1. poll out current task and trigger upload.
            DeltaWALUploadTaskContext peek = walPrepareQueue.poll();
            Objects.requireNonNull(peek).task.upload().thenAccept(nil2 -> StorageOperationStats.getInstance()
                .uploadWALUploadStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS)));
            // 2. add task to commit queue.
            boolean walObjectCommitQueueEmpty = walCommitQueue.isEmpty();
            walCommitQueue.add(peek);
            if (walObjectCommitQueueEmpty) {
                commitDeltaWALUpload(peek);
            }
            // 3. trigger next task to prepare.
            DeltaWALUploadTaskContext next = walPrepareQueue.peek();
            if (next != null) {
                prepareDeltaWALUpload(next);
            }
        }, backgroundExecutor);
    }

    private void commitDeltaWALUpload(DeltaWALUploadTaskContext context) {
        context.task.commit().thenAcceptAsync(nil -> {
            StorageOperationStats.getInstance().uploadWALCommitStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
            // 1. poll out current task
            walCommitQueue.poll();
            if (context.cache.confirmOffset() != 0) {
                LOGGER.info("try trim WAL to {}", context.cache.confirmOffset());
                deltaWAL.trim(context.cache.confirmOffset());
            }
            // transfer records ownership to block cache.
            freeCache(context.cache);
            context.cf.complete(null);

            // 2. trigger next task to commit.
            DeltaWALUploadTaskContext next = walCommitQueue.peek();
            if (next != null) {
                commitDeltaWALUpload(next);
            }
        }, backgroundExecutor).exceptionally(ex -> {
            LOGGER.error("Unexpected exception when commit stream set object", ex);
            context.cf.completeExceptionally(ex);
            System.err.println("Unexpected exception when commit stream set object");
            //noinspection CallToPrintStackTrace
            ex.printStackTrace();
            Runtime.getRuntime().halt(1);
            return null;
        });
    }

    private void freeCache(LogCache.LogCacheBlock cacheBlock) {
        deltaWALCache.markFree(cacheBlock);
    }

    /**
     * WALConfirmOffsetCalculator is used to calculate the confirmed offset of WAL.
     */
    static class WALConfirmOffsetCalculator {
        public static final long NOOP_OFFSET = -1L;
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Queue<WalWriteRequestWrapper> queue = new ConcurrentLinkedQueue<>();
        private final AtomicLong confirmOffset = new AtomicLong(NOOP_OFFSET);

        public WALConfirmOffsetCalculator() {
            // Update the confirmed offset periodically.
            Threads.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("wal-calculator-update-confirm-offset", true), LOGGER)
                .scheduleAtFixedRate(this::update, 100, 100, TimeUnit.MILLISECONDS);
        }

        /**
         * Lock of {@link #add}.
         * Operations of assigning offsets, for example {@link WriteAheadLog#append}, need to be performed while holding the lock.
         */
        public Lock addLock() {
            return rwLock.readLock();
        }

        public void add(WalWriteRequest request) {
            assert null != request;
            queue.add(new WalWriteRequestWrapper(request));
        }

        /**
         * Return the offset before and including which all records have been persisted.
         * Note: It is updated by {@link #update} periodically, and is not real-time.
         */
        public Long get() {
            return confirmOffset.get();
        }

        /**
         * Calculate and update the confirmed offset.
         */
        public void update() {
            long offset = calculate();
            if (offset != NOOP_OFFSET) {
                confirmOffset.set(offset);
            }
        }

        /**
         * Calculate the offset before and including which all records have been persisted.
         * All records whose offset is not larger than the returned offset will be removed from the queue.
         * It returns {@link #NOOP_OFFSET} if the first record is not persisted yet.
         */
        synchronized private long calculate() {
            Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                // Insert a flag.
                queue.add(WalWriteRequestWrapper.flag());
            } finally {
                lock.unlock();
            }

            long minUnconfirmedOffset = Long.MAX_VALUE;
            boolean reachFlag = false;
            for (WalWriteRequestWrapper wrapper : queue) {
                // Iterate the queue to find the min unconfirmed offset.
                if (wrapper.isFlag()) {
                    // Reach the flag.
                    reachFlag = true;
                    break;
                }
                WalWriteRequest request = wrapper.request;
                assert request.offset != NOOP_OFFSET;
                if (!request.confirmed) {
                    minUnconfirmedOffset = Math.min(minUnconfirmedOffset, request.offset);
                }
            }
            assert reachFlag;

            long confirmedOffset = NOOP_OFFSET;
            // Iterate the queue to find the max offset less than minUnconfirmedOffset.
            // Remove all records whose offset is less than minUnconfirmedOffset.
            for (Iterator<WalWriteRequestWrapper> iterator = queue.iterator(); iterator.hasNext(); ) {
                WalWriteRequestWrapper wrapper = iterator.next();
                if (wrapper.isFlag()) {
                    /// Reach and remove the flag.
                    iterator.remove();
                    break;
                }
                WalWriteRequest request = wrapper.request;
                if (request.confirmed && request.offset < minUnconfirmedOffset) {
                    confirmedOffset = Math.max(confirmedOffset, request.offset);
                    iterator.remove();
                }
            }
            return confirmedOffset;
        }

        /**
         * Wrapper of {@link WalWriteRequest}.
         * When the {@code request} is null, it is used as a flag.
         */
        static final class WalWriteRequestWrapper {
            private final WalWriteRequest request;

            /**
             *
             */
            WalWriteRequestWrapper(WalWriteRequest request) {
                this.request = request;
            }

            static WalWriteRequestWrapper flag() {
                return new WalWriteRequestWrapper(null);
            }

            public boolean isFlag() {
                return request == null;
            }

            public WalWriteRequest request() {
                return request;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this)
                    return true;
                if (obj == null || obj.getClass() != this.getClass())
                    return false;
                var that = (WalWriteRequestWrapper) obj;
                return Objects.equals(this.request, that.request);
            }

            @Override
            public int hashCode() {
                return Objects.hash(request);
            }

            @Override
            public String toString() {
                return "WalWriteRequestWrapper[" +
                    "request=" + request + ']';
            }

        }
    }

    /**
     * WALCallbackSequencer is used to sequence the unordered returned persistent data.
     */
    static class WALCallbackSequencer {
        private final Map<Long, Queue<WalWriteRequest>> stream2requests = new ConcurrentHashMap<>();

        /**
         * Add request to stream sequence queue.
         * When the {@code request.record.getStreamId()} is different, concurrent calls are allowed.
         * When the {@code request.record.getStreamId()} is the same, concurrent calls are not allowed. And it is
         * necessary to ensure that calls are made in the order of increasing offsets.
         */
        public void before(WalWriteRequest request) {
            try {
                Queue<WalWriteRequest> streamRequests = stream2requests.computeIfAbsent(request.record.getStreamId(),
                    s -> new ConcurrentLinkedQueue<>());
                streamRequests.add(request);
            } catch (Throwable ex) {
                request.cf.completeExceptionally(ex);
            }
        }

        /**
         * Try pop sequence persisted request from stream queue and move forward wal inclusive confirm offset.
         * When the {@code request.record.getStreamId()} is different, concurrent calls are allowed.
         * When the {@code request.record.getStreamId()} is the same, concurrent calls are not allowed.
         *
         * @return popped sequence persisted request.
         */
        public List<WalWriteRequest> after(WalWriteRequest request) {
            request.persisted = true;

            // Try to pop sequential persisted requests from the queue.
            long streamId = request.record.getStreamId();
            Queue<WalWriteRequest> streamRequests = stream2requests.get(streamId);
            WalWriteRequest peek = streamRequests.peek();
            if (peek == null || peek.offset != request.offset) {
                return Collections.emptyList();
            }

            LinkedList<WalWriteRequest> rst = new LinkedList<>();
            WalWriteRequest poll = streamRequests.poll();
            assert poll == peek;
            rst.add(poll);

            for (; ; ) {
                peek = streamRequests.peek();
                if (peek == null || !peek.persisted) {
                    break;
                }
                poll = streamRequests.poll();
                assert poll == peek;
                assert poll.record.getBaseOffset() == rst.getLast().record.getLastOffset();
                rst.add(poll);
            }

            return rst;
        }

        /**
         * Try free stream related resources.
         */
        public void tryFree(long streamId) {
            Queue<?> queue = stream2requests.get(streamId);
            if (queue != null && queue.isEmpty()) {
                stream2requests.remove(streamId, queue);
            }
        }
    }

    public static class DeltaWALUploadTaskContext {
        TimerUtil timer;
        LogCache.LogCacheBlock cache;
        DeltaWALUploadTask task;
        CompletableFuture<Void> cf;
        ObjectManager objectManager;
        /**
         * Indicate whether to force upload the delta wal.
         * If true, the delta wal will be uploaded without rate limit.
         */
        boolean force;

        public DeltaWALUploadTaskContext(LogCache.LogCacheBlock cache) {
            this.cache = cache;
        }
    }

    class LogCacheEvictOOMHandler implements ByteBufAlloc.OOMHandler {
        @Override
        public int handle(int memoryRequired) {
            try {
                CompletableFuture<Integer> cf = new CompletableFuture<>();
                FutureUtil.exec(() -> cf.complete(deltaWALCache.forceFree(memoryRequired)), cf, LOGGER, "handleOOM");
                return cf.get();
            } catch (Throwable e) {
                return 0;
            }
        }
    }

    /**
     * Recover result of {@link #recoverContinuousRecords(Iterator, List, Logger)}
     * Only streams not in {@link #invalidStreams} should be uploaded and closed.
     */
    static class RecoverResult {
        /**
         * Recovered records. All {@link #invalidStreams} have been filtered out.
         */
        LogCache.LogCacheBlock cacheBlock;

        /**
         * Invalid streams, for example, the recovered start offset mismatches the stream end offset from controller.
         * Key is streamId, value is the exception.
         */
        Map<Long, RuntimeException> invalidStreams = new HashMap<>();

        public Optional<RuntimeException> firstException() {
            return invalidStreams.values().stream().findFirst();
        }
    }
}
