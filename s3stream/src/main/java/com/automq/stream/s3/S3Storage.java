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

package com.automq.stream.s3;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.Context;
import com.automq.stream.api.LinkRecordDecoder;
import com.automq.stream.api.exceptions.FastReadFailFastException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.StorageFailureHandler;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.utils.FutureTicker;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;

import static com.automq.stream.s3.ByteBufAlloc.DECODE_RECORD;
import static com.automq.stream.s3.ByteBufAlloc.ENCODE_RECORD;
import static com.automq.stream.utils.FutureUtil.suppress;

public class S3Storage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Storage.class);
    private static final FastReadFailFastException FAST_READ_FAIL_FAST_EXCEPTION = new FastReadFailFastException();
    private static final ByteBufSeqAlloc DECODE_LINK_RECORD_INSTANT_ALLOC = new ByteBufSeqAlloc(DECODE_RECORD, 1);
    private static final ByteBufSupplier ENCODE_LINK_RECORD_INSTANT_ALLOC = new DefaultByteBufSupplier(ENCODE_RECORD);

    private static final int NUM_STREAM_CALLBACK_LOCKS = 128;

    private static LinkRecordDecoder linkRecordDecoder = LinkRecordDecoder.NOOP;

    public static void setLinkRecordDecoder(
        LinkRecordDecoder linkRecordDecoder) {
        S3Storage.linkRecordDecoder = linkRecordDecoder;
    }

    public static LinkRecordDecoder getLinkRecordDecoder() {
        return linkRecordDecoder;
    }

    protected final Config config;
    private final WriteAheadLog deltaWAL;
    private final ConfirmWAL confirmWAL;
    /**
     * WAL log cache
     */
    private final LogCache deltaWALCache;
    private final LogCache snapshotReadCache;
    private final Queue<DeltaWALUploadTaskContext> walPrepareQueue = new LinkedList<>();
    private final Queue<DeltaWALUploadTaskContext> walCommitQueue = new LinkedList<>();
    private final List<DeltaWALUploadTaskContext> inflightWALUploadTasks = new CopyOnWriteArrayList<>();
    private BlockingQueue<LazyCommit> lazyUploadQueue = new LinkedBlockingQueue<>();

    /**
     * A lock to ensure only one thread can trigger {@link #forceUpload()} in {@link #maybeForceUpload()}
     */
    private final AtomicBoolean forceUploadScheduled = new AtomicBoolean();
    /**
     * A lock to ensure only one thread can trigger {@link #forceUpload()} in {@link #forceUploadCallback()}
     */
    private final AtomicBoolean needForceUpload = new AtomicBoolean();
    private final ScheduledExecutorService backgroundExecutor = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("s3-storage-background", true), LOGGER);
    private final ExecutorService uploadWALExecutor = Threads.newFixedThreadPoolWithMonitor(
        4, "s3-storage-upload-wal", true, LOGGER);
    private final DelayTrim delayTrim;
    /**
     * A ticker used for batching force upload WAL.
     *
     * @see #forceUpload
     */
    private final FutureTicker forceUploadTicker = new FutureTicker(100, TimeUnit.MILLISECONDS, backgroundExecutor);
    private final Queue<WalWriteRequest> backoffRecords = new LinkedBlockingQueue<>();
    private final ScheduledFuture<?> drainBackoffTask;
    protected final StreamManager streamManager;
    protected final ObjectManager objectManager;
    protected final ObjectStorage objectStorage;
    protected final S3BlockCache blockCache;
    protected final StorageFailureHandler storageFailureHandler;
    /**
     * Stream callback locks. Used to ensure the stream callbacks will not be called concurrently.
     *
     * @see #handleAppendCallback
     */
    private final Lock[] streamCallbackLocks = IntStream.range(0, NUM_STREAM_CALLBACK_LOCKS).mapToObj(i -> new ReentrantLock()).toArray(Lock[]::new);
    private final EventLoop[] callbackExecutors = IntStream.range(0, Systems.CPU_CORES).mapToObj(i -> new EventLoop("AUTOMQ_S3STREAM_APPEND_CALLBACK-" + i))
        .toArray(EventLoop[]::new);

    private long lastLogTimestamp = 0L;
    private volatile double maxDataWriteRate = 0.0;

    private final AtomicLong pendingUploadBytes = new AtomicLong(0L);

    @SuppressWarnings("this-escape")
    public S3Storage(Config config, WriteAheadLog deltaWAL, StreamManager streamManager, ObjectManager objectManager,
        S3BlockCache blockCache, ObjectStorage objectStorage, StorageFailureHandler storageFailureHandler) {
        this.config = config;
        this.deltaWAL = deltaWAL;
        this.blockCache = blockCache;
        long deltaWALCacheSize = config.walCacheSize();
        long snapshotReadCacheSize = 0;
        if (config.snapshotReadEnable()) {
            deltaWALCacheSize = Math.max(config.walCacheSize() / 3, 10L * 1024 * 1024);
            snapshotReadCacheSize = Math.max(config.walCacheSize() / 3 * 2, 10L * 1024 * 1024);
            delayTrim = new DelayTrim(TimeUnit.SECONDS.toMillis(30));
        } else {
            delayTrim = new DelayTrim(0);
        }
        // Adjust the walUploadThreshold to be less than 2/5 of deltaWALCacheSize to avoid the upload speed being slower than the append speed.
        long walUploadThreadhold = Math.min(deltaWALCacheSize * 2 / 5, config.walUploadThreshold());
        if (walUploadThreadhold != config.walUploadThreshold()) {
            LOGGER.info("The configured walUploadThreshold {} is too large, adjust to {}", config.walUploadThreshold(), walUploadThreadhold);
        }
        this.deltaWALCache = new LogCache(deltaWALCacheSize, walUploadThreadhold, config.maxStreamNumPerStreamSetObject());
        this.snapshotReadCache = new LogCache(snapshotReadCacheSize, Math.max(snapshotReadCacheSize / 6, 1));
        S3StreamMetricsManager.registerDeltaWalCacheSizeSupplier(() -> deltaWALCache.size() + snapshotReadCache.size());
        Context.instance().snapshotReadCache(new SnapshotReadCache(streamManager, snapshotReadCache, objectStorage, linkRecordDecoder));
        this.confirmWAL = new ConfirmWAL(deltaWAL, lazyCommit -> lazyUpload(lazyCommit));
        Context.instance().confirmWAL(this.confirmWAL);
        this.streamManager = streamManager;
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
        this.storageFailureHandler = storageFailureHandler;
        this.drainBackoffTask = this.backgroundExecutor.scheduleWithFixedDelay(this::tryDrainBackoffRecords, 100, 100, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerInflightWALUploadTasksCountSupplier(this.inflightWALUploadTasks::size);
        S3StreamMetricsManager.registerDeltaWalPendingUploadBytesSupplier(this.pendingUploadBytes::get);
        if (config.walUploadIntervalMs() > 0) {
            this.backgroundExecutor.scheduleWithFixedDelay(this::maybeForceUpload, config.walUploadIntervalMs(), config.walUploadIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private static void releaseRecords(Collection<StreamRecordBatch> records) {
        records.forEach(StreamRecordBatch::release);
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
        Logger logger) throws InterruptedException, ExecutionException {
        List<StreamMetadata> streams = streamManager.getOpeningStreams().get();
        Map<Long, Long> streamEndOffsets = streams.stream().collect(Collectors.toMap(StreamMetadata::streamId, StreamMetadata::endOffset));
        Iterator<RecoverResult> iterator = deltaWAL.recover();

        WALRecovery.recover(iterator, streamEndOffsets, 1 << 29, logger, cacheBlock -> {
            try {
                uploadRecoveredRecords(objectManager, cacheBlock, logger);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        deltaWAL.reset().get();
        closeStreams(streamManager, streams, streamEndOffsets, logger);
    }

    private void uploadRecoveredRecords(ObjectManager objectManager, LogCache.LogCacheBlock cacheBlock, Logger logger)
        throws InterruptedException, ExecutionException {
        if (cacheBlock.size() != 0) {
            logger.info("try recover from crash, recover records bytes size {}", cacheBlock.size());
            try {
                UploadWriteAheadLogTask task = newUploadWriteAheadLogTask(cacheBlock.records(), objectManager, Long.MAX_VALUE);
                task.prepare().thenCompose(nil -> task.upload()).thenCompose(nil -> task.commit()).get();
            } finally {
                WALRecovery.releaseAllRecords(cacheBlock.records().values());
            }
        }
    }

    private static void closeStreams(StreamManager streamManager, List<StreamMetadata> streams,
        Map<Long, Long> streamEndOffsets, Logger logger) throws InterruptedException, ExecutionException {
        for (StreamMetadata stream : streams) {
            long newEndOffset = streamEndOffsets.getOrDefault(stream.streamId(), stream.endOffset());
            logger.info("recover try close stream {} with new end offset {}", stream, newEndOffset);
        }
        CompletableFuture.allOf(
            streams.stream()
                .map(s -> streamManager.closeStream(s.streamId(), s.epoch()))
                .toArray(CompletableFuture[]::new)
        ).get();
    }

    @Override
    public void shutdown() {
        drainBackoffTask.cancel(false);
        for (WalWriteRequest request : backoffRecords) {
            request.cf.completeExceptionally(new IOException("S3Storage is shutdown"));
        }
        suppress(() -> delayTrim.close(), LOGGER);
        deltaWAL.shutdownGracefully();
        backgroundExecutor.shutdown();
        try {
            if (!backgroundExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn("await backgroundExecutor timeout 10s");
            }
        } catch (InterruptedException e) {
            backgroundExecutor.shutdownNow();
            LOGGER.warn("await backgroundExecutor close fail", e);
        }
        for (EventLoop executor : callbackExecutors) {
            executor.shutdownGracefully();
        }
    }

    @Override
    @WithSpan
    public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
        final long startTime = System.nanoTime();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        WalWriteRequest writeRequest = new WalWriteRequest(streamRecord, null, cf, context);
        append0(context, writeRequest, false);
        return cf.whenComplete((nil, ex) -> {
            streamRecord.release();
            StorageOperationStats.getInstance().appendStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
        });
    }

    /**
     * Append record to WAL.
     *
     * @return backoff status.
     */
    @SuppressWarnings("NPathComplexity")
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
                LOGGER.warn("[BACKOFF] log cache size {} is larger than {}", deltaWALCache.size(), deltaWALCache.capacity());
                lastLogTimestamp = System.currentTimeMillis();
            }
            return true;
        }
        CompletableFuture<AppendResult> appendCf;
        try {
            try {
                if (context.linkRecord() == null) {
                    StreamRecordBatch streamRecord = request.record;
                    streamRecord.retain();
                    appendCf = deltaWAL.append(new TraceContext(context), streamRecord);
                } else {
                    StreamRecordBatch record = request.record;
                    StreamRecordBatch linkStreamRecord = toLinkRecord(record, context.linkRecord().retainedSlice());
                    appendCf = deltaWAL.append(new TraceContext(context), linkStreamRecord);
                }

            } catch (OverCapacityException e) {
                // the WAL write data align with block, 'WAL is full but LogCacheBlock is not full' may happen.
                maybeForceUpload();
                if (!fromBackoff) {
                    backoffRecords.offer(request);
                }
                if (System.currentTimeMillis() - lastLogTimestamp > 1000L) {
                    LOGGER.warn("[BACKOFF] log over capacity", e);
                    lastLogTimestamp = System.currentTimeMillis();
                }
                return true;
            }
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED] append WAL fail", e);
            request.cf.completeExceptionally(e);
            return false;
        }
        appendCf.thenAccept(rst -> {
            request.offset = rst.recordOffset();
            // Execute the ConfirmWAL#append before run callback.
            if (request.context.linkRecord() == null) {
                this.confirmWAL.onAppend(request.record, rst.recordOffset(), rst.nextOffset());
            } else {
                StreamRecordBatch linkRecord = toLinkRecord(request.record, request.context.linkRecord());
                this.confirmWAL.onAppend(linkRecord, rst.recordOffset(), rst.nextOffset());
                linkRecord.release();
            }
            handleAppendCallback(request);
        }).whenComplete((nil, ex) -> {
            if (ex != null) {
                LOGGER.error("append WAL fail", ex);
                storageFailureHandler.handle(ex);
                return;
            }
        });
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean tryAcquirePermit() {
        return deltaWALCache.size() < deltaWALCache.capacity();
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
        cf.whenComplete((nil, ex) -> StorageOperationStats.getInstance().readStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS)));
        return cf;
    }

    public LogCache snapshotReadCache() {
        return snapshotReadCache;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @WithSpan
    private CompletableFuture<ReadDataBlock> read0(FetchContext context,
        @SpanAttribute long streamId,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        LogCache firstCache = context.readOptions().snapshotRead() ? snapshotReadCache : deltaWALCache;
        List<StreamRecordBatch> logCacheRecords = firstCache.get(context, streamId, startOffset, endOffset, maxBytes);
        if (!logCacheRecords.isEmpty() && logCacheRecords.get(0).getBaseOffset() <= startOffset) {
            return CompletableFuture.completedFuture(new ReadDataBlock(logCacheRecords, CacheAccessType.DELTA_WAL_CACHE_HIT));
        }
        if (context.readOptions().fastRead()) {
            // fast read fail fast when need read from block cache.
            releaseRecords(logCacheRecords);
            logCacheRecords.clear();
            return CompletableFuture.failedFuture(FAST_READ_FAIL_FAST_EXCEPTION);
        }
        if (!logCacheRecords.isEmpty()) {
            endOffset = logCacheRecords.get(0).getBaseOffset();
        }
        long finalEndOffset = endOffset;
        CompletableFuture<ReadDataBlock> cf = blockCache.read(context, streamId, startOffset, endOffset, maxBytes).thenApply(blockCacheRst -> {
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
                releaseRecords(blockCacheRst.getRecords());
                throw e;
            }
            if (readIndex < logCacheRecords.size()) {
                // release unnecessary record
                releaseRecords(logCacheRecords.subList(readIndex + 1, logCacheRecords.size()));
            }
            return new ReadDataBlock(rst, blockCacheRst.getCacheAccessType());
        }).whenComplete((rst, ex) -> {
            if (ex != null) {
                LOGGER.error("read from block cache failed, stream={}, {}-{}, maxBytes: {}",
                    streamId, startOffset, finalEndOffset, maxBytes, ex);
                releaseRecords(logCacheRecords);
            }
        });
        return FutureUtil.timeoutWithNewReturn(cf, 2, TimeUnit.MINUTES, () -> {
            LOGGER.error("[POTENTIAL_BUG] read from block cache timeout, stream={}, [{},{}), maxBytes: {}", streamId, startOffset, finalEndOffset, maxBytes);
            cf.thenAccept(readDataBlock -> {
                releaseRecords(readDataBlock.getRecords());
            });
        });
    }

    private void continuousCheck(List<StreamRecordBatch> records) {
        long expectedStartOffset = -1L;
        for (StreamRecordBatch record : records) {
            if (expectedStartOffset == -1L || record.getBaseOffset() == expectedStartOffset) {
                expectedStartOffset = record.getLastOffset();
            } else {
                throw new IllegalArgumentException(String.format("Continuous check failed, expected offset: %d," +
                    " actual: %d, records: %s", expectedStartOffset, record.getBaseOffset(), records));
            }
        }
    }

    /**
     * Limit the number of inflight force upload tasks to 1 to avoid too many S3 objects.
     */
    private void maybeForceUpload() {
        if (hasInflightForceUploadTask()) {
            // There is already an inflight force upload task, trigger another one later after it completes.
            needForceUpload.set(true);
            return;
        }
        if (forceUploadScheduled.compareAndSet(false, true)) {
            forceUpload();
        } else {
            // There is already a force upload task scheduled, do nothing.
            needForceUpload.set(true);
        }
    }

    private boolean hasInflightForceUploadTask() {
        return inflightWALUploadTasks.stream().anyMatch(it -> it.force);
    }

    /**
     * Commit with lazy timeout. If in [0, lazyLingerMs), there is no other commit happened, then trigger a new commit.
     */
    private CompletableFuture<Void> lazyUpload(LazyCommit lazyCommit) {
        lazyUploadQueue.add(lazyCommit);
        backgroundExecutor.schedule(() -> {
            if (lazyUploadQueue.contains(lazyCommit)) {
                // If the queue does not contain the lazyCommit, it means another commit has happened after this lazyCommit.
                if (lazyCommit.lazyLingerMs == 0) {
                    // If the lazyLingerMs is 0, we need to force upload as soon as possible.
                    forceUpload();
                } else {
                    uploadDeltaWAL();
                }
            }
        }, lazyCommit.lazyLingerMs, TimeUnit.MILLISECONDS);
        return lazyCommit.awaitTrim ? lazyCommit.trimCf : lazyCommit.commitCf;
    }

    private void notifyLazyUpload(List<LazyCommit> tasks) {
        CompletableFuture.allOf(inflightWALUploadTasks.stream().map(t -> t.cf).collect(Collectors.toList()).toArray(new CompletableFuture[0]))
            .whenComplete((nil, ex) -> {
                for (LazyCommit task : tasks) {
                    if (ex != null) {
                        task.commitCf.completeExceptionally(ex);
                    } else {
                        task.commitCf.complete(null);
                    }
                }
            });

        CompletableFuture.allOf(inflightWALUploadTasks.stream().map(t -> t.trimCf).collect(Collectors.toList()).toArray(new CompletableFuture[0]))
            .whenComplete((nil, ex) -> {
                for (LazyCommit task : tasks) {
                    if (ex != null) {
                        task.trimCf.completeExceptionally(ex);
                    } else {
                        task.trimCf.complete(null);
                    }
                }
            });
    }

    private CompletableFuture<Void> forceUpload() {
        CompletableFuture<Void> cf = forceUpload(LogCache.MATCH_ALL_STREAMS);
        cf.whenComplete((nil, ignored) -> forceUploadCallback());
        return cf;
    }

    private void forceUploadCallback() {
        // Reset the force upload flag after the task completes.
        forceUploadScheduled.set(false);
        if (needForceUpload.compareAndSet(true, false)) {
            // Force upload needs to be triggered again.
            forceUpload();
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
            StorageOperationStats.getInstance().forceUploadWALAwaitStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
            uploadDeltaWAL(streamId, true);
            // Wait for all tasks contains streamId complete.
            FutureUtil.propagate(CompletableFuture.allOf(this.inflightWALUploadTasks.stream()
                .filter(it -> it.cache.containsStream(streamId))
                .map(it -> it.cf).toArray(CompletableFuture[]::new)), cf);
        });
        cf.whenComplete((nil, ex) -> StorageOperationStats.getInstance().forceUploadWALCompleteStats.record(
            TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS)));
        return cf;
    }

    private void handleAppendCallback(WalWriteRequest request) {
        final long startTime = System.nanoTime();
        request.record.retain();
        boolean added;
        synchronized (deltaWALCache) {
            // Because LogCacheBlock will use request.offset to execute WAL#trim after being uploaded,
            // this cache put order should keep consistence with WAL put order.
            added = deltaWALCache.put(request.record);
            if (!added) {
                // record was not added, archive current block and retry
                uploadDeltaWAL();
                added = deltaWALCache.put(request.record);
            }
            deltaWALCache.setLastRecordOffset(request.offset);
        }
        if (!added) {
            // cache block is full, trigger WAL upload.
            uploadDeltaWAL();
        }
        // parallel execute append callback in streamId based executor.
        EventLoop executor = callbackExecutors[Math.abs((int) (request.record.getStreamId() % callbackExecutors.length))];
        executor.execute(() -> {
            request.cf.complete(null);
            StorageOperationStats.getInstance().appendCallbackStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
        });
    }

    private Lock getStreamCallbackLock(long streamId) {
        return streamCallbackLocks[(int) ((streamId & Long.MAX_VALUE) % NUM_STREAM_CALLBACK_LOCKS)];
    }

    protected UploadWriteAheadLogTask newUploadWriteAheadLogTask(Map<Long, List<StreamRecordBatch>> streamRecordsMap,
        ObjectManager objectManager, double rate) {
        return DefaultUploadWriteAheadLogTask.builder().config(config).streamRecordsMap(streamRecordsMap)
            .objectManager(objectManager).objectStorage(objectStorage).executor(uploadWALExecutor).rate(rate).build();
    }

    @SuppressWarnings("UnusedReturnValue")
    CompletableFuture<Void> uploadDeltaWAL() {
        return uploadDeltaWAL(LogCache.MATCH_ALL_STREAMS, false);
    }

    CompletableFuture<Void> uploadDeltaWAL(long streamId, boolean force) {
        CompletableFuture<Void> cf;
        List<LazyCommit> lazyUploadTasks = new ArrayList<>();
        lazyUploadQueue.drainTo(lazyUploadTasks);

        synchronized (deltaWALCache) {
            Optional<LogCache.LogCacheBlock> blockOpt = deltaWALCache.archiveCurrentBlockIfContains(streamId);
            if (blockOpt.isPresent()) {
                LogCache.LogCacheBlock logCacheBlock = blockOpt.get();
                DeltaWALUploadTaskContext context = new DeltaWALUploadTaskContext(logCacheBlock);
                context.objectManager = this.objectManager;
                context.force = force;
                cf = uploadDeltaWAL(context);
            } else {
                cf = CompletableFuture.completedFuture(null);
            }
        }

        // notify lazy upload tasks
        notifyLazyUpload(lazyUploadTasks);
        return cf;
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

        long size = context.cache.size();
        pendingUploadBytes.addAndGet(size);

        if (context.force) {
            // trigger previous task burst.
            inflightWALUploadTasks.forEach(ctx -> {
                ctx.force = true;
                if (ctx.task != null) {
                    ctx.task.burst();
                }
            });
        }

        backgroundExecutor.execute(() -> FutureUtil.exec(() -> uploadDeltaWAL0(context), cf, LOGGER, "uploadDeltaWAL"));
        cf.whenComplete((nil, ex) -> {
            StorageOperationStats.getInstance().uploadWALCompleteStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
            pendingUploadBytes.addAndGet(-size);
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
            rate = context.cache.size() * 1000.0 / Math.min(20000L, elapsed);
            if (rate > maxDataWriteRate) {
                maxDataWriteRate = rate;
            }
            rate = maxDataWriteRate;
        }
        context.task = newUploadWriteAheadLogTask(context.cache.records(), objectManager, rate);
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
        }, backgroundExecutor).exceptionally(ex -> {
            LOGGER.error("Unexpected exception when prepare commit stream set object", ex);
            return null;
        });
    }

    private void commitDeltaWALUpload(DeltaWALUploadTaskContext context) {
        context.task.commit().thenAcceptAsync(nil -> {
            StorageOperationStats.getInstance().uploadWALCommitStats.record(context.timer.elapsedAs(TimeUnit.NANOSECONDS));
            // 1. poll out current task
            walCommitQueue.poll();
            if (context.cache.lastRecordOffset() != null) {
                delayTrim.trim(context.cache.lastRecordOffset(), context.trimCf);
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

    class DelayTrim {
        private final long delayMillis;
        private final BlockingQueue<Pair<RecordOffset, CompletableFuture<Void>>> offsets = new LinkedBlockingQueue<>();

        public DelayTrim(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        public void trim(RecordOffset recordOffset, CompletableFuture<Void> cf) {
            if (delayMillis == 0) {
                LOGGER.info("try trim WAL to {}", recordOffset);
                FutureUtil.propagate(deltaWAL.trim(recordOffset), cf);
            } else {
                offsets.add(Pair.of(recordOffset, cf));
                Threads.COMMON_SCHEDULER.schedule(() -> {
                    run();
                }, delayMillis, TimeUnit.MILLISECONDS);
            }
        }

        private void run() {
            Pair<RecordOffset, CompletableFuture<Void>> pair = offsets.poll();
            if (pair == null) {
                return;
            }
            LOGGER.info("try trim WAL to {}", pair.getKey());
            FutureUtil.propagate(deltaWAL.trim(pair.getKey()), pair.getValue());
        }

        public void close() {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            List<Pair<RecordOffset, CompletableFuture<Void>>> pending = new ArrayList<>();
            offsets.drainTo(pending);
            for (Pair<RecordOffset, CompletableFuture<Void>> pair : pending) {
                FutureUtil.propagate(deltaWAL.trim(pair.getKey()), pair.getValue());
                futures.add(pair.getValue());
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
    }

    static StreamRecordBatch toLinkRecord(StreamRecordBatch origin, ByteBuf link) {
        return StreamRecordBatch.of(origin.getStreamId(), origin.getEpoch(), origin.getBaseOffset(), -origin.getCount(), link, ENCODE_LINK_RECORD_INSTANT_ALLOC);
    }

    public static class DeltaWALUploadTaskContext {
        TimerUtil timer;
        LogCache.LogCacheBlock cache;
        UploadWriteAheadLogTask task;
        CompletableFuture<Void> cf;
        CompletableFuture<Void> trimCf = new CompletableFuture<>();
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

    public static class LazyCommit {
        final CompletableFuture<Void> trimCf = new CompletableFuture<>();
        final CompletableFuture<Void> commitCf = new CompletableFuture<>();
        final long lazyLingerMs;
        final boolean awaitTrim;

        public LazyCommit(long lazyLingerMs, boolean awaitTrim) {
            this.lazyLingerMs = lazyLingerMs;
            this.awaitTrim = awaitTrim;
        }
    }
}
