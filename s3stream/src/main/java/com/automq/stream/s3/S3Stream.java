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

package com.automq.stream.s3;

import com.automq.stream.DefaultAppendResult;
import com.automq.stream.RecordBatchWithContextWrapper;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.ErrorCode;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClientException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.utils.FutureUtil.exec;
import static com.automq.stream.utils.FutureUtil.propagate;

public class S3Stream implements Stream {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Stream.class);
    private final String logIdent;
    private final long streamId;
    private final long epoch;
    private long startOffset;
    final AtomicLong confirmOffset;
    private final AtomicLong nextOffset;
    private final Storage storage;
    private final StreamManager streamManager;
    private final Status status;
    private final Function<Long, Void> closeHook;
    private final StreamObjectsCompactionTask streamObjectsCompactionTask;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final Set<CompletableFuture<?>> pendingAppends = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<?>> pendingFetches = ConcurrentHashMap.newKeySet();
    private final AsyncNetworkBandwidthLimiter networkInboundLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundLimiter;
    private CompletableFuture<Void> lastPendingTrim = CompletableFuture.completedFuture(null);

    public S3Stream(long streamId, long epoch, long startOffset, long nextOffset, Storage storage, StreamManager streamManager,
                    StreamObjectsCompactionTask.Builder compactionTaskBuilder, Function<Long, Void> closeHook) {
        this(streamId, epoch, startOffset, nextOffset, storage, streamManager, compactionTaskBuilder, closeHook, null, null);
    }

    public S3Stream(long streamId, long epoch, long startOffset, long nextOffset, Storage storage, StreamManager streamManager,
                    StreamObjectsCompactionTask.Builder compactionTaskBuilder, Function<Long, Void> closeHook,
                    AsyncNetworkBandwidthLimiter networkInboundLimiter, AsyncNetworkBandwidthLimiter networkOutboundLimiter) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.logIdent = "[Stream id=" + streamId + " epoch" + epoch + "]";
        this.nextOffset = new AtomicLong(nextOffset);
        this.confirmOffset = new AtomicLong(nextOffset);
        this.status = new Status();
        this.storage = storage;
        this.streamManager = streamManager;
        this.streamObjectsCompactionTask = compactionTaskBuilder.withStream(this).build();
        this.closeHook = closeHook;
        this.networkInboundLimiter = networkInboundLimiter;
        this.networkOutboundLimiter = networkOutboundLimiter;
    }

    public StreamObjectsCompactionTask.CompactionSummary triggerCompactionTask() throws ExecutionException, InterruptedException {
        streamObjectsCompactionTask.prepare();
        streamObjectsCompactionTask.doCompactions().get();
        return streamObjectsCompactionTask.getCompactionsSummary();
    }

    public boolean isClosed() {
        return status.isClosed();
    }

    @Override
    public long streamId() {
        return this.streamId;
    }

    @Override
    public long startOffset() {
        return this.startOffset;
    }

    @Override
    public long confirmOffset() {
        return this.confirmOffset.get();
    }

    @Override
    public long nextOffset() {
        return nextOffset.get();
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        writeLock.lock();
        try {
            TimerUtil timerUtil = new TimerUtil();
            CompletableFuture<AppendResult> cf = exec(() -> {
                if (networkInboundLimiter != null) {
                    networkInboundLimiter.forceConsume(recordBatch.rawPayload().remaining());
                }
                return append0(recordBatch);
            }, LOGGER, "append");
            pendingAppends.add(cf);
            cf.whenComplete((nil, ex) -> {
                OperationMetricsStats.getHistogram(S3Operation.APPEND_STREAM).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                pendingAppends.remove(cf);
            });
            return cf;
        } finally {
            writeLock.unlock();
        }
    }

    private CompletableFuture<AppendResult> append0(RecordBatch recordBatch) {
        if (!status.isWritable()) {
            return FutureUtil.failedFuture(new StreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is not writable"));
        }
        long offset = nextOffset.getAndAdd(recordBatch.count());
        StreamRecordBatch streamRecordBatch = new StreamRecordBatch(streamId, epoch, offset, recordBatch.count(), Unpooled.wrappedBuffer(recordBatch.rawPayload()));
        CompletableFuture<AppendResult> cf = storage.append(streamRecordBatch).thenApply(nil -> {
            updateConfirmOffset(offset + recordBatch.count());
            return new DefaultAppendResult(offset);
        });
        return cf.whenComplete((rst, ex) -> {
            if (ex == null) {
                return;
            }
            // Wal should keep retry append until stream is fenced or wal is closed.
            status.markFenced();
            if (ex instanceof StreamClientException && ((StreamClientException) ex).getCode() == ErrorCode.EXPIRED_STREAM_EPOCH) {
                LOGGER.info("{} stream append, stream is fenced", logIdent);
            } else {
                LOGGER.warn("{} stream append fail", logIdent, ex);
            }
        });
    }

    @Override
    public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytes) {
        readLock.lock();
        try {
            TimerUtil timerUtil = new TimerUtil();
            CompletableFuture<FetchResult> cf = exec(() -> fetch0(startOffset, endOffset, maxBytes), LOGGER, "fetch");
            pendingFetches.add(cf);
            cf.whenComplete((rs, ex) -> {
                OperationMetricsStats.getHistogram(S3Operation.FETCH_STREAM).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));

                if (timerUtil.elapsedAs(TimeUnit.MILLISECONDS) > 1000) {
                    LOGGER.warn("{} stream fetch [{}, {}) {} takes {} ms", logIdent, startOffset, endOffset, maxBytes, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                }

                if (ex != null) {
                    LOGGER.error("{} stream fetch [{}, {}) {} fail", logIdent, startOffset, endOffset, maxBytes, ex);
                } else if (networkOutboundLimiter != null) {
                    long totalSize = rs.recordBatchList().stream().mapToLong(record -> record.rawPayload().remaining()).sum();
                    networkOutboundLimiter.forceConsume(totalSize);
                }
                pendingFetches.remove(cf);
            });
            return cf;
        } finally {
            readLock.unlock();
        }
    }

    private CompletableFuture<FetchResult> fetch0(long startOffset, long endOffset, int maxBytes) {
        if (!status.isReadable()) {
            return FutureUtil.failedFuture(new StreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is already closed"));
        }
        LOGGER.trace("{} stream try fetch, startOffset: {}, endOffset: {}, maxBytes: {}", logIdent, startOffset, endOffset, maxBytes);
        long confirmOffset = this.confirmOffset.get();
        if (startOffset < startOffset() || endOffset > confirmOffset) {
            return FutureUtil.failedFuture(
                    new StreamClientException(
                            ErrorCode.OFFSET_OUT_OF_RANGE_BOUNDS,
                            String.format("fetch range[%s, %s) is out of stream bound [%s, %s)", startOffset, endOffset, startOffset(), confirmOffset)
                    ));
        }
        if (startOffset > endOffset) {
            return FutureUtil.failedFuture(new IllegalArgumentException(String.format("fetch startOffset %s is greater than endOffset %s", startOffset, endOffset)));
        }
        if (startOffset == endOffset) {
            return CompletableFuture.completedFuture(new DefaultFetchResult(Collections.emptyList(), CacheAccessType.DELTA_WAL_CACHE_HIT));
        }
        return storage.read(streamId, startOffset, endOffset, maxBytes).thenApply(dataBlock -> {
            List<StreamRecordBatch> records = dataBlock.getRecords();
            LOGGER.trace("{} stream fetch, startOffset: {}, endOffset: {}, maxBytes: {}, records: {}", logIdent, startOffset, endOffset, maxBytes, records.size());
            return new DefaultFetchResult(records, dataBlock.getCacheAccessType());
        });
    }

    @Override
    public CompletableFuture<Void> trim(long newStartOffset) {
        writeLock.lock();
        try {
            TimerUtil timerUtil = new TimerUtil();
            return exec(() -> {
                CompletableFuture<Void> cf = new CompletableFuture<>();
                lastPendingTrim.whenComplete((nil, ex) -> propagate(trim0(newStartOffset), cf));
                this.lastPendingTrim = cf;
                cf.whenComplete((nil, ex) -> {
                    OperationMetricsStats.getHistogram(S3Operation.TRIM_STREAM).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                });
                return cf;
            }, LOGGER, "trim");
        } finally {
            writeLock.unlock();
        }
    }

    private CompletableFuture<Void> trim0(long newStartOffset) {
        if (newStartOffset < this.startOffset) {
            LOGGER.warn("{} trim newStartOffset[{}] less than current start offset[{}]", logIdent, newStartOffset, startOffset);
            return CompletableFuture.completedFuture(null);
        }
        this.startOffset = newStartOffset;
        CompletableFuture<Void> trimCf = new CompletableFuture<>();
        // await all pending fetches complete to avoid trim offset intersect with fetches.
        CompletableFuture<Void> awaitPendingFetchesCf = CompletableFuture.allOf(pendingFetches.toArray(new CompletableFuture[0]));
        awaitPendingFetchesCf.whenComplete((nil, ex) -> propagate(streamManager.trimStream(streamId, epoch, newStartOffset), trimCf));
        trimCf.whenComplete((nil, ex) -> {
            if (ex != null) {
                LOGGER.error("{} trim fail", logIdent, ex);
            } else {
                LOGGER.info("{} trim to {}", logIdent, newStartOffset);
            }
        });
        return trimCf;
    }

    @Override
    public CompletableFuture<Void> close() {
        writeLock.lock();
        try {
            status.markClosed();

            // await all pending append/fetch/trim request
            List<CompletableFuture<?>> pendingRequests = new ArrayList<>(pendingAppends);
            pendingRequests.addAll(pendingFetches);
            pendingRequests.add(lastPendingTrim);
            CompletableFuture<Void> awaitPendingRequestsCf = CompletableFuture.allOf(pendingRequests.toArray(new CompletableFuture[0]));
            CompletableFuture<Void> closeCf = new CompletableFuture<>();

            awaitPendingRequestsCf.whenComplete((nil, ex) -> propagate(exec(this::close0, LOGGER, "close"), closeCf));

            closeCf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    LOGGER.error("{} close fail", logIdent, ex);
                } else {
                    LOGGER.info("{} closed", logIdent);
                }
            });

            return closeCf;
        } finally {
            writeLock.unlock();
        }
    }

    private CompletableFuture<Void> close0() {
        streamObjectsCompactionTask.close();
        return storage.forceUpload(streamId)
                .thenCompose(nil -> streamManager.closeStream(streamId, epoch))
                .whenComplete((nil, ex) -> closeHook.apply(streamId));
    }

    @Override
    public CompletableFuture<Void> destroy() {
        writeLock.lock();
        try {
            CompletableFuture<Void> destroyCf = close().thenCompose(nil -> exec(this::destroy0, LOGGER, "destroy"));
            destroyCf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    LOGGER.error("{} destroy fail", logIdent, ex);
                } else {
                    LOGGER.info("{} destroyed", logIdent);
                }
            });
            return destroyCf;
        } finally {
            writeLock.unlock();
        }
    }

    private CompletableFuture<Void> destroy0() {
        status.markDestroy();
        streamObjectsCompactionTask.close();
        closeHook.apply(streamId);
        startOffset = this.confirmOffset.get();
        return streamManager.deleteStream(streamId, epoch);
    }

    private void updateConfirmOffset(long newOffset) {
        for (; ; ) {
            long oldConfirmOffset = confirmOffset.get();
            if (oldConfirmOffset >= newOffset) {
                break;
            }
            if (confirmOffset.compareAndSet(oldConfirmOffset, newOffset)) {
                LOGGER.trace("{} stream update confirm offset from {} to {}", logIdent, oldConfirmOffset, newOffset);
                break;
            }
        }
    }

    static class DefaultFetchResult implements FetchResult {
        private final List<RecordBatchWithContext> records;
        private final CacheAccessType cacheAccessType;

        public DefaultFetchResult(List<StreamRecordBatch> streamRecords, CacheAccessType cacheAccessType) {
            this.records = streamRecords.stream().map(r -> new RecordBatchWithContextWrapper(r.getRecordBatch(), r.getBaseOffset())).collect(Collectors.toList());
            this.cacheAccessType = cacheAccessType;
            streamRecords.forEach(StreamRecordBatch::release);
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return records;
        }

        @Override
        public CacheAccessType getCacheAccessType() {
            return cacheAccessType;
        }

    }

    static class Status {
        private static final int CLOSED_MARK = 1;
        private static final int FENCED_MARK = 1 << 1;
        private static final int DESTROY_MARK = 1 << 2;
        private final AtomicInteger status = new AtomicInteger();

        public void markFenced() {
            status.getAndUpdate(operand -> operand | FENCED_MARK);
        }

        public void markClosed() {
            status.getAndUpdate(operand -> operand | CLOSED_MARK);
        }

        public void markDestroy() {
            status.getAndUpdate(operand -> operand | DESTROY_MARK);
        }

        public boolean isClosed() {
            return (status.get() & CLOSED_MARK) != 0;
        }

        public boolean isWritable() {
            return status.get() == 0;
        }

        public boolean isReadable() {
            return status.get() == 0;
        }
    }
}
