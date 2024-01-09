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
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.api.exceptions.ErrorCode;
import com.automq.stream.api.exceptions.FastReadFailFastException;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StreamOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.GlobalSwitch;
import io.netty.buffer.Unpooled;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.utils.FutureUtil.exec;
import static com.automq.stream.utils.FutureUtil.propagate;

public class S3Stream implements Stream {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Stream.class);
    final AtomicLong confirmOffset;
    private final String logIdent;
    private final long streamId;
    private final long epoch;
    private final AtomicLong nextOffset;
    private final Storage storage;
    private final StreamManager streamManager;
    private final Status status;
    private final Consumer<Long> closeHook;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantLock appendLock = new ReentrantLock();
    private final Set<CompletableFuture<?>> pendingAppends = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<?>> pendingFetches = ConcurrentHashMap.newKeySet();
    private final AsyncNetworkBandwidthLimiter networkInboundLimiter;
    private final AsyncNetworkBandwidthLimiter networkOutboundLimiter;
    private long startOffset;
    private CompletableFuture<Void> lastPendingTrim = CompletableFuture.completedFuture(null);

    public S3Stream(long streamId, long epoch, long startOffset, long nextOffset, Storage storage,
        StreamManager streamManager, Consumer<Long> closeHook) {
        this(streamId, epoch, startOffset, nextOffset, storage, streamManager, closeHook, null, null);
    }

    public S3Stream(long streamId, long epoch, long startOffset, long nextOffset, Storage storage,
        StreamManager streamManager, Consumer<Long> closeHook,
        AsyncNetworkBandwidthLimiter networkInboundLimiter, AsyncNetworkBandwidthLimiter networkOutboundLimiter) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.logIdent = "[Stream id=" + streamId + " epoch=" + epoch + "]";
        this.nextOffset = new AtomicLong(nextOffset);
        this.confirmOffset = new AtomicLong(nextOffset);
        this.status = new Status();
        this.storage = storage;
        this.streamManager = streamManager;
        this.closeHook = closeHook;
        this.networkInboundLimiter = networkInboundLimiter;
        this.networkOutboundLimiter = networkOutboundLimiter;
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
    @WithSpan
    public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
        TimerUtil timerUtil = new TimerUtil();
        readLock.lock();
        try {
            CompletableFuture<AppendResult> cf = exec(() -> {
                if (networkInboundLimiter != null) {
                    networkInboundLimiter.forceConsume(recordBatch.rawPayload().remaining());
                }
                appendLock.lock();
                try {
                    return append0(context, recordBatch);
                } finally {
                    appendLock.unlock();
                }
            }, LOGGER, "append");
            pendingAppends.add(cf);
            cf.whenComplete((nil, ex) -> {
                StreamOperationStats.getInstance().appendStreamStats.record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                pendingAppends.remove(cf);
            });
            return cf;
        } finally {
            readLock.unlock();
        }
    }

    @WithSpan
    private CompletableFuture<AppendResult> append0(AppendContext context, RecordBatch recordBatch) {
        if (!status.isWritable()) {
            return FutureUtil.failedFuture(new StreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is not writable"));
        }
        long offset = nextOffset.getAndAdd(recordBatch.count());
        StreamRecordBatch streamRecordBatch = new StreamRecordBatch(streamId, epoch, offset, recordBatch.count(), Unpooled.wrappedBuffer(recordBatch.rawPayload()));
        CompletableFuture<AppendResult> cf = storage.append(context, streamRecordBatch).thenApply(nil -> {
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
    @WithSpan
    public CompletableFuture<FetchResult> fetch(FetchContext context,
        @SpanAttribute long startOffset,
        @SpanAttribute long endOffset,
        @SpanAttribute int maxBytes) {
        TimerUtil timerUtil = new TimerUtil();
        readLock.lock();
        try {
            CompletableFuture<FetchResult> cf = exec(() -> fetch0(context, startOffset, endOffset, maxBytes), LOGGER, "fetch");
            pendingFetches.add(cf);
            cf.whenComplete((rs, ex) -> {
                StreamOperationStats.getInstance().fetchStreamStats.record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                if (ex != null) {
                    Throwable cause = FutureUtil.cause(ex);
                    if (!(cause instanceof FastReadFailFastException)) {
                        LOGGER.error("{} stream fetch [{}, {}) {} fail", logIdent, startOffset, endOffset, maxBytes, ex);
                    }
                } else if (networkOutboundLimiter != null) {
                    long totalSize = rs.recordBatchList().stream().mapToLong(record -> record.rawPayload().remaining()).sum();
                    networkOutboundLimiter.forceConsume(totalSize);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("[S3BlockCache] fetch data, stream={}, {}-{}, total bytes: {}, cost={}ms", streamId,
                            startOffset, endOffset, totalSize, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                    }
                }
                pendingFetches.remove(cf);
            });
            return cf;
        } finally {
            readLock.unlock();
        }
    }

    @WithSpan
    private CompletableFuture<FetchResult> fetch0(FetchContext context, long startOffset, long endOffset,
        int maxBytes) {
        if (!status.isReadable()) {
            return FutureUtil.failedFuture(new StreamClientException(ErrorCode.STREAM_ALREADY_CLOSED, logIdent + " stream is already closed"));
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} stream try fetch, startOffset: {}, endOffset: {}, maxBytes: {}", logIdent, startOffset, endOffset, maxBytes);
        }
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
            return CompletableFuture.completedFuture(new DefaultFetchResult(Collections.emptyList(), CacheAccessType.DELTA_WAL_CACHE_HIT, false));
        }
        return storage.read(context, streamId, startOffset, endOffset, maxBytes).thenApply(dataBlock -> {
            List<StreamRecordBatch> records = dataBlock.getRecords();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} stream fetch, startOffset: {}, endOffset: {}, maxBytes: {}, records: {}", logIdent, startOffset, endOffset, maxBytes, records.size());
            }
            return new DefaultFetchResult(records, dataBlock.getCacheAccessType(), context.readOptions().pooledBuf());
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
                    StreamOperationStats.getInstance().trimStreamStats.record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
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
        TimerUtil timerUtil = new TimerUtil();
        writeLock.lock();
        try {
            status.markClosed();

            // await all pending append/fetch/trim request
            List<CompletableFuture<?>> pendingRequests = new ArrayList<>(pendingAppends);
            if (GlobalSwitch.STRICT) {
                pendingRequests.addAll(pendingFetches);
            }
            pendingRequests.add(lastPendingTrim);
            CompletableFuture<Void> awaitPendingRequestsCf = CompletableFuture.allOf(pendingRequests.toArray(new CompletableFuture[0]));
            CompletableFuture<Void> closeCf = new CompletableFuture<>();

            awaitPendingRequestsCf.whenComplete((nil, ex) -> propagate(exec(this::close0, LOGGER, "close"), closeCf));

            closeCf.whenComplete((nil, ex) -> {
                if (ex != null) {
                    LOGGER.error("{} close fail", logIdent, ex);
                    StreamOperationStats.getInstance().closeStreamStats(false).record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                } else {
                    LOGGER.info("{} closed", logIdent);
                    StreamOperationStats.getInstance().closeStreamStats(true).record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                }
            });

            return closeCf;
        } finally {
            writeLock.unlock();
        }
    }

    private CompletableFuture<Void> close0() {
        return storage.forceUpload(streamId)
            .thenCompose(nil -> streamManager.closeStream(streamId, epoch))
            .whenComplete((nil, ex) -> closeHook.accept(streamId));
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
        closeHook.accept(streamId);
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
        private static final LongAdder INFLIGHT = new LongAdder();
        private final List<StreamRecordBatch> pooledRecords;
        private final List<RecordBatchWithContext> records;
        private final CacheAccessType cacheAccessType;
        private final boolean pooledBuf;
        private volatile boolean freed = false;

        public DefaultFetchResult(List<StreamRecordBatch> streamRecords, CacheAccessType cacheAccessType,
            boolean pooledBuf) {
            this.pooledRecords = streamRecords;
            this.pooledBuf = pooledBuf;
            this.records = streamRecords.stream().map(r -> new RecordBatchWithContextWrapper(covert(r, pooledBuf), r.getBaseOffset())).collect(Collectors.toList());
            this.cacheAccessType = cacheAccessType;
            if (!pooledBuf) {
                streamRecords.forEach(StreamRecordBatch::release);
            } else {
                INFLIGHT.increment();
            }
        }

        private static RecordBatch covert(StreamRecordBatch streamRecordBatch, boolean pooledBuf) {
            ByteBuffer buf;
            if (pooledBuf) {
                buf = streamRecordBatch.getPayload().nioBuffer();
            } else {
                buf = ByteBuffer.allocate(streamRecordBatch.size());
                streamRecordBatch.getPayload().duplicate().readBytes(buf);
                buf.flip();
            }
            return new RecordBatch() {
                @Override
                public int count() {
                    return streamRecordBatch.getCount();
                }

                @Override
                public long baseTimestamp() {
                    return streamRecordBatch.getEpoch();
                }

                @Override
                public Map<String, String> properties() {
                    return Collections.emptyMap();
                }

                @Override
                public ByteBuffer rawPayload() {
                    return buf;
                }
            };
        }

        @Override
        public List<RecordBatchWithContext> recordBatchList() {
            return records;
        }

        @Override
        public CacheAccessType getCacheAccessType() {
            return cacheAccessType;
        }

        @Override
        public void free() {
            if (!freed && pooledBuf) {
                pooledRecords.forEach(StreamRecordBatch::release);
                INFLIGHT.decrement();
            }
            freed = true;
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
