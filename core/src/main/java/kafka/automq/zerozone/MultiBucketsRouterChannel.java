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

package kafka.automq.zerozone;

import com.automq.stream.RecyclingByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

import io.netty.buffer.ByteBuf;

/**
 * Batches router traffic and distributes writes across writable object WAL buckets while preserving
 * completion order for each order hint. The first bucket configures the batch size and interval.
 */
public class MultiBucketsRouterChannel implements RouterChannel {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiBucketsRouterChannel.class);
    private static final ScheduledExecutorService SCHEDULER =
        Threads.newSingleThreadScheduledExecutor("router-batch", true, LOGGER);
    private static final ExecutorService IO_EXECUTOR =
        Executors.newCachedThreadPool(ThreadUtils.createThreadFactory("router-io", true));
    private static final RecyclingByteBufSeqAlloc ALLOC =
        new RecyclingByteBufSeqAlloc(ByteBufAlloc.ROUTER_CHANNEL);
    private static final long CACHE_WEIGHT_UNIT = 100L << 20;
    private static final long HEAP_PER_CACHE_WEIGHT_UNIT = 6L << 30;
    private static final long CACHE_EXPIRE_SECONDS = 10;
    private static final long MIN_BATCH_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long CACHE_MAX_WEIGHT = cacheMaxWeight(Systems.HEAP_MEMORY_SIZE);

    private final List<Runnable> flushListeners = new ArrayList<>();

    private final int nodeId;
    private final boolean readOnly;
    private final List<Channel> allChannels = new ArrayList<>();
    private final List<Channel> writableChannels = new ArrayList<>();
    private volatile Channel writingChannel;
    private final Map<Short, Channel> channelMap = new HashMap<>();
    private final List<Queue<AppendTask>> appendQueues = new ArrayList<>();
    private final Set<Request> outstanding = ConcurrentHashMap.newKeySet();
    private final Cache<ByteBuf, CachedData> cache = CacheBuilder.newBuilder()
        .expireAfterWrite(CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
        .maximumWeight(CACHE_MAX_WEIGHT)
        .weigher((ByteBuf key, CachedData value) -> value.readableBytes())
        .removalListener(notification -> {
            CachedData data = (CachedData) notification.getValue();
            if (data != null) {
                data.release();
            }
        })
        .build();
    private final Queue<Long> channelEpochQueue = new LinkedList<>();
    private final AtomicLong mockOffset = new AtomicLong();
    private final long maxBatchBytes;
    private final long batchIntervalNanos;
    private long lastBatchForceFlushNanos;

    private long channelEpoch = 0L;
    final EventLoop eventLoop;

    private LingerBatch lingerBatch;
    private volatile boolean closed;
    private CompletableFuture<Void> closeFuture;

    /**
     * Creates one object WAL channel per unique bucket. The factory receives whether the WAL is read-only
     * and must return a started WAL. IDs, modes and first-bucket batching options are validated before
     * opening WALs.
     */
    public MultiBucketsRouterChannel(int nodeId, List<BucketURI> buckets, boolean readOnly,
        BiFunction<BucketURI, Boolean, ObjectWALService> factory) {
        this.nodeId = nodeId;
        this.readOnly = readOnly;
        validateBuckets(buckets, readOnly);
        ObjectWALConfig batching = ObjectWALConfig.builder().withURI(buckets.get(0).toIdURI()).build();
        maxBatchBytes = batching.maxBytesInBatch();
        long batchIntervalMillis = batching.batchInterval();
        if (maxBatchBytes <= 0 || batchIntervalMillis <= 0) {
            throw new IllegalArgumentException("Router batching size and interval must be positive");
        }
        batchIntervalNanos = TimeUnit.MILLISECONDS.toNanos(batchIntervalMillis);
        lastBatchForceFlushNanos = System.nanoTime();
        for (int i = 0; i < Systems.CPU_CORES * 64; i++) {
            appendQueues.add(new ConcurrentLinkedQueue<>());
        }
        channelEpochQueue.add(channelEpoch);
        try {
            for (BucketURI bucket : buckets) {
                boolean writing = !readOnly && writable(bucket);
                Channel channel = new Channel(bucket.bucketId(), factory.apply(bucket, !writing));
                allChannels.add(channel);
                channelMap.put(bucket.bucketId(), channel);
                if (writing) {
                    writableChannels.add(channel);
                }
            }
            if (!writableChannels.isEmpty()) {
                writingChannel = writableChannels.get(0);
            }
        } catch (Throwable error) {
            allChannels.forEach(Channel::close);
            throw error;
        }
        eventLoop = new EventLoop("router-channel-" + nodeId, true);
    }

    private static void validateBuckets(List<BucketURI> buckets, boolean readOnly) {
        if (buckets.isEmpty()) {
            throw new IllegalArgumentException("No router channel buckets configured");
        }
        boolean hasWritable = false;
        Set<Short> ids = new HashSet<>();
        for (BucketURI bucket : buckets) {
            boolean writing = writable(bucket);
            if (!ids.add(bucket.bucketId())) {
                throw new IllegalArgumentException("Duplicate router channel bucket ID: " + bucket.bucketId());
            }
            hasWritable |= writing;
        }
        if (!readOnly && !hasWritable) {
            throw new IllegalArgumentException("No writable router channel bucket configured");
        }
    }

    private static boolean writable(BucketURI bucket) {
        String mode = bucket.extensionString("mode", "rw");
        if (!"rw".equalsIgnoreCase(mode) && !"r".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Invalid router channel mode: " + mode);
        }
        return "rw".equalsIgnoreCase(mode);
    }

    @Override
    public CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data) {
        CompletableFuture<AppendResult> cf = new CompletableFuture<>();
        if (closed || readOnly) {
            data.release();
            cf.completeExceptionally(new IllegalStateException("Router channel is closed or read-only"));
            return cf;
        }
        try {
            eventLoop.execute(() -> {
                try {
                    append(targetNodeId, orderHint, data, cf);
                } catch (Throwable error) {
                    cf.completeExceptionally(error);
                }
            });
        } catch (IllegalStateException error) {
            data.release();
            cf.completeExceptionally(error);
        }
        // Callers only attach continuations; this future also tracks completion for close.
        return cf;
    }

    private void append(int targetNodeId, short orderHint, ByteBuf data, CompletableFuture<AppendResult> cf) {
        if (closed) {
            data.release();
            cf.completeExceptionally(new IllegalStateException("Router channel is closed"));
            return;
        }
        long size = (long) data.readableBytes() + StreamRecordBatchCodec.HEADER_SIZE
            + RecordHeader.RECORD_HEADER_SIZE;
        StreamRecordBatch record =
            StreamRecordBatch.of(targetNodeId, 0, mockOffset.incrementAndGet(), 1, data, ALLOC);
        record.retain();
        Request request = new Request(orderHint, targetNodeId, record, size);
        AppendTask task = new AppendTask(request, cf);
        appendQueue(orderHint).offer(task);
        outstanding.add(request);
        if (lingerBatch == null) {
            lingerBatch = new LingerBatch();
        }
        lingerBatch.add(request);
    }

    @Override
    public Handle addFlushListener(Runnable listener) {
        Objects.requireNonNull(listener);
        AtomicBoolean removed = new AtomicBoolean();
        Runnable registration = () -> {
            if (!removed.get()) {
                listener.run();
            }
        };
        eventLoop.execute(() -> flushListeners.add(registration));
        return () -> {
            if (removed.compareAndSet(false, true)) {
                try {
                    eventLoop.execute(() -> flushListeners.remove(registration));
                } catch (IllegalStateException error) {
                    if (!closed) {
                        throw error;
                    }
                }
            }
        };
    }

    private void notifyFlushListeners() {
        for (Runnable listener : flushListeners) {
            try {
                listener.run();
            } catch (Exception error) {
                LOGGER.warn("Router flush listener failed", error);
            }
        }
    }

    void flushBatch() {
        eventLoop.execute(this::flushBatchOnEventLoop);
    }

    private void flushBatchOnEventLoop() {
        if (lingerBatch != null) {
            lingerBatch.flush();
        }
    }

    private void flush(List<Request> requests) {
        if (requests.isEmpty()) {
            return;
        }
        nextChannel();
        Channel channel = writingChannel;
        long batchEpoch = channelEpoch;
        EpochState epochState = channel.channelEpoch2LastRecordOffset.computeIfAbsent(batchEpoch,
            ignored -> new EpochState());
        epochState.inflightAppend.addAndGet(requests.size());
        List<CompletableFuture<com.automq.stream.s3.wal.AppendResult>> appends = new ArrayList<>(requests.size());
        for (Request request : requests) {
            appends.add(channel.append(request.record));
        }
        // Wait only for this router batch, then process all results in one event-loop task.
        CompletableFuture.allOf(appends.toArray(new CompletableFuture<?>[0])).whenComplete((ignored, batchError) -> {
            eventLoop.execute(() -> {
                for (int i = 0; i < requests.size(); i++) {
                    Request request = requests.get(i);
                    appends.get(i).whenComplete((result, error) ->
                        completeAppend(channel, epochState, batchEpoch, request, result, error));
                }
                notifyFlushListeners();
            });
        });
        channel.flush();
    }

    private void completeAppend(Channel channel, EpochState epochState, long batchEpoch, Request request,
        com.automq.stream.s3.wal.AppendResult result, Throwable error) {
        channel.inflightRequests.decrement();
        if (error != null) {
            request.cf.completeExceptionally(error);
        } else {
            RecordOffset offset = result.recordOffset();
            request.cf.complete(new AppendResult(batchEpoch,
                ChannelOffset.of(channel.channelId, request.orderHint, nodeId, request.targetNodeId,
                    offset.buffer()).byteBuf()));
        }
        if (result != null) {
            // Unordered WAL callbacks may leave this below the epoch's maximum offset.
            // This only retains extra objects until a later trim; all epoch appends finish before trimming.
            epochState.lastCallbackRecordOffset = result.recordOffset();
        }
        epochState.inflightAppend.decrementAndGet();
        completeOrderedAppends(request.orderHint);
    }

    private void completeOrderedAppends(short orderHint) {
        Queue<AppendTask> queue = appendQueue(orderHint);
        for (;;) {
            AppendTask task = queue.peek();
            if (task == null || !task.channelAppendCf.isDone()) {
                break;
            }
            queue.poll();
            task.complete();
            outstanding.remove(task.request);
        }
    }

    @Override
    public CompletableFuture<ByteBuf> get(ByteBuf offset) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Router channel is closed"));
        }
        CachedData cachedData = cache.getIfPresent(offset);
        if (cachedData != null) {
            ByteBuf data = cachedData.take();
            if (data != null) {
                cache.invalidate(offset);
                return CompletableFuture.completedFuture(data);
            }
        }
        Channel channel = channelMap.get(ChannelOffset.of(offset).channelId());
        if (channel == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown router channel bucket ID"));
        }
        return channel.get(DefaultRecordOffset.of(ChannelOffset.of(offset).walRecordOffset()))
            .thenApply(record -> {
                ByteBuf payload = record.getPayload().retainedSlice();
                record.release();
                return payload;
            });
    }

    @Override
    public void nextEpoch(long nextEpoch) {
        eventLoop.execute(() -> {
            if (nextEpoch > channelEpoch) {
                channelEpoch = nextEpoch;
                channelEpochQueue.add(nextEpoch);
            }
        });
    }

    @Override
    public void trim(long epoch) {
        eventLoop.execute(() -> {
            Map<Channel, RecordOffset> trimOffsets = new HashMap<>();
            L1:
            for (;;) {
                Long candidateEpoch = channelEpochQueue.peek();
                if (candidateEpoch == null || candidateEpoch > epoch) {
                    break;
                }
                for (Channel channel : allChannels) {
                    EpochState state = channel.channelEpoch2LastRecordOffset.get(candidateEpoch);
                    if (state != null && state.inflightAppend.get() != 0) {
                        break L1;
                    }
                }
                channelEpochQueue.poll();
                for (Channel channel : allChannels) {
                    EpochState state = channel.channelEpoch2LastRecordOffset.remove(candidateEpoch);
                    if (state != null && state.lastCallbackRecordOffset != null) {
                        trimOffsets.put(channel, state.lastCallbackRecordOffset);
                    }
                }
            }
            trimOffsets.forEach(Channel::trim);
        });
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        if (closeFuture == null) {
            closeFuture = new CompletableFuture<>();
            eventLoop.execute(() -> {
                closed = true;
                cache.invalidateAll();
                cache.cleanUp();
                flushBatchOnEventLoop();
                CompletableFuture<Void> drained = CompletableFuture.allOf(
                    outstanding.stream().map(request -> request.task.finalOrderedCf).toArray(CompletableFuture[]::new))
                    .handle((ignored, error) -> null);
                drained.thenCompose(ignored -> CompletableFuture.allOf(
                    allChannels.stream().map(Channel::close).toArray(CompletableFuture[]::new)))
                    .whenComplete((ignored, error) -> eventLoop.execute(() ->
                        eventLoop.shutdownGracefully().whenComplete((nil, shutdownError) -> {
                            if (error != null) {
                                closeFuture.completeExceptionally(error);
                            } else if (shutdownError != null) {
                                closeFuture.completeExceptionally(shutdownError);
                            } else {
                                closeFuture.complete(null);
                            }
                        })));
            });
        }
        return closeFuture;
    }

    private void nextChannel() {
        if (writableChannels.size() == 1) {
            writingChannel = writableChannels.get(0);
            return;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int first = random.nextInt(writableChannels.size());
        int second = random.nextInt(writableChannels.size() - 1);
        if (second >= first) {
            second++;
        }
        Channel a = writableChannels.get(first);
        Channel b = writableChannels.get(second);
        writingChannel = a.inflightRequests.sum() <= b.inflightRequests.sum() ? a : b;
    }

    private Queue<AppendTask> appendQueue(short orderHint) {
        return appendQueues.get(Math.abs(orderHint % appendQueues.size()));
    }

    private static long cacheMaxWeight(long heapMemorySize) {
        long units = heapMemorySize / HEAP_PER_CACHE_WEIGHT_UNIT;
        if (heapMemorySize % HEAP_PER_CACHE_WEIGHT_UNIT != 0) {
            units++;
        }
        return Math.max(units, 1) * CACHE_WEIGHT_UNIT;
    }

    static long calculateBatchFlushDelayNanos(long lastForceFlushNanos, long batchStartNanos,
        long batchIntervalNanos) {
        long minIntervalNanos = Math.min(MIN_BATCH_INTERVAL_NANOS, batchIntervalNanos);
        return Math.min(Math.max(minIntervalNanos,
            lastForceFlushNanos + batchIntervalNanos - batchStartNanos), batchIntervalNanos);
    }

    private class LingerBatch {
        final List<Request> requests = new ArrayList<>();
        final ScheduledFuture<?> timer;
        long size;

        LingerBatch() {
            long startNanos = System.nanoTime();
            long delayNanos = calculateBatchFlushDelayNanos(
                lastBatchForceFlushNanos, startNanos, batchIntervalNanos);
            // Keep the planned schedule even if a size-triggered flush cancels this timer.
            lastBatchForceFlushNanos = startNanos + delayNanos;
            timer = SCHEDULER.schedule(() -> {
                try {
                    eventLoop.execute(() -> {
                        if (lingerBatch == this) {
                            flush();
                        }
                    });
                } catch (IllegalStateException error) {
                    // A timer already running when close cancels it may race with loop shutdown.
                    if (!closed) {
                        throw error;
                    }
                }
            }, delayNanos, TimeUnit.NANOSECONDS);
        }

        void add(Request request) {
            requests.add(request);
            size += request.size;
            if (size > maxBatchBytes) {
                flush();
            }
        }

        List<Request> take() {
            timer.cancel(false);
            lingerBatch = null;
            return requests;
        }

        void flush() {
            MultiBucketsRouterChannel.this.flush(take());
        }
    }

    private static class Channel {
        final short channelId;
        final ObjectWALService wal;
        final Map<Long, EpochState> channelEpoch2LastRecordOffset = new ConcurrentHashMap<>();
        final LongAdder inflightRequests = new LongAdder();

        Channel(short channelId, ObjectWALService wal) {
            this.channelId = channelId;
            this.wal = wal;
        }

        CompletableFuture<com.automq.stream.s3.wal.AppendResult> append(StreamRecordBatch record) {
            inflightRequests.increment();
            try {
                return wal.append(TraceContext.DEFAULT, record);
            } catch (OverCapacityException error) {
                return CompletableFuture.failedFuture(error);
            }
        }

        CompletableFuture<StreamRecordBatch> get(RecordOffset offset) {
            return wal.get(offset);
        }

        CompletableFuture<Void> trim(RecordOffset offset) {
            return wal.trim(offset);
        }

        CompletableFuture<Void> flush() {
            return wal.flush();
        }

        CompletableFuture<Void> close() {
            return CompletableFuture.runAsync(wal::shutdownGracefully, IO_EXECUTOR);
        }
    }

    private static class EpochState {
        final AtomicLong inflightAppend = new AtomicLong();
        RecordOffset lastCallbackRecordOffset;
    }

    private class AppendTask {
        final Request request;
        final CompletableFuture<AppendResult> channelAppendCf;
        final CompletableFuture<AppendResult> finalOrderedCf;

        AppendTask(Request request, CompletableFuture<AppendResult> cf) {
            this.finalOrderedCf = cf;
            this.request = request;
            this.channelAppendCf = request.cf;
            request.task = this;
        }

        void complete() {
            channelAppendCf.whenComplete((result, error) -> {
                try {
                    if (result != null && !closed && request.targetNodeId != nodeId) {
                        cache.put(result.channelOffset().slice(), new CachedData(request.record.getPayload()));
                    } else {
                        request.record.release();
                    }
                } catch (Throwable cacheError) {
                    finalOrderedCf.completeExceptionally(cacheError);
                    return;
                }
                if (error == null) {
                    finalOrderedCf.complete(result);
                } else {
                    finalOrderedCf.completeExceptionally(error);
                }
            });
        }
    }

    private static class Request {
        final short orderHint;
        final int targetNodeId;
        final StreamRecordBatch record;
        final long size;
        final CompletableFuture<AppendResult> cf = new CompletableFuture<>();
        AppendTask task;

        Request(short orderHint, int targetNodeId, StreamRecordBatch record, long size) {
            this.orderHint = orderHint;
            this.targetNodeId = targetNodeId;
            this.record = record;
            this.size = size;
        }
    }

    private static class CachedData {
        private final ByteBuf data;
        private final int readableBytes;
        private boolean ownedByCache = true;

        CachedData(ByteBuf data) {
            this.data = data;
            this.readableBytes = data.readableBytes();
        }

        synchronized ByteBuf take() {
            if (!ownedByCache) {
                return null;
            }
            ownedByCache = false;
            return data;
        }

        synchronized void release() {
            if (ownedByCache) {
                ownedByCache = false;
                data.release();
            }
        }

        int readableBytes() {
            return readableBytes;
        }
    }
}
