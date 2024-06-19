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

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.compact.StreamObjectCompactor;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StreamOperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR;

public class S3StreamClient implements StreamClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamClient.class);
    private static final long STREAM_OBJECT_COMPACTION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
    private final ScheduledExecutorService streamObjectCompactionScheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("stream-object-compaction-scheduler", true), LOGGER, true);
    final Map<Long, StreamWrapper> openedStreams;
    private final StreamManager streamManager;
    private final Storage storage;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final Config config;
    private final AsyncNetworkBandwidthLimiter networkInboundBucket;
    private final AsyncNetworkBandwidthLimiter networkOutboundBucket;
    private ScheduledFuture<?> scheduledCompactionTaskFuture;

    private final ReentrantLock lock = new ReentrantLock();

    final Map<Long, CompletableFuture<Stream>> openingStreams = new ConcurrentHashMap<>();
    final Map<Long, CompletableFuture<Stream>> closingStreams = new ConcurrentHashMap<>();

    private boolean closed;

    @SuppressWarnings("unused")
    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager,
        S3Operator s3Operator, Config config) {
        this(streamManager, storage, objectManager, s3Operator, config, null, null);
    }

    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager,
        S3Operator s3Operator, Config config,
        AsyncNetworkBandwidthLimiter networkInboundBucket, AsyncNetworkBandwidthLimiter networkOutboundBucket) {
        this.streamManager = streamManager;
        this.storage = storage;
        this.openedStreams = new ConcurrentHashMap<>();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.config = config;
        this.networkInboundBucket = networkInboundBucket;
        this.networkOutboundBucket = networkOutboundBucket;
        startStreamObjectsCompactions();
    }

    @Override
    public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
        return runInLock(() -> {
            checkState();
            TimerUtil timerUtil = new TimerUtil();
            return FutureUtil.exec(() -> streamManager.createStream(options.tags()).thenCompose(streamId -> {
                StreamOperationStats.getInstance().createStreamLatency.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                return openStream0(streamId, options.epoch(), options.tags());
            }), LOGGER, "createAndOpenStream");
        });
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return runInLock(() -> {
            checkState();
            return FutureUtil.exec(() -> openStream0(streamId, openStreamOptions.epoch(), openStreamOptions.tags()), LOGGER, "openStream");
        });
    }

    @Override
    public Optional<Stream> getStream(long streamId) {
        return runInLock(() -> {
            checkState();
            return Optional.ofNullable(openedStreams.get(streamId));
        });
    }

    /**
     * Start stream objects compactions.
     */
    private void startStreamObjectsCompactions() {
        scheduledCompactionTaskFuture = streamObjectCompactionScheduler.scheduleWithFixedDelay(() -> {
            List<StreamWrapper> operationStreams = new ArrayList<>(openedStreams.values());
            operationStreams.forEach(StreamWrapper::compact);
        }, 5, 5, TimeUnit.MINUTES);
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch, Map<String, String> tags) {
        return runInLock(() -> {
            TimerUtil timerUtil = new TimerUtil();
            CompletableFuture<Stream> cf = streamManager.openStream(streamId, epoch, tags).
                thenApply(metadata -> {
                    StreamWrapper stream = new StreamWrapper(newStream(metadata));
                    runInLock(() -> openedStreams.put(streamId, stream));
                    StreamOperationStats.getInstance().openStreamLatency.record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    return stream;
                });
            openingStreams.put(streamId, cf);
            cf.whenComplete((stream, ex) -> runInLock(() -> openingStreams.remove(streamId, cf)));
            return cf;
        });
    }

    S3Stream newStream(StreamMetadata metadata) {
        return new S3Stream(
            metadata.streamId(), metadata.epoch(),
            metadata.startOffset(), metadata.endOffset(),
            storage, streamManager, networkInboundBucket, networkOutboundBucket);
    }

    @Override
    public void shutdown() {
        LOGGER.info("S3StreamClient start shutting down");
        markClosed();
        // cancel the submitted task if not started; do not interrupt the task if it is running.
        if (scheduledCompactionTaskFuture != null) {
            scheduledCompactionTaskFuture.cancel(false);
        }
        streamObjectCompactionScheduler.shutdown();
        try {
            if (!streamObjectCompactionScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn("await streamObjectCompactionExecutor timeout 10s");
                streamObjectCompactionScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            streamObjectCompactionScheduler.shutdownNow();
            LOGGER.warn("await streamObjectCompactionExecutor close fail", e);
        }

        TimerUtil timerUtil = new TimerUtil();
        for (; ; ) {
            lock.lock();
            try {
                openedStreams.forEach((streamId, stream) -> {
                    LOGGER.info("trigger stream force close, streamId={}", streamId);
                    stream.close();
                });
                if (openedStreams.isEmpty() && openingStreams.isEmpty() && closingStreams.isEmpty()) {
                    LOGGER.info("all streams are closed");
                    break;
                }
                LOGGER.info("waiting streams close, opened[{}], opening[{}], closing[{}]", openedStreams.keySet(), openingStreams.keySet(), closingStreams.keySet());
            } finally {
                lock.unlock();
            }
            Threads.sleep(1000);
        }
        LOGGER.info("S3StreamClient shutdown, cost {}ms", timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
    }

    private void checkState() {
        if (closed) {
            throw new IllegalStateException("S3StreamClient is already closed");
        }
    }

    private void markClosed() {
        runInLock(() -> closed = true);
    }

    private void runInLock(Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private <T> T runInLock(Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    public class StreamWrapper implements Stream {
        private final S3Stream stream;
        private final Semaphore trimCompactionSemaphore = new Semaphore(1);
        private volatile long lastCompactionTimestamp = 0;
        private volatile long lastMajorCompactionTimestamp = System.currentTimeMillis();

        public StreamWrapper(S3Stream stream) {
            this.stream = stream;
        }

        @Override
        public long streamId() {
            return stream.streamId();
        }

        @Override
        public long streamEpoch() {
            return stream.streamEpoch();
        }

        @Override
        public long startOffset() {
            return stream.startOffset();
        }

        @Override
        public long confirmOffset() {
            return stream.confirmOffset();
        }

        @Override
        public long nextOffset() {
            return stream.nextOffset();
        }

        @Override
        public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
            return stream.append(context, recordBatch);
        }

        @Override
        public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset,
            int maxBytesHint) {
            return stream.fetch(context, startOffset, endOffset, maxBytesHint);
        }

        @Override
        public CompletableFuture<Void> trim(long newStartOffset) {
            return stream.trim(newStartOffset).whenComplete((nil, ex) -> {
                if (!trimCompactionSemaphore.tryAcquire()) {
                    // ensure only one compaction task which trim triggers
                    return;
                }
                streamObjectCompactionScheduler.execute(() -> {
                    try {
                        // trigger compaction after trim to clean up the expired stream objects.
                        this.compact(CLEANUP);
                    } finally {
                        trimCompactionSemaphore.release();
                    }
                });
            });

        }

        @Override
        public CompletableFuture<Void> close() {
            return runInLock(() -> {
                CompletableFuture<Stream> cf = new CompletableFuture<>();
                openedStreams.remove(streamId(), this);
                closingStreams.put(streamId(), cf);
                return stream.close().whenComplete((v, e) -> runInLock(() -> {
                    cf.complete(StreamWrapper.this);
                    closingStreams.remove(streamId(), cf);
                }));
            });
        }

        @Override
        public CompletableFuture<Void> destroy() {
            return runInLock(() -> {
                CompletableFuture<Stream> cf = new CompletableFuture<>();
                openedStreams.remove(streamId(), this);
                closingStreams.put(streamId(), cf);
                return stream.destroy().whenComplete((v, e) -> runInLock(() -> {
                    cf.complete(StreamWrapper.this);
                    closingStreams.remove(streamId(), cf);
                }));
            });
        }

        public boolean isClosed() {
            return stream.isClosed();
        }

        public void compact() {
            if (System.currentTimeMillis() - lastMajorCompactionTimestamp > TimeUnit.MINUTES.toMillis(config.streamObjectCompactionIntervalMinutes())) {
                compact(MAJOR);
                lastMajorCompactionTimestamp = System.currentTimeMillis();
            } else {
                compact(MINOR);
            }
        }

        public void compact(StreamObjectCompactor.CompactionType compactionType) {
            if (isClosed()) {
                // the compaction task may be taking a long time,
                // so we need to check if the stream is closed before starting the compaction.
                return;
            }
            if (System.currentTimeMillis() - lastCompactionTimestamp < STREAM_OBJECT_COMPACTION_INTERVAL_MS) {
                // skip compaction if the last compaction is within the interval.
                return;
            }
            StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).stream(this)
                .s3Operator(s3Operator).maxStreamObjectSize(config.streamObjectCompactionMaxSizeBytes()).build();
            task.compact(compactionType);
            lastCompactionTimestamp = System.currentTimeMillis();
        }
    }
}
