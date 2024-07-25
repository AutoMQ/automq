/*
 * Copyright 2024, AutoMQ HK Limited.
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
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.MINOR_V1_COMPACTION_SIZE_THRESHOLD;

public class S3StreamClient implements StreamClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamClient.class);
    private static final long COMPACTION_COOLDOWN_AFTER_OPEN_STREAM = Systems.getEnvLong("AUTOMQ_STREAM_COMPACTION_COOLDOWN_AFTER_OPEN_STREAM", TimeUnit.MINUTES.toMillis(1));
    private static final long MINOR_V1_COMPACTION_INTERVAL = Systems.getEnvLong("AUTOMQ_STREAM_COMPACTION_MINOR_V1_INTERVAL", TimeUnit.MINUTES.toMillis(10));
    private static final long MAJOR_V1_COMPACTION_INTERVAL = Systems.getEnvLong("AUTOMQ_STREAM_COMPACTION_MAJOR_V1_INTERVAL", TimeUnit.MINUTES.toMillis(60));
    private static final long MINOR_V1_COMPACTION_SIZE = Systems.getEnvLong("AUTOMQ_STREAM_COMPACTION_MINOR_V1_COMPACTION_SIZE_THRESHOLD", MINOR_V1_COMPACTION_SIZE_THRESHOLD);
    /**
     * When the cluster objects count exceed MAJOR_V1_COMPACTION_MAX_OBJECT_THRESHOLD, the MAJOR_V1 compaction will be triggered.
     * Default value is 400000: 10w partitions ~= 30w streams ~= 40w object
     */
    private static final int MAJOR_V1_COMPACTION_MAX_OBJECT_THRESHOLD = Systems.getEnvInt("AUTOMQ_STREAM_COMPACTION_MAJOR_V1_MAX_OBJECT_THRESHOLD", 400000);
    private final ScheduledExecutorService streamObjectCompactionScheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("stream-object-compaction-scheduler", true), LOGGER, true);
    final Map<Long, StreamWrapper> openedStreams;
    private final StreamManager streamManager;
    private final Storage storage;
    private final ObjectManager objectManager;
    private final ObjectStorage objectStorage;
    private final Config config;
    private final AsyncNetworkBandwidthLimiter networkInboundBucket;
    private final AsyncNetworkBandwidthLimiter networkOutboundBucket;
    private ScheduledFuture<?> scheduledCompactionTaskFuture;

    private final ReentrantLock lock = new ReentrantLock();

    final Map<Long, CompletableFuture<Stream>> openingStreams = new ConcurrentHashMap<>();
    final Map<Long, CompletableFuture<Stream>> closingStreams = new ConcurrentHashMap<>();

    private final List<StreamLifeCycleListener> streamLifeCycleListeners = new CopyOnWriteArrayList<>();

    private boolean closed;

    @SuppressWarnings("unused")
    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager,
        ObjectStorage objectStorage, Config config) {
        this(streamManager, storage, objectManager, objectStorage, config, null, null);
    }

    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager,
        ObjectStorage objectStorage, Config config,
        AsyncNetworkBandwidthLimiter networkInboundBucket, AsyncNetworkBandwidthLimiter networkOutboundBucket) {
        this.streamManager = streamManager;
        this.storage = storage;
        this.openedStreams = new ConcurrentHashMap<>();
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
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

    public void registerStreamLifeCycleListener(StreamLifeCycleListener listener) {
        streamLifeCycleListeners.add(listener);
    }

    /**
     * Start stream objects compactions.
     */
    private void startStreamObjectsCompactions() {
        long compactionJitterDelay = ThreadLocalRandom.current().nextInt(20);

        scheduledCompactionTaskFuture = streamObjectCompactionScheduler.scheduleWithFixedDelay(() -> {
            try {
                CompactionHint hint = new CompactionHint(objectManager.getObjectsCount().get());
                List<StreamWrapper> operationStreams = new ArrayList<>(openedStreams.values());
                operationStreams.forEach(s -> s.compact(hint));
            } catch (Throwable e) {
                LOGGER.info("run stream object compaction task failed", e);
            }
        }, compactionJitterDelay, 1, TimeUnit.MINUTES);
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
        private final long openedTimestamp = System.currentTimeMillis();
        private long lastMinorCompactionTimestamp = System.currentTimeMillis();
        private long lastMajorCompactionTimestamp = System.currentTimeMillis();
        private long lastMinorV1CompactionTimestamp = System.currentTimeMillis();
        private long lastMajorV1CompactionTimestamp = System.currentTimeMillis();

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
            return stream.trim(newStartOffset);
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
                    for (StreamLifeCycleListener listener : streamLifeCycleListeners) {
                        listener.onStreamClose(streamId());
                    }
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

        public void compact(CompactionHint hint) {
            if (isClosed()) {
                // the compaction task may be taking a long time,
                // so we need to check if the stream is closed before starting the compaction.
                return;
            }
            long now = System.currentTimeMillis();
            if (now - openedTimestamp < COMPACTION_COOLDOWN_AFTER_OPEN_STREAM) {
                // skip compaction in the first few minutes after the stream is opened
                return;
            }
            if (config.version().isStreamObjectCompactV1Supported()) {
                compactV1(hint, now);
            } else {
                compactV0(now);
            }

        }

        private void compactV0(long now) {
            if (now - lastMajorCompactionTimestamp > TimeUnit.MINUTES.toMillis(config.streamObjectCompactionIntervalMinutes())) {
                compact(MAJOR, null);
                lastMajorCompactionTimestamp = System.currentTimeMillis();
            } else if (now - lastMinorCompactionTimestamp > TimeUnit.MINUTES.toMillis(5)) {
                compact(MINOR, null);
                lastMinorCompactionTimestamp = System.currentTimeMillis();
            } else {
                compact(CLEANUP, null);
            }
        }

        private void compactV1(CompactionHint hint, long now) {
            if (now - lastMajorV1CompactionTimestamp > MAJOR_V1_COMPACTION_INTERVAL || hint.objectsCount >= MAJOR_V1_COMPACTION_MAX_OBJECT_THRESHOLD) {
                compact(MAJOR_V1, hint);
                lastMajorV1CompactionTimestamp = System.currentTimeMillis();
            } else if (now - lastMinorV1CompactionTimestamp > MINOR_V1_COMPACTION_INTERVAL) {
                compact(MINOR_V1, hint);
                lastMinorV1CompactionTimestamp = System.currentTimeMillis();
            } else {
                compact(CLEANUP_V1, hint);
            }
        }

        public void compact(StreamObjectCompactor.CompactionType compactionType, CompactionHint hint) {
            StreamObjectCompactor.Builder taskBuilder = StreamObjectCompactor.builder()
                .objectManager(objectManager)
                .stream(this)
                .objectStorage(objectStorage)
                .maxStreamObjectSize(config.streamObjectCompactionMaxSizeBytes())
                .minorV1CompactionThreshold(MINOR_V1_COMPACTION_SIZE);

            if (hint != null) {
                taskBuilder.majorV1CompactionSkipSmallObject(hint.objectsCount < MAJOR_V1_COMPACTION_MAX_OBJECT_THRESHOLD);
            }

            taskBuilder.build().compact(compactionType);
        }
    }

    static class CompactionHint {
        int objectsCount;

        public CompactionHint(int objectsCount) {
            this.objectsCount = objectsCount;
        }

    }

    public interface StreamLifeCycleListener {
        void onStreamClose(long streamId);
    }
}
