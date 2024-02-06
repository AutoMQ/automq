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

import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StreamOperationStats;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3StreamClient implements StreamClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamClient.class);
    private final ScheduledExecutorService streamObjectCompactionScheduler = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("stream-object-compaction-scheduler", true), LOGGER, true);
    private final Map<Long, S3Stream> openedStreams;
    private final StreamManager streamManager;
    private final Storage storage;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final Config config;
    private final AsyncNetworkBandwidthLimiter networkInboundBucket;
    private final AsyncNetworkBandwidthLimiter networkOutboundBucket;
    private ScheduledFuture<?> scheduledCompactionTaskFuture;

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
        TimerUtil timerUtil = new TimerUtil();
        return FutureUtil.exec(() -> streamManager.createStream().thenCompose(streamId -> {
            StreamOperationStats.getInstance().createStreamStats.record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            return openStream0(streamId, options.epoch());
        }), LOGGER, "createAndOpenStream");
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return FutureUtil.exec(() -> openStream0(streamId, openStreamOptions.epoch()), LOGGER, "openStream");
    }

    @Override
    public Optional<Stream> getStream(long streamId) {
        return Optional.ofNullable(openedStreams.get(streamId));
    }

    /**
     * Start stream objects compactions.
     */
    private void startStreamObjectsCompactions() {
        scheduledCompactionTaskFuture = streamObjectCompactionScheduler.scheduleWithFixedDelay(() -> {
            List<S3Stream> operationStreams = new LinkedList<>(openedStreams.values());
            operationStreams.forEach(stream -> {
                StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).stream(stream)
                    .s3Operator(s3Operator).maxStreamObjectSize(config.streamObjectCompactionMaxSizeBytes()).build();
                task.compact();
            });
        }, config.streamObjectCompactionIntervalMinutes(), config.streamObjectCompactionIntervalMinutes(), TimeUnit.MINUTES);
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch) {
        TimerUtil timerUtil = new TimerUtil();
        return streamManager.openStream(streamId, epoch).
            thenApply(metadata -> {
                StreamObjectCompactor.Builder builder = StreamObjectCompactor.builder().objectManager(objectManager).s3Operator(s3Operator)
                    .maxStreamObjectSize(config.streamObjectCompactionMaxSizeBytes());
                S3Stream stream = new S3Stream(
                    metadata.streamId(), metadata.epoch(),
                    metadata.startOffset(), metadata.endOffset(),
                    storage, streamManager, openedStreams::remove, networkInboundBucket, networkOutboundBucket);
                openedStreams.put(streamId, stream);
                StreamOperationStats.getInstance().openStreamStats.record(MetricsLevel.INFO, timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                return stream;
            });
    }

    @Override
    public void shutdown() {
        // cancel the submitted task if not started; do not interrupt the task if it is running.
        if (scheduledCompactionTaskFuture != null) {
            scheduledCompactionTaskFuture.cancel(false);
        }
        streamObjectCompactionScheduler.shutdown();
        try {
            if (streamObjectCompactionScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn("await streamObjectCompactionExecutor timeout 10s");
            }
        } catch (InterruptedException e) {
            streamObjectCompactionScheduler.shutdownNow();
            LOGGER.warn("await streamObjectCompactionExecutor close fail", e);
        }

        TimerUtil timerUtil = new TimerUtil();
        Map<Long, CompletableFuture<Void>> streamCloseFutures = new ConcurrentHashMap<>();
        openedStreams.forEach((streamId, stream) -> streamCloseFutures.put(streamId, stream.close()));
        for (; ; ) {
            Threads.sleep(1000);
            List<Long> closingStreams = streamCloseFutures.entrySet().stream().filter(e -> !e.getValue().isDone()).map(Map.Entry::getKey).collect(Collectors.toList());
            LOGGER.info("waiting streams close, closed {} / all {}, closing[{}]", streamCloseFutures.size() - closingStreams.size(), streamCloseFutures.size(), closingStreams);
            if (closingStreams.isEmpty()) {
                break;
            }
        }
        LOGGER.info("wait streams[{}] closed cost {}ms", streamCloseFutures.keySet(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
    }
}
