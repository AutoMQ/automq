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

import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.compact.AsyncTokenBucketThrottle;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class S3StreamClient implements StreamClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamClient.class);
    private final ScheduledThreadPoolExecutor streamObjectCompactionExecutor = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("stream-object-compaction-background", true), LOGGER, true);
    private ScheduledFuture<?> scheduledCompactionTaskFuture;
    private final Map<Long, S3Stream> openedStreams;

    private final StreamManager streamManager;
    private final Storage storage;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final Config config;
    private final AsyncTokenBucketThrottle readThrottle;

    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager, S3Operator s3Operator, Config config) {
        this.streamManager = streamManager;
        this.storage = storage;
        this.openedStreams = new ConcurrentHashMap<>();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.config = config;
        this.readThrottle = new AsyncTokenBucketThrottle(config.s3StreamObjectsCompactionNWInBandwidth(), 1, "s3-stream-objects-compaction");
        startStreamObjectsCompactions();
    }

    @Override
    public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
        TimerUtil timerUtil = new TimerUtil();
        return FutureUtil.exec(() -> streamManager.createStream().thenCompose(streamId -> {
            OperationMetricsStats.getOrCreateOperationMetrics(S3Operation.CREATE_STREAM).operationCount.inc();
            OperationMetricsStats.getOrCreateOperationMetrics(S3Operation.CREATE_STREAM).operationTime.update(timerUtil.elapsed());
            return openStream0(streamId, options.epoch());
        }), LOGGER, "createAndOpenStream");
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return FutureUtil.exec(() -> openStream0(streamId, openStreamOptions.epoch()), LOGGER, "openStream");
    }

    /**
     * Start stream objects compactions.
     */
    private void startStreamObjectsCompactions() {
        scheduledCompactionTaskFuture = streamObjectCompactionExecutor.scheduleWithFixedDelay(() -> {
            List<S3Stream> operationStreams = new LinkedList<>(openedStreams.values());
            CompactionTasksSummary.Builder totalSummaryBuilder = CompactionTasksSummary.builder();
            final long startTime = System.currentTimeMillis();

            operationStreams.forEach(stream -> {
                if (stream.isClosed()) {
                    return;
                }
                try {
                    StreamObjectsCompactionTask.CompactionSummary summary = stream.triggerCompactionTask();
                    if (summary == null) {
                        LOGGER.debug("[stream {}] stream objects compaction finished, no compaction happened", stream.streamId());
                    } else {
                        LOGGER.debug("[stream {}] stream objects compaction finished, compaction summary: {}", stream.streamId(), summary);
                        totalSummaryBuilder.withItem(summary);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("get exception when do stream objects compaction: {}", e.getMessage());
                    if (e.getCause() instanceof StreamObjectsCompactionTask.HaltException) {
                        LOGGER.error("halt stream objects compaction for stream {}", stream.streamId());
                    }
                } catch (Throwable e) {
                    LOGGER.error("get exception when do stream objects compaction: {}", e.getMessage());
                }
            });

            final long totalTimeCostInMs = System.currentTimeMillis() - startTime;
            LOGGER.info("stream objects compaction finished, summary: {}", totalSummaryBuilder.withTimeCostInMs(totalTimeCostInMs).build());
        }, config.s3StreamObjectCompactionTaskIntervalMinutes(), config.s3StreamObjectCompactionTaskIntervalMinutes(), TimeUnit.MINUTES);
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch) {
        TimerUtil timerUtil = new TimerUtil();
        return streamManager.openStream(streamId, epoch).
                thenApply(metadata -> {
                    OperationMetricsStats.getOrCreateOperationMetrics(S3Operation.OPEN_STREAM).operationCount.inc();
                    OperationMetricsStats.getOrCreateOperationMetrics(S3Operation.OPEN_STREAM).operationTime.update(timerUtil.elapsed());
                    StreamObjectsCompactionTask.Builder builder = new StreamObjectsCompactionTask.Builder(objectManager, s3Operator)
                            .withCompactedStreamObjectMaxSizeInBytes(config.s3StreamObjectCompactionMaxSizeBytes())
                            .withEligibleStreamObjectLivingTimeInMs(config.s3StreamObjectCompactionLivingTimeMinutes() * 60L * 1000)
                            .withS3ObjectLogEnabled(config.s3ObjectLogEnable())
                            .withReadThrottle(readThrottle);
                    S3Stream stream = new S3Stream(
                        metadata.getStreamId(), metadata.getEpoch(),
                        metadata.getStartOffset(), metadata.getEndOffset(),
                        storage, streamManager, builder, id -> {
                        openedStreams.remove(id);
                        return null;
                    });
                    openedStreams.put(streamId, stream);
                    return stream;
                });
    }

    @Override
    public void shutdown() {
        // cancel the submitted task if not started; do not interrupt the task if it is running.
        if (scheduledCompactionTaskFuture != null) {
            scheduledCompactionTaskFuture.cancel(false);
        }
        streamObjectCompactionExecutor.shutdown();
        if (readThrottle != null) {
            readThrottle.stop();
        }
    }

    private static class CompactionTasksSummary {
        private final long involvedStreamCount;
        private final long sourceObjectsTotalSize;
        private final long sourceObjectsCount;
        private final long targetObjectsCount;
        private final long smallSizeCopyWriteCount;
        private final long timeCostInMs;

        private CompactionTasksSummary(long involvedStreamCount, long sourceObjectsTotalSize, long sourceObjectsCount, long targetObjectsCount, long smallSizeCopyWriteCount,
            long timeCostInMs) {
            this.involvedStreamCount = involvedStreamCount;
            this.sourceObjectsTotalSize = sourceObjectsTotalSize;
            this.sourceObjectsCount = sourceObjectsCount;
            this.targetObjectsCount = targetObjectsCount;
            this.smallSizeCopyWriteCount = smallSizeCopyWriteCount;
            this.timeCostInMs = timeCostInMs;
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public String toString() {
            return "CompactionTasksSummary{" +
                "involvedStreamCount=" + involvedStreamCount +
                ", sourceObjectsTotalSize=" + sourceObjectsTotalSize +
                ", sourceObjectsCount=" + sourceObjectsCount +
                ", targetObjectsCount=" + targetObjectsCount +
                ", smallSizeCopyWriteCount=" + smallSizeCopyWriteCount +
                ", timeCostInMs=" + timeCostInMs +
                '}';
        }

        public static class Builder {
            private long involvedStreamCount;
            private long sourceObjectsTotalSize;
            private long sourceObjectsCount;
            private long targetObjectsCount;
            private long smallSizeCopyWriteCount;
            private long timeCostInMs;

            public Builder withItem(StreamObjectsCompactionTask.CompactionSummary compactionSummary) {
                if (compactionSummary == null) {
                    return this;
                }
                this.involvedStreamCount++;
                this.sourceObjectsTotalSize += compactionSummary.getTotalObjectSize();
                this.sourceObjectsCount += compactionSummary.getSourceObjectsCount();
                this.targetObjectsCount += compactionSummary.getTargetObjectCount();
                this.smallSizeCopyWriteCount += compactionSummary.getSmallSizeCopyWriteCount();
                return this;
            }

            public Builder withTimeCostInMs(long timeCostInMs) {
                this.timeCostInMs = timeCostInMs;
                return this;
            }

            public CompactionTasksSummary build() {
                return new CompactionTasksSummary(involvedStreamCount, sourceObjectsTotalSize, sourceObjectsCount, targetObjectsCount, smallSizeCopyWriteCount, timeCostInMs);
            }
        }

    }
}
