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

package kafka.log.s3;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kafka.log.es.api.CreateStreamOptions;
import kafka.log.es.api.OpenStreamOptions;
import kafka.log.es.api.Stream;
import kafka.log.es.api.StreamClient;
import kafka.log.es.utils.Threads;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.streams.StreamManager;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static kafka.log.es.FutureUtil.exec;

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
    private final KafkaConfig config;

    public S3StreamClient(StreamManager streamManager, Storage storage, ObjectManager objectManager, S3Operator s3Operator, KafkaConfig config) {
        this.streamManager = streamManager;
        this.storage = storage;
        this.openedStreams = new ConcurrentHashMap<>();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.config = config;
        startStreamObjectsCompactions();
    }

    @Override
    public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
        return exec(() -> streamManager.createStream().thenCompose(streamId -> openStream0(streamId, options.epoch())),
                LOGGER, "createAndOpenStream");
    }

    @Override
    public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
        return exec(() -> openStream0(streamId, openStreamOptions.epoch()), LOGGER, "openStream");
    }

    /**
     * Start stream objects compactions.
     */
    private void startStreamObjectsCompactions() {
        scheduledCompactionTaskFuture = streamObjectCompactionExecutor.scheduleWithFixedDelay(() -> {
            List<S3Stream> operationStreams = new LinkedList<>(openedStreams.values());
            operationStreams.forEach(stream -> {
                if (stream.isClosed()) {
                    return;
                }
                try {
                    LOGGER.info("start to do stream objects compaction for stream {}", stream.streamId());
                    stream.triggerCompactionTask();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("get exception when do stream objects compaction: {}", e.getMessage());
                    if (e.getCause() instanceof StreamObjectsCompactionTask.HaltException) {
                        LOGGER.error("halt stream objects compaction for stream {}", stream.streamId());
                    }
                } catch (Throwable e) {
                    LOGGER.error("get exception when do stream objects compaction: {}", e.getMessage());
                }
            });
        }, config.s3StreamObjectCompactionTaskIntervalMinutes(), config.s3StreamObjectCompactionTaskIntervalMinutes(), TimeUnit.MINUTES);
    }

    private CompletableFuture<Stream> openStream0(long streamId, long epoch) {
        return streamManager.openStream(streamId, epoch).
                thenApply(metadata -> {
                    StreamObjectsCompactionTask.Builder builder = new StreamObjectsCompactionTask.Builder(objectManager, s3Operator)
                        .withCompactedStreamObjectMaxSizeInBytes(config.s3StreamObjectCompactionMaxSizeBytes())
                        .withEligibleStreamObjectLivingTimeInMs(config.s3StreamObjectCompactionLivingTimeMinutes() * 60L * 1000);
                    S3Stream stream = new S3Stream(
                        metadata.getStreamId(), metadata.getEpoch(),
                        metadata.getStartOffset(), metadata.getNextOffset(),
                        storage, streamManager, builder, id -> {
                        openedStreams.remove(id);
                        return null;
                    });
                    openedStreams.put(streamId, stream);
                    return stream;
                });
    }

    public void shutdown() {
        // cancel the submitted task if not started; do not interrupt the task if it is running.
        if (scheduledCompactionTaskFuture != null) {
            scheduledCompactionTaskFuture.cancel(false);
        }
        streamObjectCompactionExecutor.shutdown();
    }
}
