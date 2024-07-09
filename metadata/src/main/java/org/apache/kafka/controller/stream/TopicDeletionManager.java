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

package org.apache.kafka.controller.stream;

import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.utils.Threads;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteStreamsRequestData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.stream.TopicDeletion.Status;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup deleted topic workflow:
 * 1. ReplicationControlManager#deleteTopic generate kv record with key "{@link TopicDeletion#TOPIC_DELETION_PREFIX}/{topicId}"
 * 2. {@link TopicDeletionManager} replay the kv record and put the topicId to {@link TopicDeletionManager#waitingCleanupTopics}
 * 3. {@link TopicDeletionManager} schedule scan the waiting cleanup topics and delete the streams related to topic
 * 4. After the streams related to the topic are deleted, {@link TopicDeletionManager} generate RemoveKvRecord to remove the topicId from {@link TopicDeletionManager#waitingCleanupTopics}
 */
@SuppressWarnings("this-escape")
public class TopicDeletionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicDeletionManager.class);
    final TimelineHashMap<Uuid, Status> waitingCleanupTopics;

    private final Controller quorumController;
    private final StreamControlManager streamControlManager;

    private CompletableFuture<Void> lastDeletion = CompletableFuture.completedFuture(null);

    public TopicDeletionManager(
        SnapshotRegistry registry,
        Controller quorumController,
        StreamControlManager streamControlManager
    ) {
        this.waitingCleanupTopics = new TimelineHashMap<>(registry, 0);
        this.quorumController = quorumController;
        this.streamControlManager = streamControlManager;
        Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(this::cleanupDeletedTopics, 5, 5, TimeUnit.SECONDS);
    }

    public void cleanupDeletedTopics() {
        if (quorumController.isActive()) {
            quorumController.appendWriteEvent("cleanupDeletedTopic", OptionalLong.empty(), () -> {
                try {
                    return this.cleanupDeletedTopics0();
                } catch (Throwable ex) {
                    LOGGER.error("[CLEANUP_DELETED_TOPIC],[UNEXPECTED]", ex);
                    return ControllerResult.of(Collections.emptyList(), null);
                }
            });
        }
    }

    public ControllerResult<Void> cleanupDeletedTopics0() {
        long lastStableOffset = quorumController.lastStableOffset();
        if (waitingCleanupTopics.isEmpty(lastStableOffset) || !lastDeletion.isDone()) {
            return ControllerResult.of(Collections.emptyList(), null);
        }
        String topicId = waitingCleanupTopics.entrySet(lastStableOffset).iterator().next().getKey().toString();
        TimelineHashMap<Long, S3StreamMetadata> streams = streamControlManager.streamsMetadata();
        List<Long> streamsToDelete = new LinkedList<>();
        // check all streams are closed
        for (Map.Entry<Long, S3StreamMetadata> entry : streams.entrySet(lastStableOffset)) {
            Long streamId = entry.getKey();
            S3StreamMetadata metadata = entry.getValue();
            if (topicId.equals(metadata.tags().get(StreamTags.Topic.KEY))) {
                if (metadata.currentState() == StreamState.CLOSED) {
                    streamsToDelete.add(streamId);
                } else {
                    return ControllerResult.of(Collections.emptyList(), null);
                }
            }
        }
        if (streamsToDelete.isEmpty()) {
            List<ApiMessageAndVersion> records = List.of(new ApiMessageAndVersion(new RemoveKVRecord().setKeys(List.of(TopicDeletion.TOPIC_DELETION_PREFIX + topicId)), (short) 0));
            LOGGER.info("Topic clean up completed for topic {}", topicId);
            if (waitingCleanupTopics.size(lastStableOffset) > 1) {
                // there are more topics to clean up, fast trigger next round cleanup
                cleanupDeletedTopics();
            }
            return ControllerResult.of(records, null);
        }
        lastDeletion = CompletableFuture.allOf(streamsToDelete.stream().map(streamId -> {
            ControllerRequestContext ctx = new ControllerRequestContext(null, null, OptionalLong.empty());
            DeleteStreamsRequestData req = new DeleteStreamsRequestData()
                .setDeleteStreamRequests(List.of(new DeleteStreamsRequestData.DeleteStreamRequest().setStreamId(streamId).setStreamEpoch(Long.MAX_VALUE)));
            return quorumController.deleteStreams(ctx, req).exceptionally(ex -> {
                LOGGER.error("Failed to delete stream {}", streamId, ex);
                return null;
            });
        }).toArray(CompletableFuture[]::new));
        // fast trigger next round cleanup to remove the topic
        lastDeletion.thenAccept(nil -> cleanupDeletedTopics());
        return ControllerResult.of(Collections.emptyList(), null);
    }

    public void replay(KVRecord kvRecord) {
        for (KVRecord.KeyValue kv : kvRecord.keyValues()) {
            if (kv.key() != null && kv.key().startsWith(TopicDeletion.TOPIC_DELETION_PREFIX)) {
                if (kv.value() == null || Integer.parseInt(new String(kv.value(), StandardCharsets.UTF_8)) != Status.PENDING.value()) {
                    continue;
                }
                Uuid topicId = Uuid.fromString(kv.key().substring(TopicDeletion.TOPIC_DELETION_PREFIX.length()));
                waitingCleanupTopics.putIfAbsent(topicId, Status.PENDING);
            }
        }
    }

    public void replay(RemoveKVRecord kvRecord) {
        for (String key : kvRecord.keys()) {
            if (key.startsWith(TopicDeletion.TOPIC_DELETION_PREFIX)) {
                Uuid topicId = Uuid.fromString(key.substring(TopicDeletion.TOPIC_DELETION_PREFIX.length()));
                waitingCleanupTopics.remove(topicId);
            }
        }
    }
}
