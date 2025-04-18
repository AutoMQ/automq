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

package org.apache.kafka.controller.stream;

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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    private final KVControlManager kvControlManager;

    private CompletableFuture<Void> lastDeletion = CompletableFuture.completedFuture(null);

    public TopicDeletionManager(
        SnapshotRegistry registry,
        Controller quorumController,
        StreamControlManager streamControlManager,
        KVControlManager kvControlManager
    ) {
        this.waitingCleanupTopics = new TimelineHashMap<>(registry, 0);
        this.quorumController = quorumController;
        this.streamControlManager = streamControlManager;
        this.kvControlManager = kvControlManager;
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
        TimelineHashMap<Long, StreamRuntimeMetadata> streams = streamControlManager.streamsMetadata();
        List<Long> streamsToDelete = new LinkedList<>();
        // check all streams are closed
        for (Map.Entry<Long, StreamRuntimeMetadata> entry : streams.entrySet(lastStableOffset)) {
            Long streamId = entry.getKey();
            StreamRuntimeMetadata metadata = entry.getValue();
            if (topicId.equals(metadata.tags().get(StreamTags.Topic.KEY))) {
                if (metadata.currentState() == StreamState.CLOSED) {
                    streamsToDelete.add(streamId);
                } else {
                    return ControllerResult.of(Collections.emptyList(), null);
                }
            }
        }
        if (streamsToDelete.isEmpty()) {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            List<String> metaStreamKvList = new ArrayList<>();
            String metaStreamKvPrefix = ObjectUtils.genMetaStreamKvPrefix(topicId);
            kvControlManager.kv().forEach((k, v) -> {
                if (k.startsWith(metaStreamKvPrefix)) {
                    metaStreamKvList.add(k);
                }
            });
            records.add(new ApiMessageAndVersion(new RemoveKVRecord().setKeys(metaStreamKvList), (short) 0));
            records.add(new ApiMessageAndVersion(new RemoveKVRecord().setKeys(List.of(TopicDeletion.TOPIC_DELETION_PREFIX + topicId)), (short) 0));
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
