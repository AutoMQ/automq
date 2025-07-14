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
import org.apache.kafka.common.message.DeleteStreamsResponseData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.StreamState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 40)
@Tag("S3Unit")
public class TopicDeletionManagerTest {
    final Uuid topicId = new Uuid(1, 2);
    SnapshotRegistry registry;
    Controller quorumController;

    TimelineHashMap<Long, StreamRuntimeMetadata> streams;
    StreamControlManager streamControlManager;

    TimelineHashMap<String, ByteBuffer> kvs;
    KVControlManager kvControlManager;

    TopicDeletionManager topicDeletionManager;

    @BeforeEach
    public void setup() {
        registry = new SnapshotRegistry(new LogContext());
        registry.getOrCreateSnapshot(0L);
        quorumController = mock(Controller.class);
        streams = new TimelineHashMap<>(registry, 0);
        streamControlManager = mock(StreamControlManager.class);
        when(streamControlManager.streamsMetadata()).thenReturn(streams);

        kvs = new TimelineHashMap<>(registry, 0);
        kvControlManager = mock(KVControlManager.class);
        when(kvControlManager.kv()).thenReturn(kvs);

        topicDeletionManager = new TopicDeletionManager(registry, quorumController, streamControlManager, kvControlManager);
    }

    @Test
    public void testCleanup() {
        registry.getOrCreateSnapshot(0L);
        when(quorumController.lastStableOffset()).thenReturn(1L);
        DeleteStreamsResponseData deleteStreamsResp = new DeleteStreamsResponseData();
        when(quorumController.deleteStreams(any(), any())).thenReturn(CompletableFuture.completedFuture(deleteStreamsResp));
        when(quorumController.isActive()).thenReturn(true);
        when(quorumController.appendWriteEvent(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        List<String> metadataKvList = List.of(
            ObjectUtils.genMetaStreamKvPrefix(topicId.toString()) + 1,
            ObjectUtils.genMetaStreamKvPrefix(topicId.toString()) + 2
        );
        metadataKvList.forEach(str -> kvs.put(str, ByteBuffer.wrap(new byte[0])));
        kvs.put(ObjectUtils.genMetaStreamKvPrefix(topicId + "others") + 1, ByteBuffer.wrap(new byte[0]));

        streams.put(1L, new StreamRuntimeMetadata(1L, 0, 0, 0,
            StreamState.CLOSED, Collections.emptyMap(), registry));
        Map<String, String> tags = new HashMap<>();
        tags.put(StreamTags.Topic.KEY, StreamTags.Topic.encode(topicId));
        streams.put(2L, new StreamRuntimeMetadata(2L, 0, 0, 0,
            StreamState.OPENED, tags, registry));
        streams.put(3L, new StreamRuntimeMetadata(3L, 0, 0, 0,
            StreamState.CLOSED, tags, registry));

        topicDeletionManager.replay(
            new KVRecord().setKeyValues(List.of(
                new KVRecord.KeyValue()
                    .setKey(TopicDeletion.TOPIC_DELETION_PREFIX + topicId)
                    .setValue(Integer.toString(TopicDeletion.Status.PENDING.value()).getBytes(StandardCharsets.UTF_8))
            ))
        );

        assertEquals(TopicDeletion.Status.PENDING, topicDeletionManager.waitingCleanupTopics.get(topicId));
        registry.getOrCreateSnapshot(1L);

        // streamId=2 is not closed, so it should not be deleted
        topicDeletionManager.cleanupDeletedTopics();
        verify(quorumController, times(0)).deleteStreams(any(), any());

        // close streamId=2, expect delete streamId=2/3
        streams.put(2L, new StreamRuntimeMetadata(2L, 0, 0, 0,
            StreamState.CLOSED, tags, registry));

        registry.getOrCreateSnapshot(2L);
        when(quorumController.lastStableOffset()).thenReturn(2L);

        replay(topicDeletionManager.cleanupDeletedTopics0(), topicDeletionManager);
        ArgumentCaptor<DeleteStreamsRequestData> ac = ArgumentCaptor.forClass(DeleteStreamsRequestData.class);
        verify(quorumController, times(2)).deleteStreams(any(), ac.capture());
        assertEquals(List.of(2L, 3L), ac.getAllValues().stream().map(r -> r.deleteStreamRequests().get(0).streamId()).sorted().collect(Collectors.toList()));
        // The topic will be removed in the next cleanup round
        assertEquals(1, topicDeletionManager.waitingCleanupTopics.size());

        registry.getOrCreateSnapshot(3L);
        when(quorumController.lastStableOffset()).thenReturn(3L);

        streams.remove(2L);
        streams.remove(3L);

        registry.getOrCreateSnapshot(4L);
        when(quorumController.lastStableOffset()).thenReturn(4L);

        ControllerResult<Void> rst = topicDeletionManager.cleanupDeletedTopics0();
        assertEquals(2, rst.records().size());
        assertEquals(metadataKvList.stream().sorted().collect(Collectors.toList()), ((RemoveKVRecord) rst.records().get(0).message()).keys().stream().sorted().collect(Collectors.toList()));
        assertEquals(List.of(TopicDeletion.TOPIC_DELETION_PREFIX + topicId), ((RemoveKVRecord) rst.records().get(1).message()).keys());
        replay(rst, topicDeletionManager);
        verify(quorumController, times(2)).deleteStreams(any(), any());
        assertEquals(0, topicDeletionManager.waitingCleanupTopics.size());
    }

    private void replay(ControllerResult<Void> rst, TopicDeletionManager manager) {
        for (ApiMessageAndVersion record : rst.records()) {
            switch (MetadataRecordType.fromId(record.message().apiKey())) {
                case KVRECORD:
                    manager.replay((KVRecord) record.message());
                    break;
                case REMOVE_KVRECORD:
                    manager.replay((RemoveKVRecord) record.message());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected record type " + MetadataRecordType.fromId(record.message().apiKey()));
            }
        }
    }

}
