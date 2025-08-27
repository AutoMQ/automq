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

import kafka.automq.partition.snapshot.SnapshotOperation;
import kafka.automq.zerozone.SnapshotReadPartitionsManager.OperationBatch;
import kafka.automq.zerozone.SnapshotReadPartitionsManager.SnapshotWithOperation;
import kafka.automq.zerozone.SnapshotReadPartitionsManager.Subscriber;
import kafka.cluster.Partition;
import kafka.cluster.PartitionSnapshot;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticReplicaManager;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class SnapshotReadPartitionsManagerTest {
    KafkaConfig config;
    MockTime time;

    ElasticReplicaManager replicaManager;
    Map<TopicPartition, Partition> partitions;

    MetadataCache metadataCache;
    Map<Long, Long> stream2offset;
    Map<Uuid, String> topicId2name;

    AsyncSender asyncSender;

    SnapshotReadPartitionsManager manager;

    String topicName;
    Uuid topicId;

    BrokerRegistration broker1 = new BrokerRegistration.Builder().setId(1).setListeners(List.of(new Endpoint("BROKER", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092))).build();
    BrokerRegistration broker2 = new BrokerRegistration.Builder().setId(2).setListeners(List.of(new Endpoint("BROKER", SecurityProtocol.PLAINTEXT, "127.0.0.2", 9092))).build();

    @BeforeEach
    public void setup() {
        config = mock(KafkaConfig.class);
        when(config.nodeId()).thenReturn(1);
        when(config.interBrokerListenerName()).thenReturn(ListenerName.normalised("BROKER"));

        time = new MockTime();

        replicaManager = mock(ElasticReplicaManager.class);
        partitions = new HashMap<>();
        doAnswer(args -> {
            TopicPartition tp = args.getArgument(0);
            return partitions.compute(tp, args.getArgument(1));
        }).when(replicaManager).computeSnapshotReadPartition(any(), any());
        doAnswer(args -> mock(Partition.class)).when(replicaManager).newSnapshotReadPartition(any());

        metadataCache = mock(MetadataCache.class);
        stream2offset = new HashMap<>();
        doAnswer(args -> {
            Long offset = stream2offset.get((Long) args.getArgument(0));
            if (offset == null) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(offset);
            }
        }).when(metadataCache).getStreamEndOffset(anyLong());
        topicId2name = new HashMap<>();
        topicName = "topic1";
        topicId = Uuid.randomUuid();
        topicId2name.put(topicId, topicName);
        when(metadataCache.topicIdsToNames()).thenReturn(topicId2name);

        asyncSender = mock(AsyncSender.class);

        Replayer replayer = new Replayer() {
            @Override
            public CompletableFuture<Void> replay(List<S3ObjectMetadata> objects) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> replay(WriteAheadLog confirmWAL, RecordOffset startOffset, RecordOffset endOffset) {
                return CompletableFuture.completedFuture(null);
            }
        };

        manager = new SnapshotReadPartitionsManager(config, time, null, replicaManager, metadataCache, replayer, asyncSender);
    }

    @Test
    public void testSubscriber_zerozonev1() throws Exception {
        Node node = new Node(2, "127.0.0.1", 9092);
        SubscriberRequester requester = mock(SubscriberRequester.class);
        SubscriberReplayer dataLoader = mock(SubscriberReplayer.class);
        when(dataLoader.replayWal()).thenReturn(CompletableFuture.completedFuture(null));

        Subscriber subscriber = manager.newSubscriber(node, AutoMQVersion.V3, requester, dataLoader);

        OperationBatch operationBatch = new OperationBatch();
        operationBatch.operations.add(snapshotWithOperation(1, Map.of(3L, 3L), SnapshotOperation.ADD));
        operationBatch.operations.add(snapshotWithOperation(2, Map.of(4L, 6L), SnapshotOperation.ADD));
        subscriber.onNewOperationBatch(operationBatch);

        subscriber.run();
        verify(dataLoader, timeout(1000L).times(1)).replayWal();
        awaitEventLoopClear();

        assertEquals(2, subscriber.partitions.size());
        verify(partitions.get(new TopicPartition(topicName, 1)), times(1)).snapshot(any());
        verify(partitions.get(new TopicPartition(topicName, 2)), times(1)).snapshot(any());
    }

    @Test
    public void testSubscriber_zerozonev0() throws ExecutionException, InterruptedException {
        Node node = new Node(2, "127.0.0.1", 9092);
        SubscriberRequester requester = mock(SubscriberRequester.class);
        SubscriberReplayer dataLoader = mock(SubscriberReplayer.class);
        when(dataLoader.relayObject()).thenReturn(CompletableFuture.completedFuture(null));
        Subscriber subscriber = manager.newSubscriber(node, AutoMQVersion.V2, requester, dataLoader);
        stream2offset.put(3L, 0L);
        stream2offset.put(4L, 0L);

        OperationBatch operationBatch = new OperationBatch();
        operationBatch.operations.add(snapshotWithOperation(1, Map.of(3L, 3L), SnapshotOperation.ADD));
        operationBatch.operations.add(snapshotWithOperation(2, Map.of(4L, 6L), SnapshotOperation.ADD));
        subscriber.onNewOperationBatch(operationBatch);

        // metadata unready.
        subscriber.tryReplay();
        assertEquals(1, subscriber.waitingMetadataReadyQueue.size());
        assertEquals(-1, operationBatch.readyIndex);

        // metadata ready.
        stream2offset.put(3L, 3L);
        stream2offset.put(4L, 6L);
        subscriber.tryReplay();
        awaitEventLoopClear();
        assertEquals(2, subscriber.partitions.size());
        verify(partitions.get(new TopicPartition(topicName, 1)), times(1)).snapshot(any());
        verify(partitions.get(new TopicPartition(topicName, 2)), times(1)).snapshot(any());
        assertEquals(0, subscriber.waitingMetadataReadyQueue.size());
        assertEquals(0, subscriber.waitingDataLoadedQueue.size());
        assertEquals(0, subscriber.snapshotWithOperations.size());

        // partition2 append new records
        operationBatch = new OperationBatch();
        operationBatch.operations.add(snapshotWithOperation(2, Map.of(4L, 10L), SnapshotOperation.PATCH));
        subscriber.onNewOperationBatch(operationBatch);
        stream2offset.put(4L, 10L);
        subscriber.tryReplay();
        awaitEventLoopClear();
        verify(partitions.get(new TopicPartition(topicName, 1)), times(1)).snapshot(any());
        verify(partitions.get(new TopicPartition(topicName, 2)), times(2)).snapshot(any());

        // partition2 remove
        operationBatch = new OperationBatch();
        operationBatch.operations.add(snapshotWithOperation(2, Map.of(4L, 10L), SnapshotOperation.REMOVE));
        subscriber.onNewOperationBatch(operationBatch);
        subscriber.tryReplay();
        awaitEventLoopClear();
        assertEquals(1, partitions.size());
        assertEquals(1, subscriber.partitions.size());

    }

    private SnapshotWithOperation snapshotWithOperation(int partitionIndex, Map<Long /* streamId */, Long /* endOffset */> offsets,
        SnapshotOperation operation) {
        // TODO: fix the test
        PartitionSnapshot snapshot = new PartitionSnapshot(0, null, null, null, offsets, null);
        return new SnapshotWithOperation(new TopicIdPartition(topicId, partitionIndex, topicName), snapshot, operation);
    }

    private void awaitEventLoopClear() throws ExecutionException, InterruptedException {
        manager.eventLoop.submit(() -> {
        }).get();
    }

    static AutomqGetPartitionSnapshotResponseData snapshotResponse(Uuid topicId, int partitionId, int leaderEpoch,
        long endOffset, SnapshotOperation ops) {
        AutomqGetPartitionSnapshotResponseData data = new AutomqGetPartitionSnapshotResponseData();
        AutomqGetPartitionSnapshotResponseData.TopicCollection topics = new AutomqGetPartitionSnapshotResponseData.TopicCollection();
        AutomqGetPartitionSnapshotResponseData.Topic topic = new AutomqGetPartitionSnapshotResponseData.Topic();
        topic.setTopicId(topicId);
        AutomqGetPartitionSnapshotResponseData.PartitionSnapshot snapshot = new AutomqGetPartitionSnapshotResponseData.PartitionSnapshot();
        snapshot.setPartitionIndex(partitionId);
        snapshot.setOperation(ops.code());
        snapshot.setLeaderEpoch(leaderEpoch);
        snapshot.setLogMetadata(new AutomqGetPartitionSnapshotResponseData.LogMetadata());
        snapshot.setFirstUnstableOffset(null);
        snapshot.setLogEndOffset(new AutomqGetPartitionSnapshotResponseData.LogOffsetMetadata().setMessageOffset(endOffset).setRelativePositionInSegment(1));
        snapshot.setStreamMetadata(List.of(new AutomqGetPartitionSnapshotResponseData.StreamMetadata().setStreamId(partitionId).setEndOffset(endOffset)));
        topic.setPartitions(List.of(snapshot));
        topics.add(topic);
        data.setTopics(topics);
        return data;
    }

}
