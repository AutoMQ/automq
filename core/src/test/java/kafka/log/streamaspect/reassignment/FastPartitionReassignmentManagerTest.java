/*
 * Copyright 2026, AutoMQ HK Limited.
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
package kafka.log.streamaspect.reassignment;

import kafka.log.streamaspect.MetaKeyValue;
import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies the manager's send, atomic receive, destructive take, and lifecycle cleanup boundary. */
@Tag("S3Unit")
public class FastPartitionReassignmentManagerTest {

    /** Given a whole batch is staged, destructive take returns each exact entry only once. */
    @Test
    public void testReceiveAndDestructiveTakeThroughManagerBoundary() {
        FastPartitionReassignmentManager manager = manager(new PartitionHandoffCache());
        PartitionHandoff first = handoff(0, 10);
        PartitionHandoff second = handoff(1, 20);

        manager.receive(List.of(first, second));
        assertEquals(first, manager.take(first.key()).orElseThrow());
        assertTrue(manager.take(first.key()).isEmpty());
        assertEquals(second, manager.take(second.key()).orElseThrow());
    }

    /** Given send transport fails exceptionally, manager resolves one stable fallback outcome. */
    @Test
    public void testSendFailureResolvesToFallback() {
        AtomicInteger attempts = new AtomicInteger();
        FastPartitionReassignmentManager manager = FastPartitionReassignmentManager.create((target, handoff) -> {
            attempts.incrementAndGet();
            return CompletableFuture.failedFuture(new IllegalStateException("unavailable"));
        }, new PartitionHandoffCache(), (topicId, partitionId) -> new Node(2, "target", 9092), () -> { });

        CompletionException exception = assertThrows(CompletionException.class,
            () -> manager.send(handoff(0, 10)).join());
        PartitionHandoffSendException failure = assertInstanceOf(
            PartitionHandoffSendException.class, exception.getCause());

        assertEquals(1, attempts.get());
        assertEquals(PartitionHandoffSendException.Reason.SEND_FAILURE, failure.reason());
    }

    /** Given fast reassignment is disabled, send fails with the silent not-attempted reason. */
    @Test
    public void testDisabledSendIsNotAttempted() {
        CompletionException exception = assertThrows(CompletionException.class,
            () -> FastPartitionReassignmentManager.disabled().send(handoff(0, 10)).join());
        PartitionHandoffSendException failure = assertInstanceOf(
            PartitionHandoffSendException.class, exception.getCause());

        assertEquals(PartitionHandoffSendException.Reason.NOT_ATTEMPTED, failure.reason());
    }

    /** Given V6 metadata has one healthy remote replica, source send derives that broker without caller state. */
    @Test
    public void testSendResolvesTargetFromMetadataCache() {
        Uuid topicId = Uuid.randomUuid();
        ListenerName listenerName = new ListenerName("INTERNAL");
        Node target = new Node(2, "target", 9092);
        MetadataCache metadataCache = mock(MetadataCache.class);
        when(metadataCache.autoMQVersion()).thenReturn(AutoMQVersion.V6);
        when(metadataCache.topicIdsToNames()).thenReturn(Map.of(topicId, "topic"));
        when(metadataCache.getPartitionInfo("topic", 0)).thenReturn(scala.Option.apply(
            new UpdateMetadataPartitionState().setReplicas(List.of(2))));
        when(metadataCache.getNode(2)).thenReturn(new BrokerRegistration.Builder()
            .setId(2)
            .setEpoch(1L)
            .setIncarnationId(Uuid.randomUuid())
            .build());
        when(metadataCache.getAliveBrokerNode(2, listenerName)).thenReturn(scala.Option.apply(target));
        AtomicInteger attempts = new AtomicInteger();
        FastPartitionReassignmentManager manager = FastPartitionReassignmentManager.create((node, handoff) -> {
            assertEquals(target, node);
            attempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }, new PartitionHandoffCache(),
            (resolvedTopicId, partitionId) -> FastPartitionReassignmentManager.resolveTarget(
                metadataCache, listenerName, 1, resolvedTopicId, partitionId), () -> { });

        manager.send(handoff(topicId, 0, 10)).join();
        assertEquals(1, attempts.get());
    }

    /** Given broker lifecycle cleanup runs, staged hints are discarded and the owned resource is closed. */
    @Test
    public void testCloseDiscardsCacheAndOwnedResources() {
        AtomicBoolean resourceClosed = new AtomicBoolean();
        FastPartitionReassignmentManager manager = FastPartitionReassignmentManager.create(
            (target, value) -> CompletableFuture.completedFuture(null), new PartitionHandoffCache(),
            (topicId, partitionId) -> null, () -> resourceClosed.set(true));
        PartitionHandoff handoff = handoff(0, 10);
        manager.receive(List.of(handoff));

        manager.close();

        assertTrue(resourceClosed.get());
        assertTrue(manager.take(handoff.key()).isEmpty());
        manager.receive(List.of(handoff));
        assertTrue(manager.take(handoff.key()).isEmpty());
    }

    private static PartitionHandoff handoff(int partitionId, long endOffset) {
        return handoff(Uuid.ZERO_UUID, partitionId, endOffset);
    }

    private static FastPartitionReassignmentManager manager(PartitionHandoffCache cache) {
        return FastPartitionReassignmentManager.create(
            (target, handoff) -> CompletableFuture.completedFuture(null), cache,
            (topicId, partitionId) -> null, () -> { });
    }

    private static PartitionHandoff handoff(Uuid topicId, int partitionId, long endOffset) {
        MetaKeyValue keyValue = MetaKeyValue.of("unknown", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        return new PartitionHandoff(topicId, partitionId, new MetaStreamHandoff(endOffset,
            List.of(new MetaStreamHandoffRecord(endOffset - 1, MetaKeyValue.encode(keyValue)))));
    }
}
