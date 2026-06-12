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

package kafka.automq.availability;

import kafka.automq.availability.broker.ActionResponsePublisher;
import kafka.automq.availability.broker.AvailabilitySignalPublisher;
import kafka.automq.availability.broker.BrokerActionReceiver;
import kafka.automq.availability.broker.BrokerAvailabilityService;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import com.automq.stream.s3.cache.blockcache.ColdReadInflightRegistry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.AvailabilitySignalType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.RecoveryAction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class BrokerAvailabilityServiceTest {
    @Test
    public void testSignalPublisherPublishesEmptySnapshotForFreshCoverage() {
        MockTime time = new MockTime(0L, 100L, 0L);
        BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(1, 10L, 30L, 30L, 30L, 3,
            time::nanoseconds);
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        AvailabilitySignalPublisher publisher = new AvailabilitySignalPublisher(monitor, sender, time);

        publisher.publishOnceAsync().join();
        PutKVRequest request = sender.lastPut.putKVRequests().get(0);
        BrokerAvailabilitySnapshot snapshot = AvailabilityCodecs.decodeSignal(request.value());

        assertEquals(AvailabilityConstants.SIGNAL_NAMESPACE, request.namespace());
        assertEquals("1", request.key());
        assertEquals(1, snapshot.getBrokerId());
        assertTrue(snapshot.getSignals().isEmpty());
    }

    @Test
    public void testActionReceiverFiltersByBrokerAndDeadline() {
        BrokerActionReceiver receiver = new BrokerActionReceiver(2);
        RecoveryAction target = action(2, 200L);
        RecoveryAction otherBroker = action(3, 200L);
        RecoveryAction expired = action(2, 50L);

        assertEquals(List.of(target), receiver.pollActions(List.of(target, otherBroker, expired), 100L));
    }

    @Test
    public void testActionResponsePublisherBuildsResponseWrite() {
        Uuid actionUuid = Uuid.randomUuid();
        ActionResponse response = new ActionResponse(actionUuid, AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), ActionExecutionStatus.SUCCEEDED, 100L, 120L, null);
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        ActionResponsePublisher publisher = new ActionResponsePublisher(sender);

        publisher.publishAsync(response).join();
        PutKVRequest request = sender.lastPut.putKVRequests().get(0);

        assertEquals(AvailabilityConstants.RESPONSE_NAMESPACE, request.namespace());
        assertEquals(actionUuid.toString(), request.key());
        assertEquals(response, AvailabilityCodecs.decodeResponse(request.value()));
    }

    @Test
    public void testBrokerAvailabilityServiceLifecycleFlag() {
        MockTime time = new MockTime(0L, 100L, 0L);
        BrokerAvailabilityService service = new BrokerAvailabilityService(null, null, null, null, List::of,
            null, null, null, time, 0L, 0L);
        CompletableFuture<Void> coldRead = new CompletableFuture<>();

        assertFalse(service.isRunning());
        service.start();
        assertTrue(service.isRunning());
        ColdReadInflightRegistry.track(coldRead, System.nanoTime());
        assertEquals(1, ColdReadInflightRegistry.pendingCount());
        service.shutdown();
        assertFalse(service.isRunning());
        assertEquals(0, ColdReadInflightRegistry.pendingCount());
    }

    @Test
    public void testBrokerAvailabilityServiceRegistersRuntimeHook() {
        MockTime time = new MockTime(0L, 100L, 0L);
        BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(1, 10L, 30L, 30L, 30L, 3,
            time::nanoseconds);
        BrokerAvailabilityService service = new BrokerAvailabilityService(monitor, null, null, null, List::of,
            null, null, null, time, 0L, 0L);

        service.start();
        AvailabilityRuntimeHooks.recordLogWriteFailed(new org.apache.kafka.common.TopicPartition("topic", 0));
        assertTrue(monitor.snapshot(100L, 70L, 100L).hasSignal(AvailabilitySignalType.LOG_WRITE_FAIL));

        monitor.clearWindow();
        service.shutdown();
        AvailabilityRuntimeHooks.recordLogWriteFailed(new org.apache.kafka.common.TopicPartition("topic", 0));
        assertTrue(monitor.snapshot(101L, 71L, 101L).getSignals().isEmpty());
    }

    @Test
    public void testOpenRecoverActionConsumerExecutesAndDeletesMatchingAction() {
        MockTime time = new MockTime(0L, 100L, 0L);
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY,
            AvailabilityTarget.topicPartition("topic", 0), 1, 100L, 200L, "recover", false);
        BrokerAvailabilityService service = new BrokerAvailabilityService(null, null, new BrokerActionReceiver(1),
            new ActionResponsePublisher(sender), () -> List.of(action), null, sender, null, time, 0L, 0L);
        AtomicInteger executions = new AtomicInteger();

        service.start();
        try {
            boolean consumed = AvailabilityRuntimeHooks.consumeOpenRecoverAction(
                AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY, new TopicPartition("topic", 0),
                consumedAction -> executions.incrementAndGet());

            assertTrue(consumed);
            assertEquals(1, executions.get());
            ActionResponse response = AvailabilityCodecs.decodeResponse(sender.lastPut.putKVRequests().get(0).value());
            assertEquals(ActionExecutionStatus.SUCCEEDED, response.getStatus());
            assertEquals(action.getActionUuid().toString(), sender.lastDelete.deleteKVRequests().get(0).key());
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testOpenRecoverActionConsumerDryRunDoesNotExecuteOperation() {
        MockTime time = new MockTime(0L, 100L, 0L);
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_RECREATE,
            AvailabilityTarget.topicPartition("topic", 0), 1, 100L, 200L, "recover", true);
        BrokerAvailabilityService service = new BrokerAvailabilityService(null, null, new BrokerActionReceiver(1),
            new ActionResponsePublisher(sender), () -> List.of(action), null, sender, null, time, 0L, 0L);
        AtomicInteger executions = new AtomicInteger();

        service.start();
        try {
            boolean consumed = AvailabilityRuntimeHooks.consumeOpenRecoverAction(
                AvailabilityActionType.PARTITION_RECREATE, new TopicPartition("topic", 0),
                consumedAction -> executions.incrementAndGet());

            assertFalse(consumed);
            assertEquals(0, executions.get());
            ActionResponse response = AvailabilityCodecs.decodeResponse(sender.lastPut.putKVRequests().get(0).value());
            assertEquals(ActionExecutionStatus.DRY_RUN, response.getStatus());
            assertEquals(action.getActionUuid().toString(), sender.lastDelete.deleteKVRequests().get(0).key());
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testOpenRecoverActionConsumerDeduplicatesByActionUuidBeforeKvDeleteIsVisible() {
        MockTime time = new MockTime(0L, 100L, 0L);
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_RECREATE,
            AvailabilityTarget.topicPartition("topic", 0), 1, 100L, 200L, "recover", false);
        BrokerAvailabilityService service = new BrokerAvailabilityService(null, null, new BrokerActionReceiver(1),
            new ActionResponsePublisher(sender), () -> List.of(action), null, sender, null, time, 0L, 0L);
        AtomicInteger executions = new AtomicInteger();

        service.start();
        try {
            boolean firstConsumed = AvailabilityRuntimeHooks.consumeOpenRecoverAction(
                AvailabilityActionType.PARTITION_RECREATE, new TopicPartition("topic", 0),
                consumedAction -> executions.incrementAndGet());
            boolean secondConsumed = AvailabilityRuntimeHooks.consumeOpenRecoverAction(
                AvailabilityActionType.PARTITION_RECREATE, new TopicPartition("topic", 0),
                consumedAction -> executions.incrementAndGet());

            assertTrue(firstConsumed);
            assertFalse(secondConsumed);
            assertEquals(1, executions.get());
            assertEquals(1, sender.puts);
            assertEquals(1, sender.deletes);
        } finally {
            service.shutdown();
        }
    }

    private RecoveryAction action(int executorBrokerId, long deadlineMs) {
        return new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(executorBrokerId, executorBrokerId * 10L), executorBrokerId, 0L, deadlineMs,
            "test", false);
    }

    private static final class CapturingKvRequestSender implements AvailabilityKvRequestSender {
        private PutKVsRequestData lastPut;
        private DeleteKVsRequestData lastDelete;
        private int puts;
        private int deletes;

        @Override
        public CompletableFuture<Void> put(PutKVsRequestData request) {
            lastPut = request;
            puts++;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> delete(DeleteKVsRequestData request) {
            lastDelete = request;
            deletes++;
            return CompletableFuture.completedFuture(null);
        }
    }
}
