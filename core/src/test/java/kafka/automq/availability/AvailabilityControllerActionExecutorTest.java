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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.executor.ActionExecutorService;
import kafka.automq.availability.controller.AvailabilityPartitionReassignmentAdapter;
import kafka.automq.availability.controller.ControllerActionExecutor;
import kafka.automq.availability.controller.KvBackedAvailabilityActionDispatcher;
import kafka.automq.availability.controller.PartitionReassignmentTargetSelector;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.availability.RecoveryDecision;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class AvailabilityControllerActionExecutorTest {
    private final MockTime time = new MockTime(0L, 100L, 0L);

    @Test
    public void testPartitionReassignmentTargetSelectorExcludesCurrentFencedAndNonDataPathBrokers() {
        PartitionReassignmentTargetSelector selector = new PartitionReassignmentTargetSelector(new Random(1L));

        OptionalInt selected = selector.select(1, List.of(
            new PartitionReassignmentTargetSelector.CandidateBroker(1, true, true),
            new PartitionReassignmentTargetSelector.CandidateBroker(2, false, true),
            new PartitionReassignmentTargetSelector.CandidateBroker(3, true, false),
            new PartitionReassignmentTargetSelector.CandidateBroker(4, true, true)
        ));

        assertTrue(selected.isPresent());
        assertEquals(4, selected.getAsInt());
        assertEquals(List.of(4), PartitionReassignmentTargetSelector.eligibleTargets(1, List.of(
            new PartitionReassignmentTargetSelector.CandidateBroker(1, true, true),
            new PartitionReassignmentTargetSelector.CandidateBroker(2, false, true),
            new PartitionReassignmentTargetSelector.CandidateBroker(3, true, false),
            new PartitionReassignmentTargetSelector.CandidateBroker(4, true, true)
        )).stream().map(PartitionReassignmentTargetSelector.CandidateBroker::brokerId).collect(Collectors.toList()));
    }

    @Test
    public void testPartitionReassignmentAdapterUsesAutoBalancerExecutorPath() {
        CapturingActionExecutorService actionExecutorService = new CapturingActionExecutorService();
        AvailabilityPartitionReassignmentAdapter adapter = new AvailabilityPartitionReassignmentAdapter(
            actionExecutorService,
            new PartitionReassignmentTargetSelector(),
            () -> List.of(
                new PartitionReassignmentTargetSelector.CandidateBroker(1, true, true),
                new PartitionReassignmentTargetSelector.CandidateBroker(2, true, true)
            ),
            topicPartition -> 1
        );

        adapter.execute(partitionReassignmentAction()).join();

        assertEquals(1, actionExecutorService.actions.size());
        Action action = actionExecutorService.actions.get(0);
        assertEquals(ActionType.MOVE, action.getType());
        assertEquals("topic", action.getSrcTopicPartition().topic());
        assertEquals(0, action.getSrcTopicPartition().partition());
        assertEquals(1, action.getSrcBrokerId());
        assertEquals(2, action.getDestBrokerId());
    }

    @Test
    public void testControllerActionExecutorPersistsResponseAndDeduplicatesByUuid() {
        AtomicInteger executions = new AtomicInteger();
        InMemoryControllerActionStateStore store = new InMemoryControllerActionStateStore();
        ControllerActionExecutor executor = new ControllerActionExecutor(Map.of(
            AvailabilityActionType.PARTITION_REASSIGNMENT, action -> {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }), store, time);
        RecoveryAction action = partitionReassignmentAction();

        ActionResponse first = executor.execute(action).join();
        ActionResponse second = executor.execute(action).join();

        assertEquals(ActionExecutionStatus.SUCCEEDED, first.getStatus());
        assertSame(first, second);
        assertEquals(1, executions.get());
        assertEquals(first, store.get(action.getActionUuid()).getResponse());
    }

    @Test
    public void testControllerActionExecutorMapsAdapterFailureToFailedResponse() {
        InMemoryControllerActionStateStore store = new InMemoryControllerActionStateStore();
        ControllerActionExecutor executor = new ControllerActionExecutor(Map.of(
            AvailabilityActionType.PARTITION_REASSIGNMENT, action -> {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new IllegalStateException("boom"));
                return future;
            }), store, time);

        ActionResponse response = executor.execute(partitionReassignmentAction()).join();

        assertEquals(ActionExecutionStatus.FAILED, response.getStatus());
        assertTrue(response.getFailureReason().contains("IllegalStateException"));
    }

    @Test
    public void testControllerActionExecutorRestoresPendingStateAndResumesExecution() {
        AtomicInteger executions = new AtomicInteger();
        InMemoryControllerActionStateStore store = new InMemoryControllerActionStateStore();
        ControllerActionExecutor executor = new ControllerActionExecutor(Map.of(
            AvailabilityActionType.PARTITION_REASSIGNMENT, action -> {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }), store, time);
        RecoveryAction action = partitionReassignmentAction();

        executor.restore(new ControllerActionState(action, null));

        assertEquals(1, executions.get());
        assertEquals(ActionExecutionStatus.SUCCEEDED, store.get(action.getActionUuid()).getResponse().getStatus());
    }

    @Test
    public void testControllerActionExecutorFailsClosedWhenRestoredActionExpired() {
        AtomicInteger executions = new AtomicInteger();
        InMemoryControllerActionStateStore store = new InMemoryControllerActionStateStore();
        ControllerActionExecutor executor = new ControllerActionExecutor(Map.of(
            AvailabilityActionType.PARTITION_REASSIGNMENT, action -> {
                executions.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }), store, time);
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_REASSIGNMENT,
            AvailabilityTarget.topicPartition("topic", 0), -1, 10L, 50L, "test", false);

        ActionResponse response = executor.execute(action).join();

        assertEquals(ActionExecutionStatus.INCOMPLETE, response.getStatus());
        assertTrue(response.getFailureReason().contains("deadline"));
        assertEquals(0, executions.get());
    }

    @Test
    public void testDispatcherAssignsCurrentOwnerToBrokerTopicPartitionAction() {
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        KvBackedAvailabilityActionDispatcher dispatcher = new KvBackedAvailabilityActionDispatcher(sender,
            new ControllerActionExecutor(Map.of(), new InMemoryControllerActionStateStore(), time),
            Set.of(AvailabilityActionType.PARTITION_REASSIGNMENT),
            action -> 3);
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.SKIP_READ_RANGE,
            AvailabilityTarget.topicPartitionOffset("topic", 0, 42L), -1, 100L, 200L, "read", false);

        dispatcher.dispatch(RecoveryDecision.executable(action));

        RecoveryAction stored = AvailabilityCodecs.decodeAction(sender.lastPut.putKVRequests().get(0).value());
        assertEquals(3, stored.getExecutorBrokerId());
        assertEquals(-1, action.getExecutorBrokerId());
    }

    private RecoveryAction partitionReassignmentAction() {
        return new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_REASSIGNMENT,
            AvailabilityTarget.topicPartition("topic", 0), -1, 100L, 200L, "test", false);
    }

    private static final class CapturingActionExecutorService implements ActionExecutorService {
        private final List<Action> actions = new ArrayList<>();

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public CompletableFuture<Void> execute(List<Action> actions) {
            this.actions.addAll(actions);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class InMemoryControllerActionStateStore
        implements ControllerActionExecutor.ControllerActionStateStore {
        private final Map<Uuid, ControllerActionState> states = new ConcurrentHashMap<>();

        @Override
        public ControllerActionState get(Uuid actionUuid) {
            return states.get(actionUuid);
        }

        @Override
        public void put(ControllerActionState state) {
            states.put(state.getAction().getActionUuid(), state);
        }
    }

    private static final class CapturingKvRequestSender implements AvailabilityKvRequestSender {
        private PutKVsRequestData lastPut;

        @Override
        public CompletableFuture<Void> put(PutKVsRequestData request) {
            lastPut = request;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> delete(DeleteKVsRequestData request) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
