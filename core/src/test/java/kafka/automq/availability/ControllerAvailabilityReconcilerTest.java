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

import kafka.automq.availability.controller.ControllerAvailabilityReconciler;
import kafka.automq.availability.controller.ControllerAvailabilityService;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityAttributionEngine;
import org.apache.kafka.controller.availability.AvailabilitySignal;
import org.apache.kafka.controller.availability.AvailabilitySignalType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.BlockedReason;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.CoverageBroker;
import org.apache.kafka.controller.availability.FaultAttributionType;
import org.apache.kafka.controller.availability.ProtectionStateManager;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.availability.RecoveryActionPlanner;
import org.apache.kafka.controller.availability.RecoveryDecision;
import org.apache.kafka.controller.availability.RecoveryDecisionType;
import org.apache.kafka.controller.availability.RecoveryPlannerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class ControllerAvailabilityReconcilerTest {
    private final MockTime time = new MockTime(0L, 100L, 0L);
    private final FakeSignalSource signalSource = new FakeSignalSource();
    private final List<RecoveryDecision> dispatched = new ArrayList<>();
    private final List<ActionResponse> expiredResponses = new ArrayList<>();
    private long lastCleanupMs = -1L;

    @Test
    public void testConsecutiveAttributionDispatchesExecutableAction() {
        signalSource.signals = List.of(
            nodeSnapshot(1, AvailabilitySignalType.APPEND_STUCK),
            emptySnapshot(2),
            emptySnapshot(3)
        );

        ControllerAvailabilityReconciler reconciler = reconciler(defaultPlannerConfig());
        ControllerAvailabilityReconciler.ReconcileResult first = reconciler.reconcileOnce();
        ControllerAvailabilityReconciler.ReconcileResult second = reconciler.reconcileOnce();

        assertEquals(RecoveryDecisionType.NOOP, first.decision().type());
        assertEquals(FaultAttributionType.NODE_LOCAL_FAILURE, second.attribution().type());
        assertEquals(RecoveryDecisionType.EXECUTABLE, second.decision().type());
        assertEquals(AvailabilityActionType.NODE_EXIT, second.decision().action().getActionType());
        assertEquals(1, dispatched.size());
        assertEquals(100L, lastCleanupMs);
    }

    @Test
    public void testInsufficientCoverageBlocksWithoutDispatch() {
        signalSource.signals = List.of(
            nodeSnapshot(1, AvailabilitySignalType.APPEND_STUCK),
            emptySnapshot(2)
        );

        ControllerAvailabilityReconciler.ReconcileResult result = reconciler(defaultPlannerConfig()).reconcileOnce();

        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE, result.attribution().type());
        assertEquals(RecoveryDecisionType.BLOCKED, result.decision().type());
        assertEquals(BlockedReason.INSUFFICIENT_SIGNAL_COVERAGE, result.decision().blockedReason());
        assertTrue(dispatched.isEmpty());
    }

    @Test
    public void testAllowlistMissDispatchesDryRunDecision() {
        signalSource.signals = List.of(
            nodeSnapshot(1, AvailabilitySignalType.APPEND_STUCK),
            emptySnapshot(2),
            emptySnapshot(3)
        );
        RecoveryPlannerConfig config = new RecoveryPlannerConfig(true, EnumSet.noneOf(AvailabilityActionType.class),
            1, 600_000L, 300_000L, 1, 1, 120_000L);

        ControllerAvailabilityReconciler.ReconcileResult result = reconciler(config).reconcileOnce();

        assertEquals(RecoveryDecisionType.DRY_RUN, result.decision().type());
        assertTrue(result.decision().action().isDryRun());
        assertEquals(1, dispatched.size());
    }

    @Test
    public void testRecoveredResponsesAreConsumedOnceFromBrokerAndControllerState() {
        ActionResponse response = new ActionResponse(Uuid.randomUuid(), AvailabilityActionType.PARTITION_REASSIGNMENT,
            AvailabilityTarget.topicPartition("topic", 0), ActionExecutionStatus.SUCCEEDED, 10L, 20L, null);
        signalSource.responses = List.of(response);
        signalSource.controllerStates = List.of(new ControllerActionState(null, response));
        signalSource.signals = List.of(
            partitionSnapshot(1, AvailabilitySignalType.PARTITION_RECOVER_FAILED),
            emptySnapshot(2),
            emptySnapshot(3)
        );

        ControllerAvailabilityReconciler reconciler = reconciler(defaultPlannerConfig());
        ControllerAvailabilityReconciler.ReconcileResult first = reconciler.reconcileOnce();
        ControllerAvailabilityReconciler.ReconcileResult second = reconciler.reconcileOnce();

        assertEquals(1, first.consumedResponses());
        assertEquals(0, second.consumedResponses());
        assertEquals(RecoveryDecisionType.NOOP, first.decision().type());
        assertEquals(RecoveryDecisionType.EXECUTABLE, second.decision().type());
        assertEquals(AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY, second.decision().action().getActionType());
    }

    @Test
    public void testRetainedPendingBrokerActionRestoresPlannerInflightAfterFailover() {
        RecoveryAction retained = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(1, 10L), 1, 50L, 10_000L, "retained", false);
        signalSource.actions = List.of(retained);
        signalSource.signals = List.of(
            nodeSnapshot(1, AvailabilitySignalType.APPEND_STUCK),
            emptySnapshot(2),
            emptySnapshot(3)
        );

        ControllerAvailabilityReconciler reconciler = reconciler(defaultPlannerConfig());
        reconciler.reconcileOnce();
        ControllerAvailabilityReconciler.ReconcileResult second = reconciler.reconcileOnce();

        assertEquals(RecoveryDecisionType.BLOCKED, second.decision().type());
        assertEquals(BlockedReason.CONCURRENCY_LIMIT, second.decision().blockedReason());
        assertTrue(dispatched.isEmpty());
    }

    @Test
    public void testExpiredBrokerActionPublishesIncompleteAndReleasesInflight() {
        RecoveryAction expired = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(1, 10L), 1, 50L, 90L, "expired", false);
        signalSource.actions = List.of(expired);
        signalSource.signals = List.of(
            nodeSnapshot(1, AvailabilitySignalType.APPEND_STUCK),
            emptySnapshot(2),
            emptySnapshot(3)
        );

        RecoveryPlannerConfig config = new RecoveryPlannerConfig(true, EnumSet.allOf(AvailabilityActionType.class), 1,
            1L, 300_000L, 1, 1, 120_000L);
        ControllerAvailabilityReconciler reconciler = reconciler(config);
        ControllerAvailabilityReconciler.ReconcileResult first = reconciler.reconcileOnce();
        signalSource.actions = List.of();
        time.sleep(2L);
        ControllerAvailabilityReconciler.ReconcileResult second = reconciler.reconcileOnce();

        assertEquals(0, first.consumedResponses());
        assertEquals(1, expiredResponses.size());
        assertEquals(ActionExecutionStatus.INCOMPLETE, expiredResponses.get(0).getStatus());
        assertEquals(RecoveryDecisionType.EXECUTABLE, second.decision().type());
        assertEquals(AvailabilityActionType.NODE_EXIT, second.decision().action().getActionType());
    }

    @Test
    public void testControllerAvailabilityServiceLifecycle() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            ControllerAvailabilityService service = new ControllerAvailabilityService(reconciler(defaultPlannerConfig()),
                executor, 45_000L);

            assertFalse(service.isRunning());
            service.start();
            assertTrue(service.isRunning());
            service.shutdown();
            assertFalse(service.isRunning());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testControllerAvailabilityServiceRunsOnlyOnActiveController() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AtomicBoolean active = new AtomicBoolean(false);
        try {
            ControllerAvailabilityService service = new ControllerAvailabilityService(reconciler(defaultPlannerConfig()),
                executor, 45_000L, active::get, () -> { });

            service.start();
            assertTrue(service.isRunning());
            service.runOnceForTest();
            assertEquals(-1L, lastCleanupMs);

            active.set(true);
            service.runOnceForTest();
            assertEquals(time.milliseconds(), lastCleanupMs);
            service.shutdown();
        } finally {
            executor.shutdownNow();
        }
    }

    private ControllerAvailabilityReconciler reconciler(RecoveryPlannerConfig config) {
        return new ControllerAvailabilityReconciler(
            signalSource,
            () -> List.of(new CoverageBroker(1, 10L), new CoverageBroker(2, 20L), new CoverageBroker(3, 30L)),
            new AvailabilityAttributionEngine(0.4, 2),
            new ProtectionStateManager(),
            new RecoveryActionPlanner(config),
            dispatched::add,
            (action, response) -> expiredResponses.add(response),
            nowMs -> lastCleanupMs = nowMs,
            time,
            90_000L
        );
    }

    private RecoveryPlannerConfig defaultPlannerConfig() {
        return new RecoveryPlannerConfig(true, EnumSet.allOf(AvailabilityActionType.class), 2,
            600_000L, 300_000L, 1, 1, 120_000L);
    }

    private BrokerAvailabilitySnapshot emptySnapshot(int brokerId) {
        return new BrokerAvailabilitySnapshot(brokerId, brokerId * 10L, 100L, 70L, 100L, List.of());
    }

    private BrokerAvailabilitySnapshot nodeSnapshot(int brokerId, AvailabilitySignalType signalType) {
        return new BrokerAvailabilitySnapshot(brokerId, brokerId * 10L, 100L, 70L, 100L,
            List.of(new AvailabilitySignal(signalType, AvailabilityTarget.broker(brokerId, brokerId * 10L))));
    }

    private BrokerAvailabilitySnapshot partitionSnapshot(int brokerId, AvailabilitySignalType signalType) {
        return new BrokerAvailabilitySnapshot(brokerId, brokerId * 10L, 100L, 70L, 100L,
            List.of(new AvailabilitySignal(signalType, AvailabilityTarget.topicPartition("topic", 0))));
    }

    private static final class FakeSignalSource implements ControllerAvailabilityReconciler.SignalSource {
        private List<BrokerAvailabilitySnapshot> signals = List.of();
        private List<ActionResponse> responses = List.of();
        private List<ControllerActionState> controllerStates = List.of();
        private List<RecoveryAction> actions = List.of();

        @Override
        public List<BrokerAvailabilitySnapshot> collectSignals() {
            return signals;
        }

        @Override
        public List<ActionResponse> collectActionResponses() {
            return responses;
        }

        @Override
        public List<ControllerActionState> collectControllerActionStates() {
            return controllerStates;
        }

        @Override
        public List<RecoveryAction> collectBrokerActions() {
            return actions;
        }
    }
}
