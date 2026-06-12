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

import kafka.automq.availability.action.AvailabilityActionExecutor;
import kafka.automq.availability.broker.AvailabilitySignalPublisher;
import kafka.automq.availability.controller.ControllerAvailabilityReconciler;
import kafka.automq.availability.transport.AvailabilityBrokerKvCodec;
import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityAttributionEngine;
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.BlockedReason;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ClusterAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.CoverageBroker;
import org.apache.kafka.controller.availability.FaultAttributionType;
import org.apache.kafka.controller.availability.ProtectionState;
import org.apache.kafka.controller.availability.ProtectionStateManager;
import org.apache.kafka.controller.availability.RecoveryActionPlanner;
import org.apache.kafka.controller.availability.RecoveryDecision;
import org.apache.kafka.controller.availability.RecoveryDecisionType;
import org.apache.kafka.controller.availability.RecoveryPlannerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(40)
public class AvailabilityIntegrationTest {
    private final MockTime time = new MockTime(0L, 100L, 0L);

    @Test
    public void testFakeBrokerPublisherControllerReconcilerAndActionExecutorLoop() {
        BrokerAvailabilityMonitor monitor = new BrokerAvailabilityMonitor(1, 10L, 30L, 30L, 30L, 3,
            time::nanoseconds);
        monitor.recordLogWriteFailed(new org.apache.kafka.common.TopicPartition("topic", 0));
        CapturingKvRequestSender sender = new CapturingKvRequestSender();
        AvailabilitySignalPublisher signalPublisher = new AvailabilitySignalPublisher(monitor, sender, time);
        FakeSignalSource source = new FakeSignalSource();
        signalPublisher.publishOnceAsync().join();
        source.signals = new ArrayList<>(List.of(decodeSignal(sender.lastPut)));
        source.signals.add(emptySnapshot(2));
        List<RecoveryDecision> decisions = new ArrayList<>();
        ControllerAvailabilityReconciler reconciler = reconciler(source, decisions,
            EnumSet.of(AvailabilityActionType.PARTITION_REASSIGNMENT), true);

        assertEquals(RecoveryDecisionType.NOOP, reconciler.reconcileOnce().decision().type());
        RecoveryDecision decision = reconciler.reconcileOnce().decision();
        assertEquals(RecoveryDecisionType.EXECUTABLE, decision.type());
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(
            AvailabilityActionType.PARTITION_REASSIGNMENT,
            (action, nowMs) -> new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
                ActionExecutionStatus.SUCCEEDED, nowMs, nowMs, null)), time);
        ActionResponse response = executor.execute(decision.action());
        PutKVsRequestData responseWrite = AvailabilityBrokerKvCodec.putActionResponseRequest(response);

        assertEquals(AvailabilityConstants.RESPONSE_NAMESPACE, responseWrite.putKVRequests().get(0).namespace());
        assertEquals(ActionExecutionStatus.SUCCEEDED, AvailabilityCodecs.decodeResponse(
            responseWrite.putKVRequests().get(0).value()).getStatus());
        assertEquals(1, decisions.size());
    }

    @Test
    public void testRollingUpgradeIncompatibleStaleAndEpochMismatchFailClosed() {
        AvailabilityAttributionEngine engine = new AvailabilityAttributionEngine(0.4, 2);
        BrokerAvailabilitySnapshot incompatible = emptySnapshot(1);
        incompatible.setSchemaVersion(0);
        BrokerAvailabilitySnapshot stale = emptySnapshot(1);
        stale.setTimestampMs(1L);
        BrokerAvailabilitySnapshot epochMismatch = emptySnapshot(1);
        epochMismatch.setBrokerEpoch(99L);

        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(snapshot(incompatible, 100L)).type());
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(snapshot(stale, 1000L)).type());
        assertEquals(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE,
            engine.attribute(snapshot(epochMismatch, 100L)).type());
        RecoveryDecision blocked = planner(EnumSet.allOf(AvailabilityActionType.class), true)
            .plan(engine.attribute(snapshot(incompatible, 100L)), ProtectionState.none(), 100L);
        assertEquals(RecoveryDecisionType.BLOCKED, blocked.type());
        assertEquals(BlockedReason.INSUFFICIENT_SIGNAL_COVERAGE, blocked.blockedReason());
    }

    private ControllerAvailabilityReconciler reconciler(FakeSignalSource source, List<RecoveryDecision> decisions,
                                                       EnumSet<AvailabilityActionType> allowlist, boolean enabled) {
        return new ControllerAvailabilityReconciler(source,
            () -> List.of(new CoverageBroker(1, 10L), new CoverageBroker(2, 20L)),
            new AvailabilityAttributionEngine(0.4, 2),
            new ProtectionStateManager(),
            planner(allowlist, enabled),
            decisions::add,
            (action, response) -> { },
            ignored -> { },
            time,
            90_000L);
    }

    private RecoveryActionPlanner planner(EnumSet<AvailabilityActionType> allowlist, boolean enabled) {
        return new RecoveryActionPlanner(new RecoveryPlannerConfig(enabled, allowlist, 2,
            600_000L, 300_000L, 1, 1, 120_000L));
    }

    private BrokerAvailabilitySnapshot decodeSignal(PutKVsRequestData request) {
        return AvailabilityCodecs.decodeSignal(request.putKVRequests().get(0).value());
    }

    private BrokerAvailabilitySnapshot emptySnapshot(int brokerId) {
        return new BrokerAvailabilitySnapshot(brokerId, brokerId * 10L, 100L, 70L, 100L, List.of());
    }

    private ClusterAvailabilitySnapshot snapshot(BrokerAvailabilitySnapshot brokerSnapshot, long nowMs) {
        return new ClusterAvailabilitySnapshot(List.of(new CoverageBroker(1, 10L)),
            Map.of(1, brokerSnapshot), nowMs, 90L);
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

    private static final class FakeSignalSource implements ControllerAvailabilityReconciler.SignalSource {
        private List<BrokerAvailabilitySnapshot> signals = new ArrayList<>();

        @Override
        public List<BrokerAvailabilitySnapshot> collectSignals() {
            return signals;
        }

        @Override
        public List<ActionResponse> collectActionResponses() {
            return List.of();
        }

        @Override
        public List<ControllerActionState> collectControllerActionStates() {
            return List.of();
        }
    }
}
