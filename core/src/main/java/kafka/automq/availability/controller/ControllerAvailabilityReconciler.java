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

package kafka.automq.availability.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityAttributionEngine;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ClusterAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.CoverageBroker;
import org.apache.kafka.controller.availability.FaultAttribution;
import org.apache.kafka.controller.availability.ProtectionState;
import org.apache.kafka.controller.availability.ProtectionStateManager;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.RecoveryActionPlanner;
import org.apache.kafka.controller.availability.RecoveryDecision;
import org.apache.kafka.controller.availability.RecoveryDecisionType;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Coordinates Controller-side availability reconciliation while keeping KV, liveness and action execution adapters
 * outside the pure attribution/planner state machines.
 */
public class ControllerAvailabilityReconciler {
    private final SignalSource signalSource;
    private final Supplier<List<CoverageBroker>> coverageBrokerSupplier;
    private final AvailabilityAttributionEngine attributionEngine;
    private final ProtectionStateManager protectionStateManager;
    private final RecoveryActionPlanner actionPlanner;
    private final ActionDispatcher actionDispatcher;
    private final ExpiredActionHandler expiredActionHandler;
    private final ControllerActionStateRestorer controllerActionStateRestorer;
    private final Cleanup cleanup;
    private final Time time;
    private final long staleMs;
    private final Set<Uuid> consumedResponseIds = new HashSet<>();
    private final Set<Uuid> restoredControllerActionIds = new HashSet<>();

    public ControllerAvailabilityReconciler(SignalSource signalSource,
                                            Supplier<List<CoverageBroker>> coverageBrokerSupplier,
                                            AvailabilityAttributionEngine attributionEngine,
                                            ProtectionStateManager protectionStateManager,
                                            RecoveryActionPlanner actionPlanner,
                                            ActionDispatcher actionDispatcher,
                                            ExpiredActionHandler expiredActionHandler,
                                            Cleanup cleanup,
                                            Time time,
                                            long staleMs) {
        this(signalSource, coverageBrokerSupplier, attributionEngine, protectionStateManager, actionPlanner,
            actionDispatcher, expiredActionHandler, state -> { }, cleanup, time, staleMs);
    }

    public ControllerAvailabilityReconciler(SignalSource signalSource,
                                            Supplier<List<CoverageBroker>> coverageBrokerSupplier,
                                            AvailabilityAttributionEngine attributionEngine,
                                            ProtectionStateManager protectionStateManager,
                                            RecoveryActionPlanner actionPlanner,
                                            ActionDispatcher actionDispatcher,
                                            ExpiredActionHandler expiredActionHandler,
                                            ControllerActionStateRestorer controllerActionStateRestorer,
                                            Cleanup cleanup,
                                            Time time,
                                            long staleMs) {
        this.signalSource = signalSource;
        this.coverageBrokerSupplier = coverageBrokerSupplier;
        this.attributionEngine = attributionEngine;
        this.protectionStateManager = protectionStateManager;
        this.actionPlanner = actionPlanner;
        this.actionDispatcher = actionDispatcher;
        this.expiredActionHandler = expiredActionHandler;
        this.controllerActionStateRestorer = controllerActionStateRestorer;
        this.cleanup = cleanup;
        this.time = time;
        this.staleMs = staleMs;
    }

    public ReconcileResult reconcileOnce() {
        long nowMs = time.milliseconds();
        restoreRetainedActions(nowMs);
        int consumedResponses = consumeCompletedResponses();
        FaultAttribution attribution = attributionEngine.attribute(snapshot(nowMs));
        ProtectionState protectionState = protectionStateManager.update(attribution, nowMs);
        RecoveryDecision decision = actionPlanner.plan(attribution, protectionState, nowMs);
        dispatchDecision(decision);
        cleanup.cleanup(nowMs);
        return new ReconcileResult(attribution, protectionState, decision, consumedResponses);
    }

    private void restoreRetainedActions(long nowMs) {
        for (RecoveryAction action : signalSource.collectBrokerActions()) {
            restoreRetainedAction(action, nowMs);
        }
        for (ControllerActionState state : signalSource.collectControllerActionStates()) {
            if (state.getAction() != null && state.getResponse() == null) {
                if (restoredControllerActionIds.add(state.getAction().getActionUuid())) {
                    controllerActionStateRestorer.restore(state);
                }
                restoreRetainedAction(state.getAction(), nowMs);
            }
        }
    }

    private void restoreRetainedAction(RecoveryAction action, long nowMs) {
        if (nowMs > action.getDeadlineMs()) {
            ActionResponse response = new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
                ActionExecutionStatus.INCOMPLETE, action.getCreatedAtMs(), nowMs,
                "action deadline expired before response was observed");
            if (!consumedResponseIds.contains(response.getActionUuid())) {
                expiredActionHandler.onExpiredAction(action, response);
                consumeResponse(response);
            }
            return;
        }
        actionPlanner.onActionDispatched(action);
    }

    private int consumeCompletedResponses() {
        int consumed = 0;
        for (ActionResponse response : signalSource.collectActionResponses()) {
            if (consumeResponse(response)) {
                consumed++;
            }
        }
        for (ControllerActionState state : signalSource.collectControllerActionStates()) {
            if (state.getResponse() != null && consumeResponse(state.getResponse())) {
                consumed++;
            }
        }
        return consumed;
    }

    private boolean consumeResponse(ActionResponse response) {
        if (response.getActionUuid() == null || !consumedResponseIds.add(response.getActionUuid())) {
            return false;
        }
        actionPlanner.onActionResponse(response);
        return true;
    }

    private ClusterAvailabilitySnapshot snapshot(long nowMs) {
        Map<Integer, BrokerAvailabilitySnapshot> snapshotsByBroker = new HashMap<>();
        for (BrokerAvailabilitySnapshot snapshot : signalSource.collectSignals()) {
            snapshotsByBroker.merge(snapshot.getBrokerId(), snapshot,
                (left, right) -> left.getTimestampMs() >= right.getTimestampMs() ? left : right);
        }
        return new ClusterAvailabilitySnapshot(coverageBrokerSupplier.get(), snapshotsByBroker, nowMs, staleMs);
    }

    private void dispatchDecision(RecoveryDecision decision) {
        if (decision.type() == RecoveryDecisionType.EXECUTABLE || decision.type() == RecoveryDecisionType.DRY_RUN) {
            actionDispatcher.dispatch(decision);
        }
    }

    public interface SignalSource {
        List<BrokerAvailabilitySnapshot> collectSignals();

        List<ActionResponse> collectActionResponses();

        List<ControllerActionState> collectControllerActionStates();

        default List<RecoveryAction> collectBrokerActions() {
            return List.of();
        }
    }

    public interface ActionDispatcher {
        void dispatch(RecoveryDecision decision);
    }

    public interface ExpiredActionHandler {
        void onExpiredAction(RecoveryAction action, ActionResponse response);
    }

    public interface ControllerActionStateRestorer {
        void restore(ControllerActionState state);
    }

    public interface Cleanup {
        void cleanup(long nowMs);
    }

    public static class ReconcileResult {
        private final FaultAttribution attribution;
        private final ProtectionState protectionState;
        private final RecoveryDecision decision;
        private final int consumedResponses;

        public ReconcileResult(FaultAttribution attribution, ProtectionState protectionState,
                               RecoveryDecision decision, int consumedResponses) {
            this.attribution = attribution;
            this.protectionState = protectionState;
            this.decision = decision;
            this.consumedResponses = consumedResponses;
        }

        public FaultAttribution attribution() {
            return attribution;
        }

        public ProtectionState protectionState() {
            return protectionState;
        }

        public RecoveryDecision decision() {
            return decision;
        }

        public int consumedResponses() {
            return consumedResponses;
        }
    }
}
