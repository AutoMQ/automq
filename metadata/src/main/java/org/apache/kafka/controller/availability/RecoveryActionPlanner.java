/*
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

package org.apache.kafka.controller.availability;

import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class RecoveryActionPlanner {
    private final Supplier<RecoveryPlannerConfig> configSupplier;
    private final Map<AttributionKey, Integer> attributionCounts = new HashMap<>();
    private final Map<ActionKey, Long> cooldownUntilMs = new HashMap<>();
    private final Map<ActionKey, ActionResponse> successfulResponses = new HashMap<>();
    private final Map<AvailabilityActionType, Integer> inflightByActionType = new HashMap<>();
    private final Map<AvailabilityTarget, Integer> inflightByBrokerTarget = new HashMap<>();
    private final Set<Uuid> inflightActionIds = new HashSet<>();

    public RecoveryActionPlanner(RecoveryPlannerConfig config) {
        this(() -> config);
    }

    public RecoveryActionPlanner(Supplier<RecoveryPlannerConfig> configSupplier) {
        this.configSupplier = configSupplier;
    }

    public RecoveryDecision plan(FaultAttribution attribution, ProtectionState protectionState, long nowMs) {
        RecoveryDecision preGate = preGate(attribution, protectionState, nowMs);
        if (preGate != null) {
            return preGate;
        }
        if (!isConsecutiveThresholdReached(attribution)) {
            return RecoveryDecision.noop();
        }

        AvailabilityActionType actionType = nextAction(attribution);
        if (actionType == null) {
            return RecoveryDecision.noop();
        }
        RecoveryDecision actionGate = actionGate(actionType, attribution.target(), nowMs);
        if (actionGate != null) {
            return actionGate;
        }

        return createActionDecision(actionType, attribution, nowMs);
    }

    public void onActionResponse(ActionResponse response) {
        RecoveryPlannerConfig config = config();
        ActionKey actionKey = new ActionKey(response.getActionType(), response.getTarget());
        decrementInflight(response);
        attributionCounts.clear();
        long cooldownMs = response.getTarget().getKind() == AvailabilityTarget.Kind.BROKER ?
            config.nodeCooldownMs() : config.partitionLogCooldownMs();
        cooldownUntilMs.put(actionKey, response.getCompletedAtMs() + cooldownMs);
        if (response.isSuccessfulExecution() && response.getActionType() != AvailabilityActionType.NODE_EXIT) {
            successfulResponses.put(actionKey, response);
        }
    }

    public void onActionDispatched(RecoveryAction action) {
        if (action == null || action.isDryRun() || !inflightActionIds.add(action.getActionUuid())) {
            return;
        }
        inflightByActionType.merge(action.getActionType(), 1, Integer::sum);
        if (action.getTarget().getKind() == AvailabilityTarget.Kind.BROKER) {
            inflightByBrokerTarget.merge(action.getTarget(), 1, Integer::sum);
        }
    }

    private RecoveryDecision preGate(FaultAttribution attribution, ProtectionState protectionState, long nowMs) {
        RecoveryPlannerConfig config = config();
        if (attribution.type() == FaultAttributionType.NONE) {
            attributionCounts.clear();
            return RecoveryDecision.noop();
        }
        if (attribution.type() == FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE) {
            return RecoveryDecision.blocked(BlockedReason.INSUFFICIENT_SIGNAL_COVERAGE, nowMs);
        }
        if (protectionState.globalProtected()) {
            return RecoveryDecision.blocked(BlockedReason.GLOBAL_PROTECTION, nowMs);
        }
        if (!config.fallbackEnabled()) {
            return RecoveryDecision.blocked(BlockedReason.FALLBACK_DISABLED, nowMs);
        }
        return null;
    }

    private boolean isConsecutiveThresholdReached(FaultAttribution attribution) {
        int count = incrementAttribution(attribution);
        return count >= config().consecutiveAttributionThreshold();
    }

    private RecoveryDecision actionGate(AvailabilityActionType actionType, AvailabilityTarget target, long nowMs) {
        RecoveryPlannerConfig config = config();
        ActionKey actionKey = new ActionKey(actionType, target);
        Long cooldownUntil = cooldownUntilMs.get(actionKey);
        if (cooldownUntil != null && nowMs < cooldownUntil) {
            return RecoveryDecision.blocked(BlockedReason.COOLDOWN, cooldownUntil);
        }
        if (inflightByActionType.getOrDefault(actionType, 0) >= config.clusterActionConcurrency()) {
            return RecoveryDecision.blocked(BlockedReason.CONCURRENCY_LIMIT, nowMs);
        }
        if (target.getKind() == AvailabilityTarget.Kind.BROKER &&
            inflightByBrokerTarget.getOrDefault(target, 0) >= config.brokerActionConcurrency()) {
            return RecoveryDecision.blocked(BlockedReason.CONCURRENCY_LIMIT, nowMs);
        }
        return null;
    }

    private RecoveryDecision createActionDecision(AvailabilityActionType actionType, FaultAttribution attribution, long nowMs) {
        RecoveryPlannerConfig config = config();
        boolean dryRun = !config.actionAllowlist().contains(actionType);
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), actionType, attribution.target(), executorBrokerId(attribution),
            nowMs, nowMs + config.actionDeadlineMs(), attribution.reason(), dryRun);
        if (dryRun) {
            onActionResponse(new ActionResponse(action.getActionUuid(), actionType, attribution.target(),
                ActionExecutionStatus.DRY_RUN, nowMs, nowMs, null));
            return RecoveryDecision.dryRun(action);
        }
        onActionDispatched(action);
        return RecoveryDecision.executable(action);
    }

    private int incrementAttribution(FaultAttribution attribution) {
        AttributionKey key = new AttributionKey(attribution.type(), attribution.target(), attribution.sourceSignalType());
        attributionCounts.keySet().removeIf(existing -> !existing.equals(key));
        int count = attributionCounts.getOrDefault(key, 0) + 1;
        attributionCounts.put(key, count);
        return count;
    }

    private AvailabilityActionType nextAction(FaultAttribution attribution) {
        switch (attribution.type()) {
            case NODE_LOCAL_FAILURE:
                return AvailabilityActionType.NODE_EXIT;
            case PARTITION_RECOVER_FAILED:
                return nextPartitionRecoverAction(attribution);
            case LOG_WRITE_FAIL:
                return hasSuccessful(AvailabilityActionType.PARTITION_REASSIGNMENT, attribution) ?
                    AvailabilityActionType.SEGMENT_ROLL : AvailabilityActionType.PARTITION_REASSIGNMENT;
            case LOG_READ_FAIL:
                return hasSuccessful(AvailabilityActionType.PARTITION_REASSIGNMENT, attribution) ?
                    AvailabilityActionType.SKIP_READ_RANGE : AvailabilityActionType.PARTITION_REASSIGNMENT;
            default:
                return null;
        }
    }

    private AvailabilityActionType nextPartitionRecoverAction(FaultAttribution attribution) {
        if (hasSuccessful(AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY, attribution)) {
            return AvailabilityActionType.PARTITION_RECREATE;
        }
        if (hasSuccessful(AvailabilityActionType.PARTITION_REASSIGNMENT, attribution)) {
            return AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY;
        }
        return AvailabilityActionType.PARTITION_REASSIGNMENT;
    }

    private boolean hasSuccessful(AvailabilityActionType actionType, FaultAttribution attribution) {
        return successfulResponses.containsKey(new ActionKey(actionType, attribution.target()));
    }

    private int executorBrokerId(FaultAttribution attribution) {
        if (attribution.target().getKind() == AvailabilityTarget.Kind.BROKER) {
            return attribution.target().getBrokerId();
        }
        return -1;
    }

    private RecoveryPlannerConfig config() {
        return configSupplier.get();
    }

    private void decrementInflight(ActionResponse response) {
        if (!inflightActionIds.remove(response.getActionUuid())) {
            return;
        }
        inflightByActionType.computeIfPresent(response.getActionType(), (ignored, count) -> Math.max(0, count - 1));
        if (response.getTarget().getKind() == AvailabilityTarget.Kind.BROKER) {
            inflightByBrokerTarget.computeIfPresent(response.getTarget(), (ignored, count) -> Math.max(0, count - 1));
        }
    }

    private static final class AttributionKey {
        private final FaultAttributionType type;
        private final AvailabilityTarget target;
        private final AvailabilitySignalType sourceSignalType;

        private AttributionKey(FaultAttributionType type, AvailabilityTarget target,
                               AvailabilitySignalType sourceSignalType) {
            this.type = type;
            this.target = target;
            this.sourceSignalType = sourceSignalType;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof AttributionKey)) {
                return false;
            }
            AttributionKey that = (AttributionKey) o;
            return type == that.type &&
                Objects.equals(target, that.target) &&
                sourceSignalType == that.sourceSignalType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, target, sourceSignalType);
        }
    }

    private static final class ActionKey {
        private final AvailabilityActionType actionType;
        private final AvailabilityTarget target;

        private ActionKey(AvailabilityActionType actionType, AvailabilityTarget target) {
            this.actionType = actionType;
            this.target = target;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ActionKey)) {
                return false;
            }
            ActionKey that = (ActionKey) o;
            return actionType == that.actionType && Objects.equals(target, that.target);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actionType, target);
        }
    }
}
