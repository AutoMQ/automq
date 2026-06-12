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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class AvailabilityRecoveryActionPlannerTest {
    @Test
    public void testPlannerWaitsForTwoConsecutiveAttributions() {
        RecoveryActionPlanner planner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true);
        FaultAttribution attribution = nodeAttribution();

        assertEquals(RecoveryDecisionType.NOOP, planner.plan(attribution, ProtectionState.none(), 100L).type());
        RecoveryDecision decision = planner.plan(attribution, ProtectionState.none(), 145L);

        assertEquals(RecoveryDecisionType.EXECUTABLE, decision.type());
        assertEquals(AvailabilityActionType.NODE_EXIT, decision.action().getActionType());
    }

    @Test
    public void testFallbackDisabledAndInsufficientCoverageAreBlockedNotDryRun() {
        RecoveryActionPlanner disabledPlanner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), false);

        RecoveryDecision disabled = disabledPlanner.plan(nodeAttribution(), ProtectionState.none(), 100L);
        RecoveryDecision insufficient = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true).plan(
            new FaultAttribution(FaultAttributionType.INSUFFICIENT_SIGNAL_COVERAGE, AvailabilityTarget.cluster(), null,
                "missing"), ProtectionState.none(), 100L);

        assertEquals(RecoveryDecisionType.BLOCKED, disabled.type());
        assertEquals(BlockedReason.FALLBACK_DISABLED, disabled.blockedReason());
        assertEquals(RecoveryDecisionType.BLOCKED, insufficient.type());
        assertEquals(BlockedReason.INSUFFICIENT_SIGNAL_COVERAGE, insufficient.blockedReason());
    }

    @Test
    public void testAllowlistMissCreatesDryRunActionResponseCandidate() {
        RecoveryActionPlanner planner = planner(EnumSet.noneOf(AvailabilityActionType.class), true);
        FaultAttribution attribution = nodeAttribution();

        planner.plan(attribution, ProtectionState.none(), 100L);
        RecoveryDecision decision = planner.plan(attribution, ProtectionState.none(), 145L);

        assertEquals(RecoveryDecisionType.DRY_RUN, decision.type());
        assertTrue(decision.action().isDryRun());
        assertEquals(AvailabilityActionType.NODE_EXIT, decision.action().getActionType());

        assertEquals(RecoveryDecisionType.NOOP, planner.plan(attribution, ProtectionState.none(), 190L).type());
        RecoveryDecision repeated = planner.plan(attribution, ProtectionState.none(), 235L);
        assertEquals(RecoveryDecisionType.BLOCKED, repeated.type());
        assertEquals(BlockedReason.COOLDOWN, repeated.blockedReason());
    }

    @Test
    public void testDynamicFallbackEnabledAndAllowlistAffectNextPlan() {
        AtomicReference<RecoveryPlannerConfig> config = new AtomicReference<>(
            config(EnumSet.of(AvailabilityActionType.NODE_EXIT), false));
        RecoveryActionPlanner planner = new RecoveryActionPlanner(config::get);

        assertEquals(RecoveryDecisionType.BLOCKED,
            planner.plan(nodeAttribution(), ProtectionState.none(), 100L).type());

        config.set(config(EnumSet.noneOf(AvailabilityActionType.class), true));
        planner.plan(nodeAttribution(), ProtectionState.none(), 145L);
        RecoveryDecision dryRun = planner.plan(nodeAttribution(), ProtectionState.none(), 190L);
        assertEquals(RecoveryDecisionType.DRY_RUN, dryRun.type());
        assertTrue(dryRun.action().isDryRun());

        config.set(config(EnumSet.of(AvailabilityActionType.NODE_EXIT), true));
        planner.plan(nodeAttribution(), ProtectionState.none(), 800_000L);
        RecoveryDecision executable = planner.plan(nodeAttribution(), ProtectionState.none(), 800_045L);
        assertEquals(RecoveryDecisionType.EXECUTABLE, executable.type());
        assertFalse(executable.action().isDryRun());
    }

    @Test
    public void testGlobalProtectionBlocksAllActions() {
        RecoveryActionPlanner planner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true);

        RecoveryDecision decision = planner.plan(nodeAttribution(),
            new ProtectionState(true, BlockedReason.GLOBAL_PROTECTION, 100L), 145L);

        assertEquals(RecoveryDecisionType.BLOCKED, decision.type());
        assertEquals(BlockedReason.GLOBAL_PROTECTION, decision.blockedReason());
    }

    @Test
    public void testPartitionRecoverEscalatesOnlyAfterSuccessfulExecutedReassignment() {
        RecoveryActionPlanner planner = planner(EnumSet.allOf(AvailabilityActionType.class), true);
        FaultAttribution attribution = partitionAttribution();

        planner.plan(attribution, ProtectionState.none(), 100L);
        RecoveryDecision first = planner.plan(attribution, ProtectionState.none(), 145L);
        assertEquals(AvailabilityActionType.PARTITION_REASSIGNMENT, first.action().getActionType());

        planner.onActionResponse(new ActionResponse(first.action().getActionUuid(), first.action().getActionType(),
            first.action().getTarget(), ActionExecutionStatus.SUCCEEDED, 150L, 160L, null));

        planner.plan(attribution, ProtectionState.none(), 500000L);
        RecoveryDecision second = planner.plan(attribution, ProtectionState.none(), 500045L);
        assertEquals(AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY, second.action().getActionType());
    }

    @Test
    public void testDryRunFailedAndNodeExitSuccessDoNotDriveHighRiskUpgrade() {
        RecoveryActionPlanner dryRunPlanner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true);
        FaultAttribution partition = partitionAttribution();
        dryRunPlanner.plan(partition, ProtectionState.none(), 100L);
        RecoveryDecision dryRun = dryRunPlanner.plan(partition, ProtectionState.none(), 145L);
        assertEquals(RecoveryDecisionType.DRY_RUN, dryRun.type());
        dryRunPlanner.onActionResponse(new ActionResponse(dryRun.action().getActionUuid(), dryRun.action().getActionType(),
            dryRun.action().getTarget(), ActionExecutionStatus.DRY_RUN, 145L, 145L, null));
        dryRunPlanner.plan(partition, ProtectionState.none(), 500000L);
        assertEquals(AvailabilityActionType.PARTITION_REASSIGNMENT,
            dryRunPlanner.plan(partition, ProtectionState.none(), 500045L).action().getActionType());

        RecoveryActionPlanner nodePlanner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true);
        FaultAttribution node = nodeAttribution();
        nodePlanner.plan(node, ProtectionState.none(), 100L);
        RecoveryDecision nodeDecision = nodePlanner.plan(node, ProtectionState.none(), 145L);
        nodePlanner.onActionResponse(new ActionResponse(nodeDecision.action().getActionUuid(), AvailabilityActionType.NODE_EXIT,
            nodeDecision.action().getTarget(), ActionExecutionStatus.SUCCEEDED, 150L, 160L, null));
        nodePlanner.plan(node, ProtectionState.none(), 700000L);
        RecoveryDecision replanned = nodePlanner.plan(node, ProtectionState.none(), 700045L);
        assertEquals(AvailabilityActionType.NODE_EXIT, replanned.action().getActionType());
        assertFalse(replanned.action().isDryRun());
    }

    @Test
    public void testRetainedDispatchedActionRestoresInflightConcurrencyUntilResponse() {
        RecoveryActionPlanner planner = planner(EnumSet.of(AvailabilityActionType.NODE_EXIT), true);
        RecoveryAction retained = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(1, 10L), 1, 100L, 10_000L, "retained", false);
        planner.onActionDispatched(retained);

        planner.plan(nodeAttribution(), ProtectionState.none(), 200L);
        RecoveryDecision blocked = planner.plan(nodeAttribution(), ProtectionState.none(), 245L);
        assertEquals(RecoveryDecisionType.BLOCKED, blocked.type());
        assertEquals(BlockedReason.CONCURRENCY_LIMIT, blocked.blockedReason());

        planner.onActionResponse(new ActionResponse(retained.getActionUuid(), retained.getActionType(),
            retained.getTarget(), ActionExecutionStatus.INCOMPLETE, 100L, 300L, "expired"));

        planner.plan(nodeAttribution(), ProtectionState.none(), 700_000L);
        RecoveryDecision replanned = planner.plan(nodeAttribution(), ProtectionState.none(), 700_045L);
        assertEquals(RecoveryDecisionType.EXECUTABLE, replanned.type());
        assertEquals(AvailabilityActionType.NODE_EXIT, replanned.action().getActionType());
    }

    private RecoveryActionPlanner planner(EnumSet<AvailabilityActionType> allowlist, boolean enabled) {
        return new RecoveryActionPlanner(config(allowlist, enabled));
    }

    private RecoveryPlannerConfig config(EnumSet<AvailabilityActionType> allowlist, boolean enabled) {
        return new RecoveryPlannerConfig(enabled, allowlist, 2, 600000L, 300000L, 1, 1, 120000L);
    }

    private FaultAttribution nodeAttribution() {
        return new FaultAttribution(FaultAttributionType.NODE_LOCAL_FAILURE, AvailabilityTarget.broker(1, 10L),
            AvailabilitySignalType.APPEND_STUCK, "node");
    }

    private FaultAttribution partitionAttribution() {
        return new FaultAttribution(FaultAttributionType.PARTITION_RECOVER_FAILED,
            AvailabilityTarget.topicPartition("topic", 0), AvailabilitySignalType.PARTITION_RECOVER_FAILED,
            "recover");
    }
}
