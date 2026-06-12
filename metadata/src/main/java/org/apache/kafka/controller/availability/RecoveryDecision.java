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

public class RecoveryDecision {
    private final RecoveryDecisionType type;
    private final RecoveryAction action;
    private final BlockedReason blockedReason;
    private final long nextEligibleMs;

    private RecoveryDecision(RecoveryDecisionType type, RecoveryAction action, BlockedReason blockedReason,
                             long nextEligibleMs) {
        this.type = type;
        this.action = action;
        this.blockedReason = blockedReason;
        this.nextEligibleMs = nextEligibleMs;
    }

    public static RecoveryDecision noop() {
        return new RecoveryDecision(RecoveryDecisionType.NOOP, null, BlockedReason.NONE, -1L);
    }

    public static RecoveryDecision blocked(BlockedReason reason, long nextEligibleMs) {
        return new RecoveryDecision(RecoveryDecisionType.BLOCKED, null, reason, nextEligibleMs);
    }

    public static RecoveryDecision dryRun(RecoveryAction action) {
        return new RecoveryDecision(RecoveryDecisionType.DRY_RUN, action, BlockedReason.NONE, action.getDeadlineMs());
    }

    public static RecoveryDecision executable(RecoveryAction action) {
        return new RecoveryDecision(RecoveryDecisionType.EXECUTABLE, action, BlockedReason.NONE, action.getDeadlineMs());
    }

    public RecoveryDecisionType type() {
        return type;
    }

    public RecoveryAction action() {
        return action;
    }

    public BlockedReason blockedReason() {
        return blockedReason;
    }

    public long nextEligibleMs() {
        return nextEligibleMs;
    }
}
