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

import java.util.EnumSet;
import java.util.Set;

public class RecoveryPlannerConfig {
    private final boolean fallbackEnabled;
    private final Set<AvailabilityActionType> actionAllowlist;
    private final int consecutiveAttributionThreshold;
    private final long nodeCooldownMs;
    private final long partitionLogCooldownMs;
    private final int clusterActionConcurrency;
    private final int brokerActionConcurrency;
    private final long actionDeadlineMs;

    public RecoveryPlannerConfig(boolean fallbackEnabled, Set<AvailabilityActionType> actionAllowlist,
                                 int consecutiveAttributionThreshold, long nodeCooldownMs,
                                 long partitionLogCooldownMs, int clusterActionConcurrency,
                                 int brokerActionConcurrency, long actionDeadlineMs) {
        this.fallbackEnabled = fallbackEnabled;
        this.actionAllowlist = actionAllowlist.isEmpty() ? EnumSet.noneOf(AvailabilityActionType.class) :
            EnumSet.copyOf(actionAllowlist);
        this.consecutiveAttributionThreshold = consecutiveAttributionThreshold;
        this.nodeCooldownMs = nodeCooldownMs;
        this.partitionLogCooldownMs = partitionLogCooldownMs;
        this.clusterActionConcurrency = clusterActionConcurrency;
        this.brokerActionConcurrency = brokerActionConcurrency;
        this.actionDeadlineMs = actionDeadlineMs;
    }

    public boolean fallbackEnabled() {
        return fallbackEnabled;
    }

    public Set<AvailabilityActionType> actionAllowlist() {
        return actionAllowlist;
    }

    public int consecutiveAttributionThreshold() {
        return consecutiveAttributionThreshold;
    }

    public long nodeCooldownMs() {
        return nodeCooldownMs;
    }

    public long partitionLogCooldownMs() {
        return partitionLogCooldownMs;
    }

    public int clusterActionConcurrency() {
        return clusterActionConcurrency;
    }

    public int brokerActionConcurrency() {
        return brokerActionConcurrency;
    }

    public long actionDeadlineMs() {
        return actionDeadlineMs;
    }
}
