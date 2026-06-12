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

import kafka.automq.AutoFallbackConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.RecoveryPlannerConfig;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds the fallback planner knobs that are allowed to change through dynamic broker configs.
 */
public class AutoFallbackRuntimeConfig {
    public static final Set<String> RECONFIGURABLE_CONFIGS = AutoFallbackConfig.RECONFIGURABLE_CONFIGS;

    private final int consecutiveAttributionThreshold;
    private final long nodeCooldownMs;
    private final long partitionLogCooldownMs;
    private final int clusterActionConcurrency;
    private final int brokerActionConcurrency;
    private final long actionDeadlineMs;
    private volatile boolean fallbackEnabled;
    private volatile EnumSet<AvailabilityActionType> actionAllowlist;

    public AutoFallbackRuntimeConfig(KafkaConfig config) {
        this.fallbackEnabled = config.automqAutoFallbackEnabled();
        this.actionAllowlist = actionAllowlist(config.automqAutoFallbackActionAllowlist());
        this.consecutiveAttributionThreshold = config.automqAutoFallbackAttributionConsecutiveThreshold();
        this.nodeCooldownMs = config.automqAutoFallbackNodeCooldownMs();
        this.partitionLogCooldownMs = config.automqAutoFallbackPartitionLogCooldownMs();
        this.clusterActionConcurrency = config.automqAutoFallbackClusterActionConcurrency();
        this.brokerActionConcurrency = config.automqAutoFallbackBrokerActionConcurrency();
        this.actionDeadlineMs = config.automqAutoFallbackActionDeadlineMs();
    }

    public RecoveryPlannerConfig plannerConfig() {
        return new RecoveryPlannerConfig(fallbackEnabled, actionAllowlist,
            consecutiveAttributionThreshold, nodeCooldownMs, partitionLogCooldownMs,
            clusterActionConcurrency, brokerActionConcurrency, actionDeadlineMs);
    }

    public void validateReconfiguration(Map<String, ?> configs) {
        AutoFallbackConfig.validateReconfiguration(configs);
    }

    public void reconfigure(KafkaConfig newConfig) {
        this.fallbackEnabled = newConfig.automqAutoFallbackEnabled();
        this.actionAllowlist = actionAllowlist(newConfig.automqAutoFallbackActionAllowlist());
    }

    public void reconfigure(Map<String, ?> configs) {
        AutoFallbackConfig.validateReconfiguration(configs);
        this.fallbackEnabled = AutoFallbackConfig.enabled(configs);
        this.actionAllowlist = actionAllowlist(AutoFallbackConfig.actionAllowlist(configs));
    }

    private EnumSet<AvailabilityActionType> actionAllowlist(List<String> actions) {
        EnumSet<AvailabilityActionType> allowlist = EnumSet.noneOf(AvailabilityActionType.class);
        for (String action : actions) {
            allowlist.add(AvailabilityActionType.valueOf(action));
        }
        return allowlist;
    }
}
