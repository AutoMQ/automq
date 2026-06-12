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

package kafka.automq;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.ValidList;

public final class AutoFallbackConfig {
    public static final String ENABLED_CONFIG = "automq.auto.fallback.enabled";
    public static final boolean ENABLED_DEFAULT = false;
    public static final String ENABLED_DOC = "Whether to enable automatic cluster availability fallback actions.";

    public static final String ACTION_ALLOWLIST_CONFIG = "automq.auto.fallback.action.allowlist";
    public static final String ACTION_NODE_EXIT = "NODE_EXIT";
    public static final String ACTION_PARTITION_REASSIGNMENT = "PARTITION_REASSIGNMENT";
    public static final String ACTION_SEGMENT_ROLL = "SEGMENT_ROLL";
    public static final String ACTION_CLEAN_SHUTDOWN_RECOVERY = "CLEAN_SHUTDOWN_RECOVERY";
    public static final String ACTION_SKIP_READ_RANGE = "SKIP_READ_RANGE";
    public static final String ACTION_PARTITION_RECREATE = "PARTITION_RECREATE";
    public static final String ACTION_ALLOWLIST_DEFAULT = ACTION_NODE_EXIT + "," + ACTION_PARTITION_REASSIGNMENT;
    public static final String ACTION_ALLOWLIST_DOC = "Comma-separated automatic fallback actions that are allowed to execute side effects. Actions outside the allowlist are dry-run only.";

    public static final String APPEND_STUCK_THRESHOLD_MS_CONFIG = "automq.auto.fallback.append.stuck.threshold.ms";
    public static final long APPEND_STUCK_THRESHOLD_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
    public static final String APPEND_STUCK_THRESHOLD_MS_DOC = "Only-for-test threshold in milliseconds before a pending S3Stream append is reported as APPEND_STUCK.";

    public static final String COLD_READ_STUCK_THRESHOLD_MS_CONFIG = "automq.auto.fallback.cold.read.stuck.threshold.ms";
    public static final long COLD_READ_STUCK_THRESHOLD_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
    public static final String COLD_READ_STUCK_THRESHOLD_MS_DOC = "Only-for-test threshold in milliseconds before a pending BlockCache cold read is reported as COLD_READ_STUCK.";

    public static final String PARTITION_CLOSE_STUCK_THRESHOLD_MS_CONFIG = "automq.auto.fallback.partition.close.stuck.threshold.ms";
    public static final long PARTITION_CLOSE_STUCK_THRESHOLD_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
    public static final String PARTITION_CLOSE_STUCK_THRESHOLD_MS_DOC = "Only-for-test threshold in milliseconds before a pending partition close is reported as PARTITION_CLOSE_STUCK.";

    public static final String LOG_READ_FAIL_THRESHOLD_CONFIG = "automq.auto.fallback.log.read.fail.threshold";
    public static final int LOG_READ_FAIL_THRESHOLD_DEFAULT = 3;
    public static final String LOG_READ_FAIL_THRESHOLD_DOC = "Only-for-test same-offset log read failure threshold before LOG_READ_FAIL is reported.";

    public static final String GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_CONFIG = "automq.auto.fallback.global.shared.storage.affected.ratio";
    public static final double GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_DEFAULT = 0.4;
    public static final String GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_DOC = "Only-for-test affected broker ratio threshold for global shared storage attribution.";

    public static final String GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_CONFIG = "automq.auto.fallback.global.shared.storage.small.cluster.brokers";
    public static final int GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_DEFAULT = 2;
    public static final String GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_DOC = "Only-for-test affected broker threshold for global shared storage attribution in small clusters.";

    public static final String ATTRIBUTION_CONSECUTIVE_THRESHOLD_CONFIG = "automq.auto.fallback.attribution.consecutive.threshold";
    public static final int ATTRIBUTION_CONSECUTIVE_THRESHOLD_DEFAULT = 2;
    public static final String ATTRIBUTION_CONSECUTIVE_THRESHOLD_DOC = "Only-for-test consecutive same-target attribution threshold before action planning.";

    public static final String ACTION_DEADLINE_MS_CONFIG = "automq.auto.fallback.action.deadline.ms";
    public static final long ACTION_DEADLINE_MS_DEFAULT = TimeUnit.MINUTES.toMillis(2);
    public static final String ACTION_DEADLINE_MS_DOC = "Only-for-test deadline in milliseconds for fallback action execution and response observation.";

    public static final String NODE_COOLDOWN_MS_CONFIG = "automq.auto.fallback.node.cooldown.ms";
    public static final long NODE_COOLDOWN_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    public static final String NODE_COOLDOWN_MS_DOC = "Only-for-test cooldown in milliseconds for node-level fallback actions.";

    public static final String PARTITION_LOG_COOLDOWN_MS_CONFIG = "automq.auto.fallback.partition.log.cooldown.ms";
    public static final long PARTITION_LOG_COOLDOWN_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);
    public static final String PARTITION_LOG_COOLDOWN_MS_DOC = "Only-for-test cooldown in milliseconds for partition/log-level fallback actions.";

    public static final String CLUSTER_ACTION_CONCURRENCY_CONFIG = "automq.auto.fallback.cluster.action.concurrency";
    public static final int CLUSTER_ACTION_CONCURRENCY_DEFAULT = 1;
    public static final String CLUSTER_ACTION_CONCURRENCY_DOC = "Only-for-test cluster-wide same-action concurrency limit for fallback actions.";

    public static final String BROKER_ACTION_CONCURRENCY_CONFIG = "automq.auto.fallback.broker.action.concurrency";
    public static final int BROKER_ACTION_CONCURRENCY_DEFAULT = 1;
    public static final String BROKER_ACTION_CONCURRENCY_DOC = "Only-for-test single-broker action concurrency limit for fallback actions.";

    public static final String CONTROLLER_RECONCILE_INTERVAL_MS_CONFIG = "automq.auto.fallback.controller.reconcile.interval.ms";
    public static final long CONTROLLER_RECONCILE_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(45);
    public static final String CONTROLLER_RECONCILE_INTERVAL_MS_DOC = "Only-for-test Controller reconcile interval in milliseconds for cluster availability fallback.";

    public static final String SIGNAL_WRITE_INTERVAL_MS_CONFIG = "automq.auto.fallback.signal.write.interval.ms";
    public static final long SIGNAL_WRITE_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
    public static final String SIGNAL_WRITE_INTERVAL_MS_DOC = "Only-for-test Broker availability signal write interval in milliseconds.";

    public static final String SIGNAL_STALE_MS_CONFIG = "automq.auto.fallback.signal.stale.ms";
    public static final long SIGNAL_STALE_MS_DEFAULT = TimeUnit.SECONDS.toMillis(90);
    public static final String SIGNAL_STALE_MS_DOC = "Only-for-test staleness threshold in milliseconds for Controller availability signal coverage.";

    public static final String RESPONSE_RETENTION_MS_CONFIG = "automq.auto.fallback.response.retention.ms";
    public static final long RESPONSE_RETENTION_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    public static final String RESPONSE_RETENTION_MS_DOC = "Only-for-test retention in milliseconds for completed fallback action responses.";

    public static final String ACTION_CLEANUP_GRACE_MS_CONFIG = "automq.auto.fallback.action.cleanup.grace.ms";
    public static final long ACTION_CLEANUP_GRACE_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
    public static final String ACTION_CLEANUP_GRACE_MS_DOC = "Only-for-test grace period in milliseconds before incomplete fallback actions are cleaned after their deadline.";

    public static final String CLEANUP_INTERVAL_MS_CONFIG = "automq.auto.fallback.cleanup.interval.ms";
    public static final long CLEANUP_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(60);
    public static final String CLEANUP_INTERVAL_MS_DOC = "Only-for-test cleanup interval in milliseconds for availability fallback transport state.";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        ENABLED_CONFIG,
        ACTION_ALLOWLIST_CONFIG
    );

    private AutoFallbackConfig() {
    }

    public static void define(ConfigDef configDef) {
        configDef.define(ENABLED_CONFIG, BOOLEAN, ENABLED_DEFAULT, HIGH, ENABLED_DOC)
            .define(ACTION_ALLOWLIST_CONFIG, LIST, ACTION_ALLOWLIST_DEFAULT,
                ValidList.in(ACTION_NODE_EXIT, ACTION_PARTITION_REASSIGNMENT, ACTION_SEGMENT_ROLL,
                    ACTION_CLEAN_SHUTDOWN_RECOVERY, ACTION_SKIP_READ_RANGE, ACTION_PARTITION_RECREATE),
                MEDIUM, ACTION_ALLOWLIST_DOC)
            .define(APPEND_STUCK_THRESHOLD_MS_CONFIG, LONG, APPEND_STUCK_THRESHOLD_MS_DEFAULT,
                Range.atLeast(1), LOW, APPEND_STUCK_THRESHOLD_MS_DOC)
            .define(COLD_READ_STUCK_THRESHOLD_MS_CONFIG, LONG, COLD_READ_STUCK_THRESHOLD_MS_DEFAULT,
                Range.atLeast(1), LOW, COLD_READ_STUCK_THRESHOLD_MS_DOC)
            .define(PARTITION_CLOSE_STUCK_THRESHOLD_MS_CONFIG, LONG, PARTITION_CLOSE_STUCK_THRESHOLD_MS_DEFAULT,
                Range.atLeast(1), LOW, PARTITION_CLOSE_STUCK_THRESHOLD_MS_DOC)
            .define(LOG_READ_FAIL_THRESHOLD_CONFIG, INT, LOG_READ_FAIL_THRESHOLD_DEFAULT,
                Range.atLeast(1), LOW, LOG_READ_FAIL_THRESHOLD_DOC)
            .define(GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_CONFIG, DOUBLE, GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_DEFAULT,
                Range.between(0.0, 1.0), LOW, GLOBAL_SHARED_STORAGE_AFFECTED_RATIO_DOC)
            .define(GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_CONFIG, INT, GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_DEFAULT,
                Range.atLeast(1), LOW, GLOBAL_SHARED_STORAGE_SMALL_CLUSTER_BROKERS_DOC)
            .define(ATTRIBUTION_CONSECUTIVE_THRESHOLD_CONFIG, INT, ATTRIBUTION_CONSECUTIVE_THRESHOLD_DEFAULT,
                Range.atLeast(1), LOW, ATTRIBUTION_CONSECUTIVE_THRESHOLD_DOC)
            .define(ACTION_DEADLINE_MS_CONFIG, LONG, ACTION_DEADLINE_MS_DEFAULT,
                Range.atLeast(1), LOW, ACTION_DEADLINE_MS_DOC)
            .define(NODE_COOLDOWN_MS_CONFIG, LONG, NODE_COOLDOWN_MS_DEFAULT,
                Range.atLeast(1), LOW, NODE_COOLDOWN_MS_DOC)
            .define(PARTITION_LOG_COOLDOWN_MS_CONFIG, LONG, PARTITION_LOG_COOLDOWN_MS_DEFAULT,
                Range.atLeast(1), LOW, PARTITION_LOG_COOLDOWN_MS_DOC)
            .define(CLUSTER_ACTION_CONCURRENCY_CONFIG, INT, CLUSTER_ACTION_CONCURRENCY_DEFAULT,
                Range.atLeast(1), LOW, CLUSTER_ACTION_CONCURRENCY_DOC)
            .define(BROKER_ACTION_CONCURRENCY_CONFIG, INT, BROKER_ACTION_CONCURRENCY_DEFAULT,
                Range.atLeast(1), LOW, BROKER_ACTION_CONCURRENCY_DOC)
            .define(CONTROLLER_RECONCILE_INTERVAL_MS_CONFIG, LONG, CONTROLLER_RECONCILE_INTERVAL_MS_DEFAULT,
                Range.atLeast(1), LOW, CONTROLLER_RECONCILE_INTERVAL_MS_DOC)
            .define(SIGNAL_WRITE_INTERVAL_MS_CONFIG, LONG, SIGNAL_WRITE_INTERVAL_MS_DEFAULT,
                Range.atLeast(1), LOW, SIGNAL_WRITE_INTERVAL_MS_DOC)
            .define(SIGNAL_STALE_MS_CONFIG, LONG, SIGNAL_STALE_MS_DEFAULT,
                Range.atLeast(1), LOW, SIGNAL_STALE_MS_DOC)
            .define(RESPONSE_RETENTION_MS_CONFIG, LONG, RESPONSE_RETENTION_MS_DEFAULT,
                Range.atLeast(1), LOW, RESPONSE_RETENTION_MS_DOC)
            .define(ACTION_CLEANUP_GRACE_MS_CONFIG, LONG, ACTION_CLEANUP_GRACE_MS_DEFAULT,
                Range.atLeast(1), LOW, ACTION_CLEANUP_GRACE_MS_DOC)
            .define(CLEANUP_INTERVAL_MS_CONFIG, LONG, CLEANUP_INTERVAL_MS_DEFAULT,
                Range.atLeast(1), LOW, CLEANUP_INTERVAL_MS_DOC);
    }

    public static boolean enabled(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        Object value = configs.get(ENABLED_CONFIG);
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new ConfigException(ENABLED_CONFIG, value, "Expected a boolean value.");
    }

    @SuppressWarnings("unchecked")
    public static List<String> actionAllowlist(Map<String, ?> raw) {
        Map<String, Object> configs = new HashMap<>(raw);
        Object value = configs.get(ACTION_ALLOWLIST_CONFIG);
        List<String> actions;
        if (value instanceof List) {
            actions = (List<String>) value;
        } else if (value instanceof String) {
            Object parsed = ConfigDef.parseType(ACTION_ALLOWLIST_CONFIG, value, LIST);
            actions = (List<String>) parsed;
        } else {
            throw new ConfigException(ACTION_ALLOWLIST_CONFIG, value, "Expected a comma separated list.");
        }
        for (String action : actions) {
            if (!Set.of(ACTION_NODE_EXIT, ACTION_PARTITION_REASSIGNMENT, ACTION_SEGMENT_ROLL,
                ACTION_CLEAN_SHUTDOWN_RECOVERY, ACTION_SKIP_READ_RANGE, ACTION_PARTITION_RECREATE).contains(action)) {
                throw new ConfigException(ACTION_ALLOWLIST_CONFIG, value, "Invalid fallback action " + action + ".");
            }
        }
        return List.copyOf(actions);
    }

    public static void validateReconfiguration(Map<String, ?> raw) {
        if (raw.containsKey(ENABLED_CONFIG)) {
            enabled(raw);
        }
        if (raw.containsKey(ACTION_ALLOWLIST_CONFIG)) {
            actionAllowlist(raw);
        }
    }
}
