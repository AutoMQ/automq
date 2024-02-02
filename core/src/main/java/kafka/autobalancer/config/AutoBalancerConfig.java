/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AutoBalancerConfig extends AbstractConfig {
    protected static final ConfigDef CONFIG;
    private static final String PREFIX = "autobalancer";

    /* Configurations */
    public static final String AUTO_BALANCER_TOPIC_CONFIG = PREFIX + "topic";
    public static final String AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG = PREFIX + "topic.num.partitions";
    public static final String AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_CONFIG = PREFIX + "topic.retention.ms";
    public static final String AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY = PREFIX + "topic.cleanup.policy";
    /* Default values */
    public static final String DEFAULT_AUTO_BALANCER_TOPIC = "__auto_balancer_metrics";
    public static final Integer DEFAULT_AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS = 1;
    public static final long DEFAULT_AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS = TimeUnit.MINUTES.toMillis(30);
    public static final String DEFAULT_AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY = String.join(",", TopicConfig.CLEANUP_POLICY_DELETE);
    /* Documents */
    private static final String AUTO_BALANCER_TOPIC_DOC = "The topic to which Auto Balancer metrics reporter "
            + "should send messages";
    private static final String AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions of Auto Balancer metrics topic";
    private static final String AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_DOC = TopicConfig.RETENTION_MS_DOC;
    public static final String AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY_DOC = TopicConfig.CLEANUP_POLICY_DOC;

    static {
        CONFIG = new ConfigDef()
                .define(AUTO_BALANCER_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_TOPIC,
                        ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_TOPIC_DOC)
                .define(AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_DOC)
                .define(AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_DOC)
                .define(AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY,
                        ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY_DOC);
    }

    public AutoBalancerConfig(Map<?, ?> originals, boolean doLogs) {
        super(CONFIG, originals, doLogs);
    }
}
