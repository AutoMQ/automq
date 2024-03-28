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

import kafka.autobalancer.goals.NetworkInUsageDistributionGoal;
import kafka.autobalancer.goals.NetworkOutUsageDistributionGoal;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class AutoBalancerControllerConfig extends AbstractConfig {
    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final String PREFIX = "autobalancer.controller.";
    /* Configurations */
    public static final String AUTO_BALANCER_CONTROLLER_ENABLE = PREFIX + "enable";
    public static final String AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG = PREFIX + "topic.num.partitions";
    public static final String AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG = PREFIX + "topic.retention.ms";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT = PREFIX + "consumer.poll.timeout";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX = PREFIX + "consumer.client.id";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS = PREFIX + CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    public static final String AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS = PREFIX + "metrics.delay.ms";
    public static final String AUTO_BALANCER_CONTROLLER_GOALS = PREFIX + "goals";
    public static final String AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS = PREFIX + "anomaly.detect.interval.ms";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD = PREFIX + "network.in.usage.distribution.detect.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION = PREFIX + "network.in.distribution.detect.avg.deviation";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD = PREFIX + "network.out.usage.distribution.detect.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION = PREFIX + "network.out.distribution.detect.avg.deviation";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS = PREFIX + "execution.interval.ms";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS = PREFIX + "execution.steps";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS = PREFIX + "exclude.broker.ids";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS = PREFIX + "exclude.topics";
    /* Default values */
    public static final boolean DEFAULT_AUTO_BALANCER_CONTROLLER_ENABLE = false;
    public static final Integer DEFAULT_AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG = 1;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG = TimeUnit.MINUTES.toMillis(30);
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT = 1000L;
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX = "AutoBalancerControllerConsumer";
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS = 1000;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS = Duration.ofMinutes(1).toMillis();
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_GOALS = new StringJoiner(",")
            .add(NetworkInUsageDistributionGoal.class.getName())
            .add(NetworkOutUsageDistributionGoal.class.getName()).toString();
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS = 60000;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD = 1024 * 1024;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION = 0.2;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD = 1024 * 1024;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION = 0.2;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS = 1000;
    public static final int DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS = 60;
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS = "";
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS = "";
    /* Documents */
    private static final String AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG_DOC = "The number of partitions of Auto Balancer metrics topic";
    private static final String AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG_DOC = TopicConfig.RETENTION_MS_DOC;
    public static final String AUTO_BALANCER_CONTROLLER_ENABLE_DOC = "Whether to enable auto balancer";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT_DOC = "The maximum time to block for one poll request in millisecond";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX_DOC = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MS_DOC;
    public static final String AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS_DOC = "The maximum delayed time to consider a metrics valid";
    public static final String AUTO_BALANCER_CONTROLLER_GOALS_DOC = "The goals to be detect in anomaly detector";
    public static final String AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS_DOC = "Time interval between anomaly detections in milliseconds";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC = "The network input bandwidth usage detect threshold in bytes";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The acceptable range of deviation for average network input bandwidth usage";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC = "The network output bandwidth usage detect threshold in bytes";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The acceptable range of deviation for average network output bandwidth usage";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS_DOC = "Time interval between reassignments per broker in milliseconds";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS_DOC = "The max number of reassignments per broker in one execution";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS_DOC = "Broker ids that auto balancer will ignore during balancing, separated by comma";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS_DOC = "Topics that auto balancer will ignore during balancing, separated by comma";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS,
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS
    );

    static {
        CONFIG_DEF.define(AUTO_BALANCER_CONTROLLER_ENABLE, ConfigDef.Type.BOOLEAN,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ENABLE, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ENABLE_DOC)
                .define(AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
                        ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG_DOC)
                .define(AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX, ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX, ConfigDef.Importance.LOW,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_GOALS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_GOALS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_GOALS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS, ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS_DOC);
    }

    public AutoBalancerControllerConfig(Map<?, ?> originals, boolean doLog) {
        super(CONFIG_DEF, originals, doLog);
    }
}
