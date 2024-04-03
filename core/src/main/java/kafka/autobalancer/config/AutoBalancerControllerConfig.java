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
    public static final String AUTO_BALANCER_CONTROLLER_ENABLE_DOC = "Whether to enable AutoBalancing.";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT_DOC = "The maximum time to block for one poll request in millisecond";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX_DOC = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MS_DOC;
    public static final String AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS_DOC = "The Controller's tolerable Broker metrics reporting delay, in milliseconds, has a default value of 60 seconds. " +
        "When the Controller checks for data rebalancing, if the most recent metrics reporting interval of the Broker exceeds the configured value, the Controller considers the state of that Broker to be based on outdated data, " +
        "and does not perform data rebalancing on that Broker. This configuration cannot be less than the Broker metrics reporting interval.";
    public static final String AUTO_BALANCER_CONTROLLER_GOALS_DOC = "The goal list is to be detected in the anomaly detector.";
    public static final String AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS_DOC = "The Controller checks whether data rebalancing is necessary at a minimum interval, in milliseconds, with a default value of 60 seconds. " +
        "The actual time to check for the next data rebalancing is also related to the scale of the partition migration that occurred this time. The more partitions that are migrated, the longer the interval for the next data rebalancing check." +
        "Shortening the minimum check interval can improve the sensitivity of data rebalancing, but in actual use, it should be configured in conjunction with the Broker metrics reporting interval to avoid the Controller being unable to perceive the result of the most recent scheduling, thereby causing duplicate scheduling.";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC = "Network in monitoring threshold. If it is below this threshold, the load of this Broker will not be actively optimized.";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The NetworkInUsageDistributionGoal tries to adjust the network inbound bandwidth utilization rate of each broker to a traffic distribution range." +
        "The default value is 0.2, which means the network inbound bandwidth utilization rate distribution range of each broker will be attempted to be adjusted to within the range of [average inbound bandwidth utilization rate * 0.8, average inbound bandwidth utilization rate * 1.2].";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD_DOC = "Network out monitoring threshold. If it is below this threshold, the load of this Broker will not be actively optimized.";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The acceptable range of deviation for average network output bandwidth usage";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS_DOC = "Time interval between reassignments per broker in milliseconds";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS_DOC = "The max number of reassignments per broker in one execution";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS_DOC = "Broker ids that auto balancer will ignore during balancing";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS_DOC = "Topics that auto balancer will ignore during balancing";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ENABLE,
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
