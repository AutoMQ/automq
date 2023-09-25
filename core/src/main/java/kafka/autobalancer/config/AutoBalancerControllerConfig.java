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

package kafka.autobalancer.config;

import kafka.autobalancer.goals.NetworkInCapacityGoal;
import kafka.autobalancer.goals.NetworkInDistributionGoal;
import kafka.autobalancer.goals.NetworkOutCapacityGoal;
import kafka.autobalancer.goals.NetworkOutDistributionGoal;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;

public class AutoBalancerControllerConfig extends AutoBalancerConfig {
    /* Configurations */
    private static final String PREFIX = "autobalancer.controller.";
    public static final String AUTO_BALANCER_CONTROLLER_ENABLE = PREFIX + "enable";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT = PREFIX + "consumer.poll.timeout";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX = PREFIX + "consumer.client.id";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX = PREFIX + CommonClientConfigs.GROUP_ID_CONFIG;
    public static final String AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS = PREFIX + "metrics.delay.ms";
    public static final String AUTO_BALANCER_CONTROLLER_GOALS = PREFIX + "goals";
    public static final String AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS = PREFIX + "anomaly.detect.interval.ms";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD = PREFIX + "network.in.distribution.detect.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION = PREFIX + "network.in.distribution.detect.avg.deviation";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD = PREFIX + "network.out.distribution.detect.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION = PREFIX + "network.out.distribution.detect.avg.deviation";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_UTILIZATION_THRESHOLD = PREFIX + "network.in.utilization.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_UTILIZATION_THRESHOLD = PREFIX + "network.out.utilization.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS = PREFIX + "execution.interval.ms";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS = PREFIX + "execution.steps";
    public static final String AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION = PREFIX + "load.aggregation";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS = PREFIX + "exclude.broker.ids";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS = PREFIX + "exclude.topics";
    /* Default values */
    public static final boolean DEFAULT_AUTO_BALANCER_CONTROLLER_ENABLE = false;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT = 1000L;
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX = "AutoBalancerControllerConsumer";
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX = "AutoBalancerControllerConsumerGroup";
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS = Duration.ofMinutes(1).toMillis();
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_GOALS = new StringJoiner(",")
            .add(NetworkInCapacityGoal.class.getName())
            .add(NetworkOutCapacityGoal.class.getName())
            .add(NetworkInDistributionGoal.class.getName())
            .add(NetworkOutDistributionGoal.class.getName()).toString();
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS = 60000;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD = 0.2;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION = 0.2;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD = 0.2;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION = 0.2;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_THRESHOLD = 0.8;
    public static final double DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_THRESHOLD = 0.8;
    public static final long DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS = 1000;
    public static final int DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS = 60;
    public static final boolean DEFAULT_AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION = false;
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS = "";
    public static final String DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS = "";
    /* Documents */
    public static final String AUTO_BALANCER_CONTROLLER_ENABLE_DOC = "Whether to enable auto balancer";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT_DOC = "The maximum time to block for one poll request in millisecond";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX_DOC = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";
    public static final String AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX_DOC = CommonClientConfigs.GROUP_ID_DOC;
    public static final String AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS_DOC = "The maximum delayed time to consider a metrics valid";
    public static final String AUTO_BALANCER_CONTROLLER_GOALS_DOC = "The goals to be detect in anomaly detector";
    public static final String AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS_DOC = "Time interval between anomaly detections in milliseconds";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD_DOC = "The network input bandwidth usage detect threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The acceptable range of deviation for average network input bandwidth usage";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD_DOC = "The network output bandwidth usage detect threshold";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC = "The acceptable range of deviation for average network output bandwidth usage";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_THRESHOLD_DOC = "The maximum network input bandwidth usage of broker before trigger load balance";
    public static final String AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_THRESHOLD_DOC = PREFIX + "network.out.usage.threshold";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS_DOC = "Time interval between reassignments per broker in milliseconds";
    public static final String AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS_DOC = "The max number of reassignments per broker in one execution";
    public static final String AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION_DOC = "Use aggregation of partition load as broker load, instead of using reported broker metrics directly";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS_DOC = "Broker ids that auto balancer will ignore during balancing, separated by comma";
    public static final String AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS_DOC = "Topics that auto balancer will ignore during balancing, separated by comma";

    static {
        CONFIG.define(AUTO_BALANCER_CONTROLLER_ENABLE, ConfigDef.Type.BOOLEAN,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ENABLE, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ENABLE_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX, ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX, ConfigDef.Importance.LOW,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX_DOC)
                .define(AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX, ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX_DOC)
                .define(AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_GOALS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_GOALS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_GOALS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_IN_UTILIZATION_THRESHOLD, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_NETWORK_OUT_UTILIZATION_THRESHOLD, ConfigDef.Type.DOUBLE,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_THRESHOLD, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_THRESHOLD_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXECUTION_INTERVAL_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS, ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXECUTION_STEPS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION, ConfigDef.Type.BOOLEAN,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXCLUDE_BROKER_IDS_DOC)
                .define(AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS, ConfigDef.Type.LIST,
                        DEFAULT_AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS, ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS_DOC);
    }

    public AutoBalancerControllerConfig(Map<?, ?> originals, boolean doLog) {
        super(originals, doLog);
    }
}
