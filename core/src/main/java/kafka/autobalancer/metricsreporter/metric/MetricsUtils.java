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
/*
 * Some portion of this file Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.autobalancer.metricsreporter.metric;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import kafka.metrics.KafkaMetricsGroup$;
import scala.collection.immutable.Map$;
import scala.jdk.javaapi.CollectionConverters;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils.
 */
public final class MetricsUtils {
    // Names
    public static final String BYTES_IN_PER_SEC = "BytesInPerSec";
    public static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
    /* Used to identify idle partitions  */
    public static final String SIZE = "Size";
    // Attribute
    static final String ATTRIBUTE_MEAN = "Mean";
    static final String ATTRIBUTE_MAX = "Max";
    static final String ATTRIBUTE_50TH_PERCENTILE = "50thPercentile";
    static final String ATTRIBUTE_999TH_PERCENTILE = "999thPercentile";
    // Groups
    private static final String KAFKA_SERVER = "kafka.server";
    private static final String KAFKA_LOG_PREFIX = "kafka.log";
    // Type Keys
    private static final String TOPIC_KEY = "topic";
    private static final String PARTITION_KEY = "partition";
    // Type
    private static final String LOG_GROUP = "Log";
    private static final String BROKER_TOPIC_PARTITION_METRICS_GROUP = "BrokerTopicPartitionMetrics";
    private static final String BROKER_TOPIC_METRICS_GROUP = "BrokerTopicMetrics";
    // Name Set.
    private static final Set<String> INTERESTED_TOPIC_PARTITION_METRIC_NAMES =
            Set.of(BYTES_IN_PER_SEC, BYTES_OUT_PER_SEC);
    private static final Set<String> INTERESTED_LOG_METRIC_NAMES = Set.of(SIZE);

    private MetricsUtils() {

    }

    /**
     * Get empty metric for specified metric name
     *
     * @param name metric name
     * @return empty metric implementation
     */
    public static Metric getEmptyMetricFor(String name) {
        switch (name) {
            case BYTES_IN_PER_SEC:
            case BYTES_OUT_PER_SEC:
                return new EmptyMeter();
            default:
                return null;
        }
    }

    public static MetricName buildBrokerMetricName(String name) {
        String group = null;
        String type = null;
        if (BYTES_IN_PER_SEC.equals(name) || BYTES_OUT_PER_SEC.equals(name)) {
            group = KAFKA_SERVER;
            type = BROKER_TOPIC_METRICS_GROUP;
        }
        if (group == null) {
            return null;
        }
        return KafkaMetricsGroup$.MODULE$.explicitMetricName(group, type, name, Map$.MODULE$.empty());
    }

    public static MetricName buildTopicPartitionMetricName(String name, String topic, String partition) {
        Map<String, String> tags = Map.of("topic", topic, "partition", partition);
        String group = null;
        String type = null;
        if (BYTES_IN_PER_SEC.equals(name) || BYTES_OUT_PER_SEC.equals(name)) {
            group = KAFKA_SERVER;
            type = BROKER_TOPIC_PARTITION_METRICS_GROUP;
        }
        if (group == null) {
            return null;
        }
        return KafkaMetricsGroup$.MODULE$.explicitMetricName(group, type, name, CollectionConverters.asScala(tags));
    }

    public static Set<String> getMetricNameMaybeMissing() {
        return new HashSet<>(INTERESTED_TOPIC_PARTITION_METRIC_NAMES);
    }

    /**
     * Create a Auto Balancer Metric.
     *
     * @param nowMs      The current time in milliseconds.
     * @param brokerId   Broker Id.
     * @param metricName Yammer metric name.
     * @param value      Metric value
     * @return A Yammer metric converted as a AutoBalancerMetric.
     */
    public static AutoBalancerMetrics toAutoBalancerMetric(long nowMs,
                                                           int brokerId,
                                                           String brokerRack,
                                                           com.yammer.metrics.core.MetricName metricName,
                                                           double value) {
        return toAutoBalancerMetric(nowMs, brokerId, brokerRack, metricName, value, null);
    }

    /**
     * Create a Auto Balancer Metric.
     *
     * @param nowMs      The current time in milliseconds.
     * @param brokerId   Broker Id.
     * @param brokerRack Broker rack.
     * @param metricName Yammer metric name.
     * @param value      Metric value
     * @param attribute  Metric attribute.
     * @return A Yammer metric converted as a AutoBalancerMetric.
     */
    public static AutoBalancerMetrics toAutoBalancerMetric(long nowMs,
                                                           int brokerId,
                                                           String brokerRack,
                                                           com.yammer.metrics.core.MetricName metricName,
                                                           double value,
                                                           String attribute) {
        Map<String, String> tags = yammerMetricScopeToTags(metricName.getScope());
        AutoBalancerMetrics ccm = tags == null ? null : toAutoBalancerMetric(nowMs, brokerId, brokerRack, metricName.getName(), tags, value, attribute);
        if (ccm == null) {
            throw new IllegalArgumentException(String.format("Cannot convert yammer metric %s to a Auto Balancer metric for "
                    + "broker %d at time %d for tag %s", metricName, brokerId, nowMs, attribute));
        }
        return ccm;
    }

    /**
     * Build a AutoBalancerMetric object.
     *
     * @param nowMs      The current time in milliseconds.
     * @param brokerId   Broker Id.
     * @param brokerRack Broker rack.
     * @param name       Name of the metric.
     * @param tags       Tags of the metric.
     * @param value      Metric value.
     * @param attribute  Metric attribute -- can be {@code null}.
     * @return A {@link AutoBalancerMetrics} object with the given properties.
     */
    private static AutoBalancerMetrics toAutoBalancerMetric(long nowMs,
                                                            int brokerId,
                                                            String brokerRack,
                                                            String name,
                                                            Map<String, String> tags,
                                                            double value,
                                                            String attribute) {
        String topic = tags.get(TOPIC_KEY);
        String partitionStr = tags.get(PARTITION_KEY);
        int partition = -1;
        if (partitionStr != null) {
            try {
                partition = Integer.parseInt(partitionStr);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        switch (name) {
            case BYTES_IN_PER_SEC:
                // network inbound bandwidth capacity is in KB/s
                value = value / 1024;
                return bytesInToMetric(topic, partition, nowMs, brokerId, brokerRack, value);
            case BYTES_OUT_PER_SEC:
                // network inbound bandwidth capacity is in KB/s
                value = value / 1024;
                return bytesOutToMetric(topic, partition, nowMs, brokerId, brokerRack, value);
            case SIZE:
                if (partition == -1) {
                    return null;
                }
                return new TopicPartitionMetrics(nowMs, brokerId, brokerRack, topic, partition).put(RawMetricType.PARTITION_SIZE, value);
            default:
                return null;
        }
    }

    /**
     * Get the "recent CPU usage" for the JVM process.
     *
     * @param nowMs          The current time in milliseconds.
     * @param brokerId       Broker Id.
     * @param brokerRack     Broker rack.
     * @param kubernetesMode If {@code true}, gets CPU usage values with respect to the operating environment instead of node.
     * @return the "recent CPU usage" for the JVM process as a double in [0.0,1.0].
     */
    public static BrokerMetrics getCpuMetric(long nowMs, int brokerId, String brokerRack, boolean kubernetesMode) throws IOException {
        double cpuUtil = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuLoad();

        if (kubernetesMode) {
            cpuUtil = ContainerMetricUtils.getContainerProcessCpuLoad(cpuUtil);
        }

        if (cpuUtil < 0) {
            throw new IOException("Java Virtual Machine recent CPU usage is not available.");
        }
        BrokerMetrics brokerMetric = new BrokerMetrics(nowMs, brokerId, brokerRack);
        brokerMetric.put(RawMetricType.BROKER_CPU_UTIL, cpuUtil);
        return brokerMetric;
    }

    /**
     * Check whether the yammer metric name is an interested metric.
     *
     * @param metricName Yammer metric name.
     * @return {@code true} if the yammer metric name is an interested metric, {@code false} otherwise.
     */
    public static boolean isInterested(com.yammer.metrics.core.MetricName metricName) {
        Map<String, String> tags = yammerMetricScopeToTags(metricName.getScope());
        return tags != null && isInterested(metricName.getGroup(), metricName.getName(), metricName.getType(), tags);
    }

    /**
     * Check if a metric is an interested metric.
     *
     * @param group Group of the metric.
     * @param name  Name of the metric.
     * @param type  Type of the metric.
     * @param tags  Tags of the metric.
     * @return {@code true} for a metric of interest, {@code false} otherwise.
     */
    private static boolean isInterested(String group, String name, String type, Map<String, String> tags) {
        if (group.equals(KAFKA_SERVER)) {
            if (BROKER_TOPIC_PARTITION_METRICS_GROUP.equals(type)) {
                return INTERESTED_TOPIC_PARTITION_METRIC_NAMES.contains(name) && sanityCheckTopicPartitionTags(tags);
            } else if (BROKER_TOPIC_METRICS_GROUP.equals(type)) {
                return INTERESTED_TOPIC_PARTITION_METRIC_NAMES.contains(name) && tags.isEmpty();
            }
        } else if (group.startsWith(KAFKA_LOG_PREFIX) && INTERESTED_LOG_METRIC_NAMES.contains(name)) {
            return LOG_GROUP.equals(type);
        }
        return false;
    }

    /**
     * Convert a yammer metrics scope to a tags map.
     *
     * @param scope Scope of the Yammer metric.
     * @return Empty map for {@code null} scope, {@code null} for scope with keys without a matching value (i.e. unacceptable
     * scope) (see <a href="https://github.com/linkedin/cruise-control/issues/1296">...</a>), parsed tags otherwise.
     */
    public static Map<String, String> yammerMetricScopeToTags(String scope) {
        if (scope != null) {
            String[] kv = scope.split("\\.");
            if (kv.length % 2 != 0) {
                return null;
            }
            Map<String, String> tags = new HashMap<>();
            for (int i = 0; i < kv.length; i += 2) {
                tags.put(kv[i], kv[i + 1]);
            }
            return tags;
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Check if tags are valid for a topic-partition metric
     *
     * @param tags metric tags
     * @return true if valid, false otherwise
     */
    public static boolean sanityCheckTopicPartitionTags(Map<String, String> tags) {
        return tags.containsKey("topic") && tags.containsKey("partition");
    }

    private static AutoBalancerMetrics bytesInToMetric(String topic, int partition, long nowMs,
                                                       int brokerId, String brokerRack, double value) {

        if (topic != null && partition != -1) {
            return new TopicPartitionMetrics(nowMs, brokerId, brokerRack, topic, partition).put(RawMetricType.TOPIC_PARTITION_BYTES_IN, value);
        } else if (topic == null && partition == -1) {
            return new BrokerMetrics(nowMs, brokerId, brokerRack).put(RawMetricType.ALL_TOPIC_BYTES_IN, value);
        }
        return null;
    }

    private static AutoBalancerMetrics bytesOutToMetric(String topic, int partition, long nowMs,
                                                        int brokerId, String brokerRack, double value) {

        if (topic != null && partition != -1) {
            return new TopicPartitionMetrics(nowMs, brokerId, brokerRack, topic, partition).put(RawMetricType.TOPIC_PARTITION_BYTES_OUT, value);
        } else if (topic == null && partition == -1) {
            return new BrokerMetrics(nowMs, brokerId, brokerRack).put(RawMetricType.ALL_TOPIC_BYTES_OUT, value);
        }
        return null;
    }

    public static boolean sanityCheckBrokerMetricsCompleteness(AutoBalancerMetrics metrics) {
        return metrics.getMetricTypeValueMap().keySet().containsAll(RawMetricType.brokerMetricTypes());
    }

    public static boolean sanityCheckTopicPartitionMetricsCompleteness(AutoBalancerMetrics metrics) {
        return metrics.getMetricTypeValueMap().keySet().containsAll(RawMetricType.partitionMetricTypes());
    }
}
