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
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.metric;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class MetricsUtils {
    // Attribute
    static final String ATTRIBUTE_MEAN = "Mean";
    static final String ATTRIBUTE_MAX = "Max";
    static final String ATTRIBUTE_50TH_PERCENTILE = "50thPercentile";
    static final String ATTRIBUTE_999TH_PERCENTILE = "999thPercentile";
    // Names
    private static final String BYTES_IN_PER_SEC = "BytesInPerSec";
    private static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
    private static final String REQUEST_QUEUE_SIZE = "RequestQueueSize";
    private static final String RESPONSE_QUEUE_SIZE = "ResponseQueueSize";
    private static final String REQUEST_QUEUE_TIME_MS = "RequestQueueTimeMs";
    private static final String TOTAL_TIME_MS = "TotalTimeMs";
    // Groups
    private static final String KAFKA_SERVER = "kafka.server";
    private static final String KAFKA_NETWORK = "kafka.network";
    // Type Keys
    private static final String TYPE_KEY = "type";
    private static final String TOPIC_KEY = "topic";
    private static final String PARTITION_KEY = "partition";
    private static final String REQUEST_TYPE_KEY = "request";
    // Type
    private static final String BROKER_TOPIC_METRICS_GROUP = "BrokerTopicMetrics";
    private static final String BROKER_TOPIC_PARTITION_METRICS_GROUP = "BrokerTopicPartitionMetrics";
    private static final String REQUEST_METRICS_GROUP = "RequestMetrics";
    private static final String REQUEST_CHANNEL_GROUP = "RequestChannel";
    private static final String CONSUMER_FETCH_REQUEST_TYPE = "FetchConsumer";
    private static final String PRODUCE_REQUEST_TYPE = "Produce";
    // Name Set.
    private static final Set<String> INTERESTED_NETWORK_METRIC_NAMES =
        new HashSet<>(Arrays.asList(REQUEST_QUEUE_SIZE, RESPONSE_QUEUE_SIZE, REQUEST_QUEUE_TIME_MS, TOTAL_TIME_MS));

    private static final Set<String> INTERESTED_TOPIC_METRIC_NAMES =
        new HashSet<>(Arrays.asList(BYTES_IN_PER_SEC, BYTES_OUT_PER_SEC));

    // Request type set
    private static final Set<String> INTERESTED_REQUEST_TYPE =
        new HashSet<>(Arrays.asList(CONSUMER_FETCH_REQUEST_TYPE, PRODUCE_REQUEST_TYPE));

    private MetricsUtils() {

    }
    /**
     * Create a Cruise Control Metric.
     *
     * @param nowMs      The current time in milliseconds.
     * @param brokerId   Broker Id.
     * @param metricName Yammer metric name.
     * @param value      Metric value
     * @return A Yammer metric converted as a CruiseControlMetric.
     */
    public static CruiseControlMetric toCruiseControlMetric(long nowMs,
                                                            int brokerId,
                                                            com.yammer.metrics.core.MetricName metricName,
                                                            double value) {
        return toCruiseControlMetric(nowMs, brokerId, metricName, value, null);
    }

    /**
     * Create a Cruise Control Metric.
     *
     * @param nowMs      The current time in milliseconds.
     * @param brokerId   Broker Id.
     * @param metricName Yammer metric name.
     * @param value      Metric value
     * @param attribute  Metric attribute.
     * @return A Yammer metric converted as a CruiseControlMetric.
     */
    public static CruiseControlMetric toCruiseControlMetric(long nowMs,
                                                            int brokerId,
                                                            com.yammer.metrics.core.MetricName metricName,
                                                            double value,
                                                            String attribute) {
        Map<String, String> tags = yammerMetricScopeToTags(metricName.getScope());
        CruiseControlMetric ccm = tags == null ? null : toCruiseControlMetric(nowMs, brokerId, metricName.getName(), tags, value, attribute);
        if (ccm == null) {
            throw new IllegalArgumentException(String.format("Cannot convert yammer metric %s to a Cruise Control metric for "
                + "broker %d at time %d for tag %s", metricName, brokerId, nowMs, attribute));
        }
        return ccm;
    }

    /**
     * Build a CruiseControlMetric object.
     *
     * @param nowMs     The current time in milliseconds.
     * @param brokerId  Broker Id.
     * @param name      Name of the metric.
     * @param tags      Tags of the metric.
     * @param value     Metric value.
     * @param attribute Metric attribute -- can be {@code null}.
     * @return A {@link CruiseControlMetric} object with the given properties.
     */
    private static CruiseControlMetric toCruiseControlMetric(long nowMs,
                                                             int brokerId,
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
                if (topic != null && partition != -1) {
                    return new PartitionMetric(RawMetricType.TOPIC_PARTITION_BYTES_IN, nowMs, brokerId, topic, partition, value);
                } else {
                    return null;
                }
            case BYTES_OUT_PER_SEC:
                if (topic != null && partition != -1) {
                    return new PartitionMetric(RawMetricType.TOPIC_PARTITION_BYTES_OUT, nowMs, brokerId, topic, partition, value);
                } else {
                    return null;
                }
            case REQUEST_QUEUE_SIZE:
                return new BrokerMetric(RawMetricType.BROKER_REQUEST_QUEUE_SIZE, nowMs, brokerId, value);
            case RESPONSE_QUEUE_SIZE:
                return new BrokerMetric(RawMetricType.BROKER_RESPONSE_QUEUE_SIZE, nowMs, brokerId, value);
            case REQUEST_QUEUE_TIME_MS:
                return requestQueueTimeToMetric(tags.get(REQUEST_TYPE_KEY), attribute, nowMs, brokerId, value);
            case TOTAL_TIME_MS:
                return totalTimeToMetric(tags.get(REQUEST_TYPE_KEY), attribute, nowMs, brokerId, value);
            default:
                return null;
        }
    }

    /**
     * Get the "recent CPU usage" for the JVM process.
     *
     * @param nowMs          The current time in milliseconds.
     * @param brokerId       Broker Id.
     * @param kubernetesMode If {@code true}, gets CPU usage values with respect to the operating environment instead of node.
     * @return the "recent CPU usage" for the JVM process as a double in [0.0,1.0].
     */
    public static BrokerMetric getCpuMetric(long nowMs, int brokerId, boolean kubernetesMode) throws IOException {
        double cpuUtil = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuLoad();

        if (kubernetesMode) {
            cpuUtil = ContainerMetricUtils.getContainerProcessCpuLoad(cpuUtil);
        }

        if (cpuUtil < 0) {
            throw new IOException("Java Virtual Machine recent CPU usage is not available.");
        }
        return new BrokerMetric(RawMetricType.BROKER_CPU_UTIL, nowMs, brokerId, cpuUtil);
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
            return INTERESTED_TOPIC_METRIC_NAMES.contains(name) && BROKER_TOPIC_PARTITION_METRICS_GROUP.equals(type);
        } else if (group.equals(KAFKA_NETWORK) && INTERESTED_NETWORK_METRIC_NAMES.contains(name)) {
            if (REQUEST_METRICS_GROUP.equals(type)) {
                String requestType = tags.get(REQUEST_TYPE_KEY);
                if (requestType == null) {
                    return false;
                }
                return INTERESTED_NETWORK_METRIC_NAMES.contains(name) && INTERESTED_REQUEST_TYPE.contains(requestType);
            } else return REQUEST_CHANNEL_GROUP.equals(type);
        }
        return false;
    }

    /**
     * Convert a yammer metrics scope to a tags map.
     *
     * @param scope Scope of the Yammer metric.
     * @return Empty map for {@code null} scope, {@code null} for scope with keys without a matching value (i.e. unacceptable
     * scope) (see https://github.com/linkedin/cruise-control/issues/1296), parsed tags otherwise.
     */
    private static Map<String, String> yammerMetricScopeToTags(String scope) {
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

    private static CruiseControlMetric requestQueueTimeToMetric(String requestType, String attribute, long nowMs,
                                                         int brokerId, double value) {
        switch (requestType) {
            case PRODUCE_REQUEST_TYPE:
                switch (attribute) {
                    case ATTRIBUTE_MAX:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX, nowMs, brokerId, value);
                    case ATTRIBUTE_MEAN:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, nowMs, brokerId, value);
                    case ATTRIBUTE_50TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH, nowMs, brokerId, value);
                    case ATTRIBUTE_999TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, nowMs, brokerId, value);
                    default:
                        return null;
                }
            case CONSUMER_FETCH_REQUEST_TYPE:
                switch (attribute) {
                    case ATTRIBUTE_MAX:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, nowMs, brokerId, value);
                    case ATTRIBUTE_MEAN:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, nowMs, brokerId, value);
                    case ATTRIBUTE_50TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, nowMs, brokerId, value);
                    case ATTRIBUTE_999TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, nowMs, brokerId, value);
                    default:
                        return null;
                }
            default:
                return null;
        }
    }

    private static CruiseControlMetric totalTimeToMetric(String requestType, String attribute, long nowMs,
                                                  int brokerId, double value) {
        switch (requestType) {
            case PRODUCE_REQUEST_TYPE:
                switch (attribute) {
                    case ATTRIBUTE_MAX:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX, nowMs, brokerId, value);
                    case ATTRIBUTE_MEAN:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, nowMs, brokerId, value);
                    case ATTRIBUTE_50TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH, nowMs, brokerId, value);
                    case ATTRIBUTE_999TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH, nowMs, brokerId, value);
                    default:
                        return null;
                }
            case CONSUMER_FETCH_REQUEST_TYPE:
                switch (attribute) {
                    case ATTRIBUTE_MAX:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX, nowMs, brokerId, value);
                    case ATTRIBUTE_MEAN:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, nowMs, brokerId, value);
                    case ATTRIBUTE_50TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH, nowMs, brokerId, value);
                    case ATTRIBUTE_999TH_PERCENTILE:
                        return new BrokerMetric(RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, nowMs, brokerId, value);
                    default:
                        return null;
                }
            default:
                return null;
        }
    }
}
