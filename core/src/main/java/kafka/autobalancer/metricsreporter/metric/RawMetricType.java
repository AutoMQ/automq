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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static kafka.autobalancer.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.MetricScope.PARTITION;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.
 */
/*
 * The metric type helps the metric sampler to distinguish what metric a value is representing. These metrics are
 * called raw metrics because they are the most basic information reported by the Kafka brokers without any processing.
 * Each metric type has an id for serde purpose.
 */
public enum RawMetricType {
    BROKER_CAPACITY_NW_IN(BROKER, (byte) 0, (byte) 0),
    BROKER_CAPACITY_NW_OUT(BROKER, (byte) 1, (byte) 0),
    ALL_TOPIC_BYTES_IN(BROKER, (byte) 2, (byte) 0),
    ALL_TOPIC_BYTES_OUT(BROKER, (byte) 3, (byte) 0),
    TOPIC_PARTITION_BYTES_IN(PARTITION, (byte) 4, (byte) 0),
    TOPIC_PARTITION_BYTES_OUT(PARTITION, (byte) 5, (byte) 0),
    PARTITION_SIZE(PARTITION, (byte) 6, (byte) 0),
    BROKER_CPU_UTIL(BROKER, (byte) 7, (byte) 0);

    private static final List<RawMetricType> CACHED_VALUES = List.of(RawMetricType.values());
    private static final SortedMap<Byte, Set<RawMetricType>> BROKER_METRIC_TYPES_DIFF_BY_VERSION = buildBrokerMetricTypesDiffByVersion();
    private static final List<RawMetricType> BROKER_METRIC_TYPES = Collections.unmodifiableList(buildMetricTypeList(BROKER));
    private static final List<RawMetricType> PARTITION_METRIC_TYPES = Collections.unmodifiableList(buildMetricTypeList(PARTITION));
    private final byte id;
    private final MetricScope metricScope;
    private final byte supportedVersionSince;

    RawMetricType(MetricScope scope, byte id) {
        this(scope, id, (byte) -1);
    }

    RawMetricType(MetricScope scope, byte id, byte supportedVersionSince) {
        this.id = id;
        metricScope = scope;
        this.supportedVersionSince = supportedVersionSince;
    }

    public static List<RawMetricType> allMetricTypes() {
        return Collections.unmodifiableList(CACHED_VALUES);
    }

    public static Map<Byte, Set<RawMetricType>> brokerMetricTypesDiffByVersion() {
        return BROKER_METRIC_TYPES_DIFF_BY_VERSION;
    }

    public static Set<RawMetricType> brokerMetricTypesDiffForVersion(byte version) {
        return BROKER_METRIC_TYPES_DIFF_BY_VERSION.get(version);
    }

    public static List<RawMetricType> partitionMetricTypes() {
        return PARTITION_METRIC_TYPES;
    }

    public static List<RawMetricType> brokerMetricTypes() {
        return BROKER_METRIC_TYPES;
    }

    /**
     * @param id Auto Balancer Metric type.
     * @return Raw metric type.
     */
    public static RawMetricType forId(byte id) {
        if (id < values().length) {
            return values()[id];
        } else {
            throw new IllegalArgumentException("AutoBalancerMetric type " + id + " does not exist.");
        }
    }

    private static SortedMap<Byte, Set<RawMetricType>> buildBrokerMetricTypesDiffByVersion() {
        SortedMap<Byte, Set<RawMetricType>> buildBrokerMetricTypesDiffByVersion = new TreeMap<>();
        for (RawMetricType type : RawMetricType.values()) {
            if (type.metricScope() == BROKER) {
                buildBrokerMetricTypesDiffByVersion.computeIfAbsent(type.supportedVersionSince(), t -> new HashSet<>()).add(type);
            }
        }

        return buildBrokerMetricTypesDiffByVersion;
    }

    private static List<RawMetricType> buildMetricTypeList(MetricScope metricScope) {
        List<RawMetricType> brokerMetricTypes = new ArrayList<>();
        for (RawMetricType type : RawMetricType.values()) {
            if (type.metricScope() == metricScope) {
                brokerMetricTypes.add(type);
            }
        }
        return brokerMetricTypes;
    }

    public byte id() {
        return id;
    }

    public MetricScope metricScope() {
        return metricScope;
    }

    public byte supportedVersionSince() {
        return supportedVersionSince;
    }

    public enum MetricScope {
        BROKER, PARTITION
    }
}
