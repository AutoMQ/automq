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

import kafka.autobalancer.common.RawMetricType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric.
 */
/*
 * An interface for all the raw metrics reported by {@link AutoBalancerMetricsReporter}.
 */
public abstract class AutoBalancerMetrics {
    private final Map<RawMetricType, Double> metricValueMap = new HashMap<>();
    private final long time;
    private final int brokerId;
    private final String brokerRack;

    public AutoBalancerMetrics(long time, int brokerId, String brokerRack) {
        this(time, brokerId, brokerRack, Collections.emptyMap());
    }

    public AutoBalancerMetrics(long time, int brokerId, String brokerRack, Map<RawMetricType, Double> metricValueMap) {
        this.time = time;
        this.brokerId = brokerId;
        this.brokerRack = brokerRack;
        this.metricValueMap.putAll(metricValueMap);
    }

    static Map<RawMetricType, Double> parseMetricsMap(ByteBuffer buffer) {
        Map<RawMetricType, Double> metricsMap = new HashMap<>();
        int metricNumber = buffer.getInt();
        for (int i = 0; i < metricNumber; i++) {
            byte id = buffer.get();
            double value = buffer.getDouble();
            metricsMap.put(RawMetricType.forId(id), value);
        }
        return metricsMap;
    }

    public AutoBalancerMetrics put(RawMetricType type, double value) {
        this.metricValueMap.put(type, value);
        return this;
    }

    public void add(AutoBalancerMetrics metrics) {
        for (Map.Entry<RawMetricType, Double> metricEntry : metrics.metricValueMap.entrySet()) {
            this.metricValueMap.putIfAbsent(metricEntry.getKey(), metricEntry.getValue());
        }
    }

    public Map<RawMetricType, Double> getMetricValueMap() {
        return metricValueMap;
    }

    public abstract String key();

    /**
     * @return the metric class id for this metric. The metric class id will be stored in the serialized metrics
     * so that the deserializer will know which class should be used to deserialize the data.
     */
    public abstract MetricClassId metricClassId();

    /**
     * @return the timestamp for this metric.
     */
    public long time() {
        return time;
    }

    /**
     * @return the broker id who reported this metric.
     */
    public int brokerId() {
        return brokerId;
    }

    public String brokerRack() {
        return brokerRack;
    }

    public int bodySize() {
        return Integer.SIZE + (Byte.SIZE + Double.SIZE) * metricValueMap.size();
    }

    public ByteBuffer writeBody(ByteBuffer buffer) {
        buffer.putInt(metricValueMap.size());
        for (Map.Entry<RawMetricType, Double> entry : metricValueMap.entrySet()) {
            buffer.put(entry.getKey().id());
            buffer.putDouble(entry.getValue());
        }
        return buffer;
    }

    /**
     * Serialize the metric to a byte buffer with the header size reserved.
     *
     * @param headerSize the header size to reserve.
     * @return A ByteBuffer with header size reserved at the beginning.
     */
    abstract ByteBuffer toBuffer(int headerSize);

    public String buildKVString() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<RawMetricType, Double> entry : metricValueMap.entrySet()) {
            builder.append(entry.getKey());
            builder.append(":");
            builder.append(String.format("%.4f", entry.getValue()));
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return String.format("[BrokerId=%d,Time=%d,Key:Value=%s]", brokerId, time, buildKVString());
    }
}
