/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.autobalancer.metricsreporter.metric;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric.
 */
/*
 * An interface for all the raw metrics reported by {@link AutoBalancerMetricsReporter}.
 */
public abstract class AutoBalancerMetrics {
    public static final byte METRIC_VERSION = 0;
    private final Map<Byte, Double> metricValueMap;
    private final long time;
    private final int brokerId;
    private final String brokerRack;

    public AutoBalancerMetrics(long time, int brokerId, String brokerRack) {
        this(time, brokerId, brokerRack, new HashMap<>());
    }

    public AutoBalancerMetrics(long time, int brokerId, String brokerRack, Map<Byte, Double> metricValueMap) {
        this.time = time;
        this.brokerId = brokerId;
        this.brokerRack = brokerRack;
        this.metricValueMap = metricValueMap;
    }

    protected static Map<Byte, Double> parseMetricsMap(ByteBuffer buffer) {
        Map<Byte, Double> metricsMap = new HashMap<>();
        int metricNumber = buffer.getInt();
        for (int i = 0; i < metricNumber; i++) {
            byte id = buffer.get();
            double value = buffer.getDouble();
            metricsMap.put(id, value);
        }
        return metricsMap;
    }

    public AutoBalancerMetrics put(byte type, double value) {
        this.metricValueMap.put(type, value);
        return this;
    }

    public void add(AutoBalancerMetrics metrics) {
        this.metricValueMap.putAll(metrics.metricValueMap);
    }

    public Map<Byte, Double> getMetricValueMap() {
        return metricValueMap;
    }

    public abstract String key();

    /**
     * @return the metric type for this metric. The metric type will be stored in the serialized metrics
     * so that the deserializer will know which class should be used to deserialize the data.
     */
    public abstract byte metricType();

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
        for (Map.Entry<Byte, Double> entry : metricValueMap.entrySet()) {
            buffer.put(entry.getKey());
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
    protected abstract ByteBuffer toBuffer(int headerSize);

    public String buildKVString() {
        StringBuilder builder = new StringBuilder();
        metricValueMap.forEach((k, v) -> builder.append(k).append(":").append(v).append(","));
        if (builder.length() == 0) {
            return "";
        }
        return builder.substring(0, builder.length() - 1);
    }

    @Override
    public String toString() {
        return String.format("[BrokerId=%d,Time=%d,Key:Value=%s]", brokerId, time, buildKVString());
    }
}
