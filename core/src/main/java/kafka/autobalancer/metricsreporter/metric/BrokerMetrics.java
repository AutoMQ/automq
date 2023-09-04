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

import kafka.autobalancer.metricsreporter.exception.UnknownVersionException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetrics.
 */
/*
 * A container class to hold broker metric.
 */
public class BrokerMetrics extends AutoBalancerMetrics {

    public BrokerMetrics(long time, int brokerId, String brokerRack) {
        super(time, brokerId, brokerRack);
    }

    public BrokerMetrics(long time, int brokerId, String brokerRack, Map<RawMetricType, Double> metricTypeValueMap) {
        super(time, brokerId, brokerRack, metricTypeValueMap);
    }

    static BrokerMetrics fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
        byte version = buffer.get();
        if (version > METRIC_VERSION) {
            throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                    + "Current version is " + METRIC_VERSION);
        }
        long time = buffer.getLong();
        int brokerId = buffer.getInt();
        int brokerRackLength = buffer.getInt();
        String brokerRack = "";
        if (brokerRackLength > 0) {
            brokerRack = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), brokerRackLength, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + brokerRackLength);
        }
        Map<RawMetricType, Double> metricsMap = parseMetricsMap(buffer);
        return new BrokerMetrics(time, brokerId, brokerRack, metricsMap);
    }

    @Override
    public AutoBalancerMetrics put(RawMetricType type, double value) {
        if (type.metricScope() != RawMetricType.MetricScope.BROKER) {
            throw new IllegalArgumentException(String.format("Cannot construct a BrokerMetric for %s whose scope is %s",
                    type, type.metricScope()));
        }
        return super.put(type, value);
    }

    @Override
    public String key() {
        return Integer.toString(brokerId());
    }

    @Override
    public MetricClassId metricClassId() {
        return MetricClassId.BROKER_METRIC;
    }

    /**
     * The buffer capacity is calculated as follows:
     * <ul>
     *   <li>(headerPos + {@link Byte#BYTES}) - version</li>
     *   <li>{@link Long#BYTES} - time</li>
     *   <li>{@link Integer#BYTES} - broker id</li>
     *   <li>{@link Integer#BYTES} - broker rack length</li>
     *   <li>brokerRack.length - broker rack</li>
     *   <li>body length - metric-value body</li>
     *
     * </ul>
     *
     * @param headerPos Header position
     * @return Byte buffer of the partition metric.
     */
    @Override
    public ByteBuffer toBuffer(int headerPos) {
        byte[] brokerRackBytes = brokerRack().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(headerPos + Byte.BYTES
                + Long.BYTES
                + Integer.BYTES
                + Integer.BYTES
                + brokerRackBytes.length
                + bodySize());
        buffer.position(headerPos);
        buffer.put(METRIC_VERSION);
        buffer.putLong(time());
        buffer.putInt(brokerId());
        buffer.putInt(brokerRackBytes.length);
        if (brokerRackBytes.length > 0) {
            buffer.put(brokerRackBytes);
        }
        buffer = writeBody(buffer);
        return buffer;
    }

    @Override
    public String toString() {
        return String.format("[%s,BrokerId=%d,Time=%d,Key:Value=%s]",
                MetricClassId.BROKER_METRIC, brokerId(), time(), buildKVString());
    }
}

