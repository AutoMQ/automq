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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricSerde.
 */
public class MetricSerde implements Serializer<AutoBalancerMetrics>, Deserializer<AutoBalancerMetrics> {

    // The overhead of the type bytes
    private static final int METRIC_TYPE_OFFSET = 0;
    private static final int HEADER_LENGTH = 1;

    /**
     * Serialize the Auto Balancer metric to a byte array.
     *
     * @param metric Metric to be serialized.
     * @return Serialized Auto Balancer metric as a byte array.
     */
    public static byte[] toBytes(AutoBalancerMetrics metric) {
        ByteBuffer byteBuffer = metric.toBuffer(HEADER_LENGTH);
        byteBuffer.put(METRIC_TYPE_OFFSET, metric.metricClassId().id());
        return byteBuffer.array();
    }

    /**
     * Deserialize from byte array to Auto Balancer metric
     *
     * @param bytes Bytes array corresponding to Auto Balancer metric.
     * @return Deserialized byte array as Auto Balancer metric.
     */
    public static AutoBalancerMetrics fromBytes(byte[] bytes) throws UnknownVersionException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        switch (AutoBalancerMetrics.MetricClassId.forId(buffer.get())) {
            case BROKER_METRIC:
                return BrokerMetrics.fromBuffer(buffer);
            case PARTITION_METRIC:
                return TopicPartitionMetrics.fromBuffer(buffer);
            default:
                // This could happen when a new type of metric is added but we are still running the old code.
                // simply ignore the metric by returning a null.
                return null;
        }
    }

    @Override
    public AutoBalancerMetrics deserialize(String topic, byte[] bytes) {
        try {
            return fromBytes(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred when deserialize auto balancer metrics.", e);
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, AutoBalancerMetrics autoBalancerMetric) {
        return toBytes(autoBalancerMetric);
    }

    @Override
    public void close() {

    }
}
