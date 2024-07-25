/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.metricsreporter.metric;

import kafka.autobalancer.common.types.MetricTypes;
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
        byteBuffer.put(METRIC_TYPE_OFFSET, metric.metricType());
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

        switch (buffer.get()) {
            case MetricTypes.TOPIC_PARTITION_METRIC:
                return TopicPartitionMetrics.fromBuffer(buffer);
            case MetricTypes.BROKER_METRIC:
                return BrokerMetrics.fromBuffer(buffer);
            default:
                // This could happen when a new type of metric is added, but we are still running the old code.
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
