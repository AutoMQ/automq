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

package kafka.autobalancer.metricsreporter.metric;

import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.metricsreporter.exception.UnknownVersionException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
@Tag("S3Unit")
public class MetricSerdeTest {
    private static final long TIME = 123L;
    private static final int BROKER_ID = 0;
    private static final String TOPIC = "topic";
    private static final int PARTITION = 100;
    private static final double VALUE = 0.1;
    private static final double VALUE1 = 0.2;

    @Test
    public void testPartitionMetricSerde() throws UnknownVersionException {
        AutoBalancerMetrics topicPartitionMetrics = new TopicPartitionMetrics(123L, 0, "", TOPIC, PARTITION)
                .put(RawMetricTypes.PARTITION_SIZE, VALUE)
                .put(RawMetricTypes.PARTITION_BYTES_IN, VALUE1);
        AutoBalancerMetrics deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(topicPartitionMetrics));
        assertNotNull(deserialized);
        assertEquals(MetricTypes.TOPIC_PARTITION_METRIC, deserialized.metricType());
        Map<Byte, Double> metricMap = deserialized.getMetricValueMap();
        assertEquals(2, metricMap.size());
        assertTrue(metricMap.containsKey(RawMetricTypes.PARTITION_SIZE));
        assertTrue(metricMap.containsKey(RawMetricTypes.PARTITION_BYTES_IN));
        assertEquals(VALUE, metricMap.get(RawMetricTypes.PARTITION_SIZE), 0.000001);
        assertEquals(VALUE1, metricMap.get(RawMetricTypes.PARTITION_BYTES_IN), 0.000001);
        assertEquals(TIME, deserialized.time());
        assertEquals(BROKER_ID, deserialized.brokerId());
        assertEquals(TOPIC, ((TopicPartitionMetrics) deserialized).topic());
        assertEquals(PARTITION, ((TopicPartitionMetrics) deserialized).partition());
        assertEquals("", deserialized.brokerRack());
    }
}
