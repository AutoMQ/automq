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

import kafka.autobalancer.common.types.RawMetricTypes;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class AutoBalancerMetricsTest {

    private static final long TIME = 123L;
    private static final int BROKER_ID = 0;
    private static final String BROKER_RACK = "rack-1";
    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;

    @Test
    public void testBodySizeCalculation() {
        // Test 1: Empty metrics - bodySize = Integer.BYTES (metrics count) = 4 bytes
        TopicPartitionMetrics emptyMetrics = new TopicPartitionMetrics(TIME, BROKER_ID, BROKER_RACK, TOPIC, PARTITION);
        assertEquals(4, emptyMetrics.bodySize(), "Empty metrics should have bodySize of 4 bytes (count only)");

        // Test 2: Single metric - bodySize = 4 + (1 + 8) * 1 = 13 bytes
        TopicPartitionMetrics singleMetric = new TopicPartitionMetrics(TIME, BROKER_ID, BROKER_RACK, TOPIC, PARTITION);
        singleMetric.put(RawMetricTypes.PARTITION_SIZE, 100.0);
        assertEquals(13, singleMetric.bodySize(), "Single metric should have bodySize of 13 bytes");

        // Verify bodySize matches actual bytes written for single metric
        ByteBuffer buffer1 = ByteBuffer.allocate(singleMetric.bodySize());
        singleMetric.writeBody(buffer1);
        assertEquals(singleMetric.bodySize(), buffer1.position(), "bodySize should match actual bytes written for single metric");

        // Test 3: Multiple metrics (3) - bodySize = 4 + (1 + 8) * 3 = 31 bytes
        TopicPartitionMetrics multiMetrics = new TopicPartitionMetrics(TIME, BROKER_ID, BROKER_RACK, TOPIC, PARTITION);
        multiMetrics.put(RawMetricTypes.PARTITION_SIZE, 100.0);
        multiMetrics.put(RawMetricTypes.PARTITION_BYTES_IN, 200.0);
        multiMetrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 300.0);
        assertEquals(31, multiMetrics.bodySize(), "Three metrics should have bodySize of 31 bytes");

        // Verify bodySize matches actual bytes written for multiple metrics
        ByteBuffer buffer3 = ByteBuffer.allocate(multiMetrics.bodySize());
        multiMetrics.writeBody(buffer3);
        assertEquals(multiMetrics.bodySize(), buffer3.position(), "bodySize should match actual bytes written for multiple metrics");

        // Test 4: Ten metrics - verifies the 8x over-allocation bug fix
        // Before fix: (32 + (8 + 64) * 10) = 1000 (incorrect - using bit sizes)
        // After fix:  (4 + (1 + 8) * 10) = 94 (correct - using byte sizes)
        TopicPartitionMetrics tenMetrics = new TopicPartitionMetrics(TIME, BROKER_ID, BROKER_RACK, TOPIC, PARTITION);
        for (byte i = 0; i < 10; i++) {
            tenMetrics.put(i, i * 10.0);
        }
        assertEquals(94, tenMetrics.bodySize(), "Ten metrics should have bodySize of 94 bytes (not 1000 with old bit-based calculation)");

        // Verify bodySize matches actual bytes written for ten metrics
        ByteBuffer buffer10 = ByteBuffer.allocate(tenMetrics.bodySize());
        tenMetrics.writeBody(buffer10);
        assertEquals(94, buffer10.position(), "bodySize should match actual bytes written for ten metrics");

        // Test 5: BrokerMetrics - bodySize = 4 + (1 + 8) * 2 = 22 bytes
        BrokerMetrics brokerMetrics = new BrokerMetrics(TIME, BROKER_ID, BROKER_RACK);
        brokerMetrics.put(RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS, 1.5);
        brokerMetrics.put(RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS, 3.0);
        assertEquals(22, brokerMetrics.bodySize(), "BrokerMetrics with 2 metrics should have bodySize of 22 bytes");

        // Verify bodySize matches actual bytes written for BrokerMetrics
        ByteBuffer brokerBuffer = ByteBuffer.allocate(brokerMetrics.bodySize());
        brokerMetrics.writeBody(brokerBuffer);
        assertEquals(brokerMetrics.bodySize(), brokerBuffer.position(), "bodySize should match actual bytes written for BrokerMetrics");
    }
}
