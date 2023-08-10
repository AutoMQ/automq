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

import kafka.metrics.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricSerdeTest {
    private static final long TIME = 123L;
    private static final int BROKER_ID = 0;
    private static final String TOPIC = "topic";
    private static final int PARTITION = 100;
    private static final double VALUE = 0.1;

    @Test
    public void testBrokerMetricSerde() throws UnknownVersionException {
        BrokerMetric brokerMetric = new BrokerMetric(RawMetricType.ALL_TOPIC_BYTES_IN, 123L, 0, 0.1);
        CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(brokerMetric));
        assertEquals(CruiseControlMetric.MetricClassId.BROKER_METRIC.id(), deserialized.metricClassId().id());
        assertEquals(RawMetricType.ALL_TOPIC_BYTES_IN.id(), deserialized.rawMetricType().id());
        assertEquals(TIME, deserialized.time());
        assertEquals(BROKER_ID, deserialized.brokerId());
        assertEquals(VALUE, deserialized.value(), 0.000001);
    }

    @Test
    public void testTopicMetricSerde() throws UnknownVersionException {
        TopicMetric topicMetric = new TopicMetric(RawMetricType.TOPIC_BYTES_IN, 123L, 0, TOPIC, 0.1);
        CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(topicMetric));
        assertEquals(CruiseControlMetric.MetricClassId.TOPIC_METRIC.id(), deserialized.metricClassId().id());
        assertEquals(RawMetricType.TOPIC_BYTES_IN.id(), deserialized.rawMetricType().id());
        assertEquals(TIME, deserialized.time());
        assertEquals(BROKER_ID, deserialized.brokerId());
        assertEquals(TOPIC, ((TopicMetric) deserialized).topic());
        assertEquals(VALUE, deserialized.value(), 0.000001);
    }

    @Test
    public void testPartitionMetricSerde() throws UnknownVersionException {
        PartitionMetric partitionMetric = new PartitionMetric(RawMetricType.PARTITION_SIZE, 123L, 0, TOPIC, PARTITION, 0.1);
        CruiseControlMetric deserialized = MetricSerde.fromBytes(MetricSerde.toBytes(partitionMetric));
        assertEquals(CruiseControlMetric.MetricClassId.PARTITION_METRIC.id(), deserialized.metricClassId().id());
        assertEquals(RawMetricType.PARTITION_SIZE.id(), deserialized.rawMetricType().id());
        assertEquals(TIME, deserialized.time());
        assertEquals(BROKER_ID, deserialized.brokerId());
        assertEquals(TOPIC, ((PartitionMetric) deserialized).topic());
        assertEquals(PARTITION, ((PartitionMetric) deserialized).partition());
        assertEquals(VALUE, deserialized.value(), 0.000001);
    }
}
