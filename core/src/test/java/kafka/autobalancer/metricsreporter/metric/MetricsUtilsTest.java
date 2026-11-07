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

import kafka.autobalancer.common.types.RawMetricTypes;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.Map;

@Timeout(60)
@Tag("S3Unit")
public class MetricsUtilsTest {

    @Test
    public void testSanityCheckTopicPartitionMetricsCompleteness() {
        TopicPartitionMetrics metrics = new TopicPartitionMetrics(System.currentTimeMillis(), 1, "", "testTopic", 0);
        metrics.put(RawMetricTypes.PARTITION_BYTES_IN, 10);
        Assertions.assertFalse(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
        metrics.put(RawMetricTypes.PARTITION_BYTES_OUT, 10);
        Assertions.assertFalse(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
        metrics.put(RawMetricTypes.PARTITION_SIZE, 10);
        Assertions.assertTrue(MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics));
    }

    @Test
    public void testToAutoBalancerMetric() {
        KafkaMetricsGroup kafkaMetricsGroup = new KafkaMetricsGroup(MetricsUtilsTest.class);
        long now = System.currentTimeMillis();
        int brokerId = 0;
        String rack = "rack-a";
        MetricName metricName = kafkaMetricsGroup.metricName(MetricsUtils.BYTES_IN_PER_SEC, Map.of(
            "topic", "topic.with.dot",
            "partition", "0"
        ));
        AutoBalancerMetrics metrics = MetricsUtils.toAutoBalancerMetric(now, brokerId, rack, metricName, 10.5);
        Assertions.assertNotNull(metrics);
        Assertions.assertInstanceOf(TopicPartitionMetrics.class, metrics);
        Assertions.assertEquals(10.5, metrics.getMetricValueMap().get(RawMetricTypes.PARTITION_BYTES_IN));
        TopicPartitionMetrics topicPartitionMetrics = (TopicPartitionMetrics) metrics;
        Assertions.assertEquals("topic.with.dot", topicPartitionMetrics.topic());
        Assertions.assertEquals(0, topicPartitionMetrics.partition());

        metricName = kafkaMetricsGroup.metricName(MetricsUtils.BYTES_IN_PER_SEC, Map.of(
            "topic", "topic_without_dot",
            "partition", "1"
        ));
        metrics = MetricsUtils.toAutoBalancerMetric(now, brokerId, rack, metricName, 10.5);
        Assertions.assertNotNull(metrics);
        Assertions.assertInstanceOf(TopicPartitionMetrics.class, metrics);
        Assertions.assertEquals(10.5, metrics.getMetricValueMap().get(RawMetricTypes.PARTITION_BYTES_IN));
        topicPartitionMetrics = (TopicPartitionMetrics) metrics;
        Assertions.assertEquals("topic_without_dot", topicPartitionMetrics.topic());
        Assertions.assertEquals(1, topicPartitionMetrics.partition());

        MetricName metricNameWithEmptyTags = kafkaMetricsGroup.metricName(MetricsUtils.BYTES_IN_PER_SEC, Collections.emptyMap());
        Assertions.assertThrows(IllegalArgumentException.class, () -> MetricsUtils.toAutoBalancerMetric(now, brokerId, rack, metricNameWithEmptyTags, 10.5));
        Map<String, String> tags = MetricsUtils.mBeanNameToTags(metricNameWithEmptyTags.getMBeanName());
        Assertions.assertNotNull(tags);
        Assertions.assertTrue(tags.isEmpty());
    }
}
