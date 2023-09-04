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

package kafka.autobalancer.metricsreporter;

import kafka.autobalancer.config.AutoBalancerConfig;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.RawMetricType;
import kafka.autobalancer.utils.AutoBalancerClientsIntegrationTestHarness;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static kafka.autobalancer.metricsreporter.metric.RawMetricType.ALL_TOPIC_BYTES_IN;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.ALL_TOPIC_BYTES_OUT;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.BROKER_CAPACITY_NW_IN;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.BROKER_CAPACITY_NW_OUT;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.BROKER_CPU_UTIL;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.PARTITION_SIZE;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.TOPIC_PARTITION_BYTES_IN;
import static kafka.autobalancer.metricsreporter.metric.RawMetricType.TOPIC_PARTITION_BYTES_OUT;

@Tag("esUnit")
public class AutoBalancerMetricsReporterTest extends AutoBalancerClientsIntegrationTestHarness {

    /**
     * Setup the unit test.
     */
    @BeforeEach
    public void setUp() {
        super.setUp();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    @Override
    protected Map<String, String> overridingNodeProps() {
        Map<String, String> props = new HashMap<>();
        props.put(AutoBalancerConfig.AUTO_BALANCER_TOPIC_CONFIG, METRIC_TOPIC);
        props.put(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        props.put(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        props.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        props.put(KafkaConfig.DefaultReplicationFactorProp(), "1");
        return props;
    }

    @Override
    public Map<String, String> overridingBrokerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AutoBalancerMetricsReporter.class.getName());
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG, "1000");
        return props;
    }

    @Test
    public void testReportingMetrics() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "testReportingMetrics");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, AutoBalancerMetrics> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(METRIC_TOPIC));
        long startMs = System.currentTimeMillis();
        Set<Integer> expectedBrokerMetricTypes = new HashSet<>(Arrays.asList(
                (int) BROKER_CAPACITY_NW_IN.id(),
                (int) BROKER_CAPACITY_NW_OUT.id(),
                (int) ALL_TOPIC_BYTES_IN.id(),
                (int) ALL_TOPIC_BYTES_OUT.id(),
                (int) BROKER_CPU_UTIL.id()));
        Set<Integer> expectedTopicPartitionMetricTypes = new HashSet<>(Arrays.asList(
                (int) TOPIC_PARTITION_BYTES_IN.id(),
                (int) TOPIC_PARTITION_BYTES_OUT.id(),
                (int) PARTITION_SIZE.id()));
        Set<Integer> expectedMetricTypes = new HashSet<>(expectedBrokerMetricTypes);
        expectedMetricTypes.addAll(expectedTopicPartitionMetricTypes);

        Set<Integer> metricTypes = new HashSet<>();
        ConsumerRecords<String, AutoBalancerMetrics> records;
        while (metricTypes.size() < (expectedBrokerMetricTypes.size() + expectedTopicPartitionMetricTypes.size())
                && System.currentTimeMillis() < startMs + 15000) {
            records = consumer.poll(Duration.ofMillis(10L));
            for (ConsumerRecord<String, AutoBalancerMetrics> record : records) {
                AutoBalancerMetrics metrics = record.value();
                Set<Integer> localMetricTypes = new HashSet<>();
                for (RawMetricType type : record.value().getMetricTypeValueMap().keySet()) {
                    int typeId = type.id();
                    metricTypes.add(typeId);
                    localMetricTypes.add(typeId);
                }
                Set<Integer> expectedMap = metrics.metricClassId() == AutoBalancerMetrics.MetricClassId.BROKER_METRIC ?
                        expectedBrokerMetricTypes : expectedTopicPartitionMetricTypes;
                Assertions.assertEquals(expectedMap, localMetricTypes, "Expected " + expectedMap + ", but saw " + localMetricTypes);
            }
        }
        Assertions.assertEquals(expectedMetricTypes, metricTypes, "Expected " + expectedMetricTypes + ", but saw " + metricTypes);
    }
}
