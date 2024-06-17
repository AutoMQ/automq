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

import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.utils.AutoBalancerClientsIntegrationTestHarness;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Tag("S3Unit")
// TODO fix it
@Disabled
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
        props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, "1");
        props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1");
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
        try (Consumer<String, AutoBalancerMetrics> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME));
            long startMs = System.currentTimeMillis();
            Set<Byte> expectedBrokerMetricTypes = new HashSet<>(RawMetricTypes.BROKER_METRICS);
            Set<Byte> expectedPartitionMetricTypes = new HashSet<>(RawMetricTypes.PARTITION_METRICS);
            Set<Byte> expectedMetricTypes = new HashSet<>();
            expectedMetricTypes.addAll(expectedBrokerMetricTypes);
            expectedMetricTypes.addAll(expectedPartitionMetricTypes);

            Set<Byte> metricTypes = new HashSet<>();
            ConsumerRecords<String, AutoBalancerMetrics> records;
            while (metricTypes.size() < expectedMetricTypes.size() && System.currentTimeMillis() < startMs + 15000) {
                records = consumer.poll(Duration.ofMillis(10L));
                for (ConsumerRecord<String, AutoBalancerMetrics> record : records) {
                    Set<Byte> localMetricTypes = new HashSet<>();
                    AutoBalancerMetrics metrics = record.value();
                    Assertions.assertNotNull(metrics);
                    for (Byte type : metrics.getMetricValueMap().keySet()) {
                        metricTypes.add(type);
                        localMetricTypes.add(type);
                    }
                    if (metrics.metricType() == MetricTypes.BROKER_METRIC) {
                        Assertions.assertEquals(expectedBrokerMetricTypes, localMetricTypes,
                                "Expected " + expectedBrokerMetricTypes + ", but saw " + localMetricTypes);
                    } else if (metrics.metricType() == MetricTypes.TOPIC_PARTITION_METRIC) {
                        Assertions.assertEquals(expectedPartitionMetricTypes, localMetricTypes,
                                "Expected " + expectedPartitionMetricTypes + ", but saw " + localMetricTypes);
                    } else {
                        Assertions.fail("Unexpected metric type: " + metrics.metricType());
                    }
                }
            }
            Assertions.assertEquals(expectedMetricTypes, metricTypes, "Expected " + expectedMetricTypes + ", but saw " + metricTypes);
        }
    }
}
