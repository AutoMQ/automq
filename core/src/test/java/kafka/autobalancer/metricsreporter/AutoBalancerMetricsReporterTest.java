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

import java.util.HashMap;
import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.config.StaticAutoBalancerConfig;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.network.SocketServerConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.mockito.Mockito;

@Tag("S3Unit")
public class AutoBalancerMetricsReporterTest {

    @Test
    @Disabled
    public void testReportingMetrics() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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

    @Test
    public void testBootstrapServersConfig() {
        AutoBalancerMetricsReporter reporter = Mockito.mock(AutoBalancerMetricsReporter.class);
        Mockito.doCallRealMethod().when(reporter).getBootstrapServers(Mockito.anyMap(), Mockito.anyString());

        // test default config
        StaticAutoBalancerConfig staticConfig = new StaticAutoBalancerConfig(new HashMap<>(), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test default config with multiple listeners
        StaticAutoBalancerConfig staticConfig1 = new StaticAutoBalancerConfig(new HashMap<>(), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,BROKER://127.0.0.1:9092"
        ), staticConfig1.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test illegal listener
        StaticAutoBalancerConfig staticConfig2 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "CONTROLLER"
        ), false);
        Assertions.assertThrows(ConfigException.class, () -> reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://127.0.0.1:9092"
        ), staticConfig2.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test not existed listener
        StaticAutoBalancerConfig staticConfig3 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "BROKER"
        ), false);
        Assertions.assertThrows(ConfigException.class, () -> reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig3.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test valid listener
        StaticAutoBalancerConfig staticConfig4 = new StaticAutoBalancerConfig(Map.of(
            StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "PLAINTEXT"
        ), false);
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://127.0.0.1:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test multiple listeners
        Assertions.assertEquals("127.0.0.1:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,PLAINTEXT://127.0.0.1:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

        // test default hostname
        Assertions.assertEquals("localhost:9092", reporter.getBootstrapServers(Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://:9093,PLAINTEXT://:9092"
        ), staticConfig4.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG)));

    }
}
