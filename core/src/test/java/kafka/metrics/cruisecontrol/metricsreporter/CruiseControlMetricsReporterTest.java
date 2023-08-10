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

package kafka.metrics.cruisecontrol.metricsreporter;

import kafka.metrics.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import kafka.metrics.cruisecontrol.metricsreporter.metric.MetricSerde;
import kafka.metrics.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CPU_UTIL;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_50TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_999TH;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MAX;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_PRODUCE_TOTAL_TIME_MS_MEAN;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_REQUEST_QUEUE_SIZE;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_RESPONSE_QUEUE_SIZE;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.TOPIC_PARTITION_BYTES_IN;
import static kafka.metrics.cruisecontrol.metricsreporter.metric.RawMetricType.TOPIC_PARTITION_BYTES_OUT;

public class CruiseControlMetricsReporterTest extends CCKafkaClientsIntegrationTestHarness {
    protected static final String TOPIC = "CruiseControlMetricsReporterTest";

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
    public Properties overridingProps() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, CruiseControlMetricsReporter.class.getName());
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");

        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        props.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "2");
        return props;
    }

    @Test
    public void testReportingMetrics() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testReportingMetrics");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, CruiseControlMetric> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(TOPIC));
        long startMs = System.currentTimeMillis();
        HashSet<Integer> expectedMetricTypes = new HashSet<>(Arrays.asList(
            (int) TOPIC_PARTITION_BYTES_IN.id(),
            (int) TOPIC_PARTITION_BYTES_OUT.id(),
            (int) BROKER_CPU_UTIL.id(),
            (int) BROKER_REQUEST_QUEUE_SIZE.id(),
            (int) BROKER_RESPONSE_QUEUE_SIZE.id(),
            (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX.id(),
            (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN.id(),
            (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX.id(),
            (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN.id(),
            (int) BROKER_PRODUCE_TOTAL_TIME_MS_MAX.id(),
            (int) BROKER_PRODUCE_TOTAL_TIME_MS_MEAN.id(),
            (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX.id(),
            (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN.id(),
            (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH.id(),
            (int) BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH.id(),
            (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH.id(),
            (int) BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH.id(),
            (int) BROKER_PRODUCE_TOTAL_TIME_MS_50TH.id(),
            (int) BROKER_PRODUCE_TOTAL_TIME_MS_999TH.id(),
            (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH.id(),
            (int) BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH.id()));
        Set<Integer> metricTypes = new HashSet<>();
        ConsumerRecords<String, CruiseControlMetric> records;
        while (metricTypes.size() < expectedMetricTypes.size() && System.currentTimeMillis() < startMs + 15000) {
            records = consumer.poll(Duration.ofMillis(10L));
            for (ConsumerRecord<String, CruiseControlMetric> record : records) {
                metricTypes.add((int) record.value().rawMetricType().id());
            }
        }
        Assertions.assertEquals(expectedMetricTypes, metricTypes, "Expected " + expectedMetricTypes + ", but saw " + metricTypes);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUpdatingMetricsTopicConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        AdminClient adminClient = AdminClient.create(props);
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(TOPIC)).values().get(TOPIC).get();
        Assertions.assertEquals(1, topicDescription.partitions().size());
        // Shutdown broker
        BrokerServer brokerServer = cluster.brokers().values().iterator().next();
        brokerServer.shutdown();

        Map<String, String> brokerConfig = (Map<String, String>) brokerServer.sharedServer().brokerConfig().props();

        // Change broker config
        brokerConfig.put(CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "2");
        brokerServer.sharedServer().brokerConfig().updateCurrentConfig(new KafkaConfig(brokerConfig));

        // Restart broker
        brokerServer.startup();
        // Wait for broker to boot up
        Thread.sleep(5000);
        // Check whether the topic config is updated
        topicDescription = adminClient.describeTopics(Collections.singleton(TOPIC)).values().get(TOPIC).get();
        Assertions.assertEquals(2, topicDescription.partitions().size());
    }
}
