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

import kafka.metrics.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Tag("esUnit")
public class CruiseControlMetricsReporterAutoCreateTopicTest extends CCKafkaClientsIntegrationTestHarness {
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
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "100");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG, TOPIC);
        // configure metrics topic auto-creation by the metrics reporter
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG, "5000");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG, "1");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
        props.setProperty(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        // disable topic auto-creation to leave the metrics reporter to create the metrics topic
        props.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
        props.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
        props.setProperty(KafkaConfig.DefaultReplicationFactorProp(), "2");
        props.setProperty(KafkaConfig.NumPartitionsProp(), "2");
        return props;
    }

    @Test
    public void testAutoCreateMetricsTopic() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        AdminClient adminClient = AdminClient.create(props);
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(TOPIC)).values().get(TOPIC).get();
        // assert that the metrics topic was created with partitions and replicas as configured for the metrics report auto-creation
        Assertions.assertEquals(1, topicDescription.partitions().size());
        Assertions.assertEquals(1, topicDescription.partitions().get(0).replicas().size());
    }
}
