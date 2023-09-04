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

package kafka.autobalancer;

import kafka.autobalancer.config.AutoBalancerConfig;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.goals.NetworkInCapacityGoal;
import kafka.autobalancer.goals.NetworkInDistributionGoal;
import kafka.autobalancer.goals.NetworkOutCapacityGoal;
import kafka.autobalancer.goals.NetworkOutDistributionGoal;
import kafka.autobalancer.metricsreporter.AutoBalancerMetricsReporter;
import kafka.autobalancer.utils.AbstractLoadGenerator;
import kafka.autobalancer.utils.AutoBalancerClientsIntegrationTestHarness;
import kafka.autobalancer.utils.ConsumeLoadGenerator;
import kafka.autobalancer.utils.ProduceLoadGenerator;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;

@Tag("esUnit")
public class AutoBalancerManagerTest extends AutoBalancerClientsIntegrationTestHarness {
    /**
     * Set up the unit test.
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
        props.put(KafkaConfig.OffsetsTopicPartitionsProp(), "1");
        return props;
    }

    @Override
    public Map<String, String> overridingBrokerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, AutoBalancerMetricsReporter.class.getName());
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG, "1000");
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_BROKER_NW_IN_CAPACITY, "100"); // KB/s
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_BROKER_NW_OUT_CAPACITY, "100"); // KB/s

        return props;
    }

    @Override
    public Map<String, String> overridingControllerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_GOALS, new StringJoiner(",")
                        .add(NetworkInCapacityGoal.class.getName())
                        .add(NetworkOutCapacityGoal.class.getName())
                        .add(NetworkInDistributionGoal.class.getName())
                        .add(NetworkOutDistributionGoal.class.getName()).toString());
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_THRESHOLD, "0.2");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_THRESHOLD, "0.2");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, "0.2");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, "0.2");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ANOMALY_DETECT_INTERVAL_MS, "10000");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_ACCEPTED_METRICS_DELAY_MS, "10000");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_LOAD_AGGREGATION, "true");
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_EXCLUDE_TOPICS, "__consumer_offsets," + METRIC_TOPIC);

        return props;
    }

    private Properties createProducerProperties() {
        long randomToken = TestUtils.RANDOM.nextLong();
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "ab_producer-" + randomToken);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return getProducerProperties(producerProps);
    }

    private Properties createConsumerProperties() {
        long randomToken = TestUtils.RANDOM.nextLong();
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ab_consumer-" + randomToken);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ab-group-" + randomToken);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return consumerProps;
    }

    @Disabled
    @Test
    public void testShutdown() throws Exception {
        createTopic(TOPIC_1, Map.of(0, List.of(0), 1, List.of(0)));
        List<AbstractLoadGenerator> producers = new ArrayList<>();
        List<AbstractLoadGenerator> consumers = new ArrayList<>();
        // broker-0: in: 60, out: 60
        AbstractLoadGenerator topic1Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 0, 4);
        AbstractLoadGenerator topic1Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 0);
        producers.add(topic1Partition0Producer);
        consumers.add(topic1Partition0Consumer);

        // broker-1: in: 60, out: 60
        AbstractLoadGenerator topic1Partition1Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 1, 4);
        AbstractLoadGenerator topic1Partition1Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 1);
        producers.add(topic1Partition1Producer);
        consumers.add(topic1Partition1Consumer);

        consumers.forEach(AbstractLoadGenerator::start);
        producers.forEach(AbstractLoadGenerator::start);

        Thread.sleep(5000);

        Assertions.assertTimeout(Duration.ofMillis(5000), () -> producers.forEach(AbstractLoadGenerator::shutdown));
        Assertions.assertTimeout(Duration.ofMillis(5000), () -> consumers.forEach(AbstractLoadGenerator::shutdown));

        System.out.printf("Completed%n");
    }

    @Disabled
    @Test
    public void testMoveActionBalance() throws Exception {
        createTopic(TOPIC_1, Map.of(0, List.of(0), 1, List.of(1)));
        createTopic(TOPIC_2, Map.of(0, List.of(0)));
        List<AbstractLoadGenerator> producers = new ArrayList<>();
        List<AbstractLoadGenerator> consumers = new ArrayList<>();
        // broker-0: in: 90, out: 90
        AbstractLoadGenerator topic1Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 0, 40);
        AbstractLoadGenerator topic1Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 0);
        producers.add(topic1Partition0Producer);
        consumers.add(topic1Partition0Consumer);

        AbstractLoadGenerator topic2Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_2, 0, 50);
        AbstractLoadGenerator topic2Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_2, 0);
        producers.add(topic2Partition0Producer);
        consumers.add(topic2Partition0Consumer);

        // broker-1: in: 10, out: 10
        AbstractLoadGenerator topic1Partition1Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 1, 10);
        AbstractLoadGenerator topic1Partition1Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 1);
        producers.add(topic1Partition1Producer);
        consumers.add(topic1Partition1Consumer);

        consumers.forEach(AbstractLoadGenerator::start);
        producers.forEach(AbstractLoadGenerator::start);
        TestUtils.waitForCondition(() -> getReplicaFor(TOPIC_2, 0) == 1, 30000L, 1000L,
                () -> "failed to reassign");
        producers.forEach(AbstractLoadGenerator::shutdown);
        consumers.forEach(AbstractLoadGenerator::shutdown);

        // check only for msg failed here, leave message integration check to E2E test
        Assertions.assertEquals(0, topic1Partition0Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition0Consumer.msgFailed());

        Assertions.assertEquals(0, topic1Partition1Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition1Consumer.msgFailed());

        Assertions.assertEquals(0, topic2Partition0Producer.msgFailed());
        Assertions.assertEquals(0, topic2Partition0Consumer.msgFailed());
    }

    @Disabled
    @Test
    public void testBrokerShutdown() throws Exception {
        createTopic(TOPIC_1, Map.of(0, List.of(0), 1, List.of(1)));
        List<AbstractLoadGenerator> producers = new ArrayList<>();
        List<AbstractLoadGenerator> consumers = new ArrayList<>();
        // broker-0: in: 60, out: 60
        AbstractLoadGenerator topic1Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 0, 4);
        AbstractLoadGenerator topic1Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 0);
        producers.add(topic1Partition0Producer);
        consumers.add(topic1Partition0Consumer);

        // broker-1: in: 60, out: 60
        AbstractLoadGenerator topic1Partition1Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 1, 4);
        AbstractLoadGenerator topic1Partition1Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 1);
        producers.add(topic1Partition1Producer);
        consumers.add(topic1Partition1Consumer);

        consumers.forEach(AbstractLoadGenerator::start);
        producers.forEach(AbstractLoadGenerator::start);

        System.out.printf("Shutdown broker-1%n");
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> cluster.brokers().get(1).shutdown());
        System.out.printf("Broker-1 shutdown complete%n");
        TestUtils.waitForCondition(() -> {
            Optional<TopicPartitionInfo> partitionInfoOptional1 = getPartitionInfoFor(TOPIC_1, 1);
            Optional<TopicPartitionInfo> partitionInfoOptional2 = getPartitionInfoFor(TOPIC_1, 0);
            if (partitionInfoOptional1.isEmpty() || partitionInfoOptional2.isEmpty()) {
                return false;
            }
            TopicPartitionInfo partitionInfo1 = partitionInfoOptional1.get();
            TopicPartitionInfo partitionInfo2 = partitionInfoOptional2.get();
            if (partitionInfo1.replicas().isEmpty() || partitionInfo2.replicas().isEmpty()) {
                return false;
            }
            return partitionInfo1.leader().id() == 0 && partitionInfo2.leader().id() == 0;
        }, 30000L, 1000L, () -> "failed to reassign replicas from offline broker");

        producers.forEach(AbstractLoadGenerator::shutdown);
        consumers.forEach(AbstractLoadGenerator::shutdown);

        Assertions.assertEquals(0, topic1Partition0Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition0Consumer.msgFailed());

        Assertions.assertEquals(0, topic1Partition1Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition1Consumer.msgFailed());

        System.out.printf("Complete%n");
    }

    @Disabled
    @Test
    public void testControllerShutdown() throws Exception {
        createTopic(TOPIC_1, Map.of(0, List.of(0), 1, List.of(1)));
        createTopic(TOPIC_2, Map.of(0, List.of(0)));
        List<AbstractLoadGenerator> producers = new ArrayList<>();
        List<AbstractLoadGenerator> consumers = new ArrayList<>();
        // broker-0: in: 90, out: 90
        AbstractLoadGenerator topic1Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 0, 40);
        AbstractLoadGenerator topic1Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 0);
        producers.add(topic1Partition0Producer);
        consumers.add(topic1Partition0Consumer);

        AbstractLoadGenerator topic2Partition0Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_2, 0, 50);
        AbstractLoadGenerator topic2Partition0Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_2, 0);
        producers.add(topic2Partition0Producer);
        consumers.add(topic2Partition0Consumer);

        // broker-1: in: 10, out: 10
        AbstractLoadGenerator topic1Partition1Producer = new ProduceLoadGenerator(createProducerProperties(), TOPIC_1, 1, 10);
        AbstractLoadGenerator topic1Partition1Consumer = new ConsumeLoadGenerator(createConsumerProperties(), TOPIC_1, 1);
        producers.add(topic1Partition1Producer);
        consumers.add(topic1Partition1Consumer);

        consumers.forEach(AbstractLoadGenerator::start);
        producers.forEach(AbstractLoadGenerator::start);

        int leaderId = cluster.raftManagers().values().iterator().next().client().leaderAndEpoch().leaderId().getAsInt();
        System.out.printf("Shutdown controller-%d%n", leaderId);
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> cluster.controllers().get(leaderId).shutdown());
        System.out.printf("Controller-%d shutdown complete%n", leaderId);

        TestUtils.waitForCondition(() -> getReplicaFor(TOPIC_2, 0) == 1, 30000L, 1000L,
                () -> "failed to reassign");
        producers.forEach(AbstractLoadGenerator::shutdown);
        consumers.forEach(AbstractLoadGenerator::shutdown);

        Assertions.assertEquals(0, topic1Partition0Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition0Consumer.msgFailed());

        Assertions.assertEquals(0, topic1Partition1Producer.msgFailed());
        Assertions.assertEquals(0, topic1Partition1Consumer.msgFailed());

        Assertions.assertEquals(0, topic2Partition0Producer.msgFailed());
        Assertions.assertEquals(0, topic2Partition0Consumer.msgFailed());
    }
}
