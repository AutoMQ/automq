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

package kafka.autobalancer.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class AutoBalancerClientsIntegrationTestHarness extends AutoBalancerIntegrationTestHarness {
    protected static final String TOPIC_0 = "TestTopic";
    protected static final String TOPIC_1 = "TestTopic1";
    protected static final String TOPIC_2 = "TestTopic2";
    protected static final String TOPIC_3 = "TestTopic3";
    protected static final String TOPIC_4 = "TestTopic4";

    protected AdminClient adminClient;

    @Override
    public void tearDown() {
        this.adminClient.close();
        super.tearDown();
    }

    @Override
    public void setUp() {
        super.setUp();
        Properties adminProps = new Properties();
        adminProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        adminClient = AdminClient.create(adminProps);
        try {
            createTopic(TOPIC_0, 1, (short) 1);
        } catch (Exception e) {
            Assertions.fail("failed to create test topic");
        }

        // starting producer to verify that Kafka cluster is working fine
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        AtomicInteger producerFailed = new AtomicInteger(0);
        try (Producer<String, String> producer = createProducer(producerProps)) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(TOPIC_0, Integer.toString(i)),
                        (m, e) -> {
                            if (e != null) {
                                producerFailed.incrementAndGet();
                            }
                        });
            }
        }
        Assertions.assertEquals(0, producerFailed.get());
    }

    protected boolean checkTopicCreation(String topicName, int numPartitions) {
        try {
            Map<String, TopicDescription> topics = adminClient.describeTopics(List.of(topicName)).allTopicNames().get();
            TopicDescription desc = topics.get(topicName);
            if (desc == null) {
                return false;
            }
            return desc.partitions().size() == numPartitions;
        } catch (Exception e) {
            return false;
        }
    }

    protected void createTopic(String topicName, int numPartitions, short replicaFactor) throws Exception {
        NewTopic topic = new NewTopic(topicName, numPartitions, replicaFactor);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));
        Assertions.assertNull(createTopicsResult.values().get(topicName).get());
        TestUtils.waitForCondition(() -> checkTopicCreation(topicName, numPartitions), 5000L, 1000L,
                () -> "topic not ready in given time");
    }

    protected void createTopic(String topicName, Map<Integer, List<Integer>> replicasAssignments) throws Exception {
        NewTopic topic = new NewTopic(topicName, replicasAssignments);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));
        Assertions.assertNull(createTopicsResult.values().get(topicName).get());
        TestUtils.waitForCondition(() -> checkTopicCreation(topicName, replicasAssignments.size()), 5000L, 1000L,
                () -> "topic not ready in given time");
    }

    protected Producer<String, String> createProducer(Properties overrides) {
        Properties props = getProducerProperties(overrides);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Assertions.assertNotNull(producer);
        return producer;
    }

    protected Properties getProducerProperties(Properties overrides) {
        Properties result = new Properties();

        //populate defaults
        result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        //apply overrides
        if (overrides != null) {
            result.putAll(overrides);
        }

        return result;
    }

    protected int getReplicaFor(String topic, int partition) {
        try {
            Map<String, TopicDescription> topics = adminClient.describeTopics(List.of(topic)).allTopicNames().get();
            TopicDescription desc = topics.get(topic);
            if (desc == null) {
                return -1;
            }
            Optional<TopicPartitionInfo> optionalTp = desc.partitions().stream().filter(p -> p.partition() == partition).findFirst();
            if (optionalTp.isEmpty()) {
                return -1;
            }
            if (optionalTp.get().replicas().isEmpty()) {
                return -1;
            }
            return optionalTp.get().replicas().get(0).id();
        } catch (Exception e) {
            return -1;
        }
    }

    protected Optional<TopicPartitionInfo> getPartitionInfoFor(String topic, int partition) {
        try {
            Map<String, TopicDescription> topics = adminClient.describeTopics(List.of(topic)).allTopicNames().get();
            TopicDescription desc = topics.get(topic);
            if (desc == null) {
                return Optional.empty();
            }
            return desc.partitions().stream().filter(p -> p.partition() == partition).findFirst();
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
