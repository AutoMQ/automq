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

package kafka.metrics.cruisecontrol.metricsreporter.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class CCKafkaClientsIntegrationTestHarness extends CCKafkaIntegrationTestHarness {
    protected static final String TEST_TOPIC = "TestTopic";

    @Override
    public void setUp() {
        super.setUp();

        AdminClient adminClient = null;
        try {
            // creating the "TestTopic" explicitly because the topic auto-creation is disabled on the broker
            Properties adminProps = new Properties();
            adminProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
            adminClient = AdminClient.create(adminProps);
            NewTopic testTopic = new NewTopic(TEST_TOPIC, 1, (short) 1);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(testTopic));

            AtomicInteger adminFailed = new AtomicInteger(0);
            createTopicsResult.all().whenComplete((v, e) -> {
                if (e != null) {
                    adminFailed.incrementAndGet();
                }
            });
            Assertions.assertEquals(0, adminFailed.get());

            // starting producer to verify that Kafka cluster is working fine
            Properties producerProps = new Properties();
            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
            AtomicInteger producerFailed = new AtomicInteger(0);
            try (Producer<String, String> producer = createProducer(producerProps)) {
                for (int i = 0; i < 10; i++) {
                    producer.send(new ProducerRecord<>(TEST_TOPIC, Integer.toString(i)),
                        (m, e) -> {
                            if (e != null) {
                                producerFailed.incrementAndGet();
                            }
                        });
                }
            }
            Assertions.assertEquals(0, producerFailed.get());
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
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
}
