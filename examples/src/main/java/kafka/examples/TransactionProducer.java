/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducer {

    public static void main(String... args) {
        // Set the required properties for running the Kafka producer.
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

        // Create a Kafka producer with the provided properties.
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Initialize the transactions for the producer.
        producer.initTransactions();

        try {
            // Begin a new transaction.
            producer.beginTransaction();

            // Send a few messages as part of the transaction.
            for (int i = 0; i < 20; i++) {
                producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));
            }
            // Commit the transaction.
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            // Abort the transaction in case of any exceptions.
            producer.abortTransaction();
        } finally {
            // Close the producer.
            producer.close();
        }
    }
}
