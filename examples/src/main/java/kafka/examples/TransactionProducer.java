/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1");

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
