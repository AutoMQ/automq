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

package org.apache.kafka.tools.automq.perf;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);
    private final List<Producer> producers = new LinkedList<>();

    /**
     * Create producers for the given topics. NOT thread-safe.
     *
     * @param topics topic names
     * @param config producer configuration
     * @return number of producers created
     */
    public int createProducers(List<String> topics, ProducersConfig config) {
        int count = 0;
        for (String topic : topics) {
            for (int i = 0; i < config.producersPerTopic; i++) {
                Producer producer = createProducer(topic, config);
                producers.add(producer);
                count++;
            }
        }
        return count;
    }

    private Producer createProducer(String topic, ProducersConfig config) {
        Properties properties = new Properties();
        properties.putAll(config.producerConfigs);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);

        KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(properties);
        return new Producer(kafkaProducer, topic);
    }

    @Override
    public void close() {
        // TODO
    }

    public static class ProducersConfig {
        final String bootstrapServer;
        final int producersPerTopic;
        final Map<String, String> producerConfigs;

        public ProducersConfig(String bootstrapServer, int producersPerTopic, Map<String, String> producerConfigs) {
            this.bootstrapServer = bootstrapServer;
            this.producersPerTopic = producersPerTopic;
            this.producerConfigs = producerConfigs;
        }
    }

    static class Producer {
        private final KafkaProducer<String, byte[]> producer;
        private final String topic;

        public Producer(KafkaProducer<String, byte[]> producer, String topic) {
            this.producer = producer;
            this.topic = topic;
        }
    }
}
