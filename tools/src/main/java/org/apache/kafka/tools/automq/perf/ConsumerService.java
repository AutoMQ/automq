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

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);
    private final List<Consumer> consumers = new LinkedList<>();

    /**
     * Create consumers for the given topics. NOT thread-safe.
     * Note: the created consumers will start polling immediately.
     *
     * @param topics   topic names
     * @param config   consumer configuration
     * @param callback callback to be called when a message is received
     * @return the number of consumers created
     */
    public int createConsumers(List<String> topics, ConsumersConfig config, ConsumerCallback callback) {
        int count = 0;
        for (String topic : topics) {
            for (int g = 0; g < config.groupsPerTopic; g++) {
                String groupId = String.format("sub-%s-%03d", topic, g);
                for (int c = 0; c < config.consumersPerGroup; c++) {
                    Consumer consumer = createConsumer(topic, groupId, config, callback);
                    consumers.add(consumer);
                    count++;
                }
            }
        }
        return count;
    }

    private static Consumer createConsumer(String topic, String groupId,
        ConsumersConfig config, ConsumerCallback callback) {
        Properties properties = new Properties();
        properties.putAll(config.consumerConfigs);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topic));
        // start polling
        return new Consumer(kafkaConsumer, callback);
    }

    @Override
    public void close() {
        consumers.forEach(Consumer::preClose);
        consumers.forEach(Consumer::close);
    }

    @FunctionalInterface
    public interface ConsumerCallback {
        /**
         * Called when a message is received.
         *
         * @param payload       the received message payload
         * @param publishTimeMs the time in milliseconds when the message was published
         */
        void messageReceived(byte[] payload, long publishTimeMs);
    }

    public static class ConsumersConfig {
        final String bootstrapServer;
        final int groupsPerTopic;
        final int consumersPerGroup;
        final Map<String, String> consumerConfigs;

        public ConsumersConfig(String bootstrapServer, int groupsPerTopic, int consumersPerGroup,
            Map<String, String> consumerConfigs) {
            this.bootstrapServer = bootstrapServer;
            this.groupsPerTopic = groupsPerTopic;
            this.consumersPerGroup = consumersPerGroup;
            this.consumerConfigs = consumerConfigs;
        }
    }

    static class Consumer {
        private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
        private final KafkaConsumer<String, byte[]> consumer;
        private final ExecutorService executor;
        private final Future<?> task;
        private volatile boolean closing = false;

        public Consumer(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
            this.consumer = consumer;
            this.executor = Executors.newSingleThreadExecutor();
            this.task = this.executor.submit(() -> pollRecords(consumer, callback));
        }

        private void pollRecords(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
            while (!closing) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        callback.messageReceived(record.value(), record.timestamp());
                    }
                } catch (Exception e) {
                    LOGGER.error("exception occur while consuming message", e);
                }
            }
        }

        /**
         * Signal the consumer to close.
         */
        public void preClose() {
            closing = true;
            executor.shutdown();
        }

        /**
         * Wait for the consumer to finish processing and then close it.
         */
        public void close() {
            assert closing;
            try {
                task.get();
            } catch (Exception e) {
                LOGGER.error("exception occur while closing consumer", e);
            }
            consumer.close();
        }
    }
}
