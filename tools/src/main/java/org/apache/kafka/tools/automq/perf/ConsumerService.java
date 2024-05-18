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
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_CHARSET;
import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_SEND_TIME_NANOS;

public class ConsumerService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private final List<Consumer> consumers = new LinkedList<>();

    /**
     * Create consumers for the given topics.
     * Note: the created consumers will start polling immediately.
     * NOT thread-safe.
     *
     * @param topics   topic names
     * @param config   consumer configuration
     * @param callback callback to be called when a message is received
     * @return the number of consumers created
     */
    public int createConsumers(List<Topic> topics, ConsumersConfig config, ConsumerCallback callback) {
        int count = 0;
        for (Topic topic : topics) {
            for (int g = 0; g < config.groupsPerTopic; g++) {
                String groupId = String.format("sub-%s-%03d", topic.name, g);
                for (int c = 0; c < config.consumersPerGroup; c++) {
                    Consumer consumer = createConsumer(topic, groupId, config, callback);
                    consumers.add(consumer);
                    count++;
                }
            }
        }
        return count;
    }

    private static Consumer createConsumer(Topic topic, String groupId,
        ConsumersConfig config, ConsumerCallback callback) {
        Properties properties = new Properties();
        properties.putAll(config.consumerConfigs);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topic.name));
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
         * @param sendTimeNanos the time in nanoseconds when the message was sent
         */
        void messageReceived(byte[] payload, long sendTimeNanos);
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

    private static class Consumer {
        private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
        private final KafkaConsumer<String, byte[]> consumer;
        private final ExecutorService executor;
        private final Future<?> task;
        private volatile boolean closing = false;

        public Consumer(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
            this.consumer = consumer;
            this.executor = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("perf-consumer", false));
            this.task = this.executor.submit(() -> pollRecords(consumer, callback));
        }

        private void pollRecords(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
            while (!closing) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        long sendTimeNanos = Long.parseLong(new String(record.headers().lastHeader(HEADER_KEY_SEND_TIME_NANOS).value(), HEADER_KEY_CHARSET));
                        callback.messageReceived(record.value(), sendTimeNanos);
                    }
                } catch (InterruptException e) {
                    // ignore, as we are closing
                } catch (Exception e) {
                    LOGGER.warn("exception occur while consuming message", e);
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
