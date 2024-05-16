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

import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService implements AutoCloseable {

    public static final String HEADER_KEY_SEND_TIME_NANOS = "send_time_nanos";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    private final List<Producer> producers = new LinkedList<>();

    /**
     * Create producers for the given topics.
     * NOT thread-safe.
     *
     * @param topics   topic names
     * @param config   producer configuration
     * @param callback callback to be called when a message is sent
     * @return number of producers created
     */
    public int createProducers(List<Topic> topics, ProducersConfig config, ProducerCallback callback) {
        int count = 0;
        for (Topic topic : topics) {
            for (int i = 0; i < config.producersPerTopic; i++) {
                Producer producer = createProducer(topic, config, callback);
                producers.add(producer);
                count++;
            }
        }
        return count;
    }

    private Producer createProducer(Topic topic, ProducersConfig config, ProducerCallback callback) {
        Properties properties = new Properties();
        properties.putAll(config.producerConfigs);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);

        KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(properties);
        return new Producer(kafkaProducer, topic, callback);
    }

    /**
     * Send a message to each partition of each topic to ensure that the topic is created.
     * It throws an exception if any of the producers fail to send the message.
     * NOT thread-safe.
     *
     * @return number of messages sent
     */
    public int probe() {
        List<CompletableFuture<Void>> futures = producers.stream()
            .map(Producer::probe)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return futures.size();
    }

    @Override
    public void close() {
        producers.forEach(Producer::close);
    }

    @FunctionalInterface
    public interface ProducerCallback {
        /**
         * Called when a message is sent.
         *
         * @param payload       the sent message payload
         * @param sendTimeNanos the time in nanoseconds when the message was sent
         * @param exception     the exception if the message failed to send, or null
         */
        void messageSent(byte[] payload, long sendTimeNanos, Exception exception);
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

    private static class Producer implements AutoCloseable {

        private static final String[] PRESET_KEYS = new String[16384];

        static {
            byte[] buffer = new byte[7];
            Random random = new Random();
            for (int i = 0; i < PRESET_KEYS.length; i++) {
                random.nextBytes(buffer);
                PRESET_KEYS[i] = Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
            }
        }

        private final Random random = ThreadLocalRandom.current();
        private final KafkaProducer<String, byte[]> producer;
        private final Topic topic;
        private final ProducerCallback callback;

        private int partitionIndex = 0;

        public Producer(KafkaProducer<String, byte[]> producer, Topic topic, ProducerCallback callback) {
            this.producer = producer;
            this.topic = topic;
            this.callback = callback;
        }

        /**
         * Send the payload to a random partition with a random key.
         * NOT thread-safe.
         */
        public CompletableFuture<Void> sendAsync(byte[] payload) {
            return sendAsync(nextKey(), payload, nextPartition());
        }

        private int nextPartition() {
            return partitionIndex++ % topic.partitions;
        }

        private String nextKey() {
            return PRESET_KEYS[random.nextInt(PRESET_KEYS.length)];
        }

        /**
         * Send a message to each partition.
         */
        public List<CompletableFuture<Void>> probe() {
            return IntStream.range(0, topic.partitions)
                .mapToObj(i -> sendAsync("probe", new byte[42], i))
                .collect(Collectors.toList());
        }

        /**
         * Send a message to the topic.
         *
         * @param key       the key of the message, optional
         * @param payload   the payload of the message
         * @param partition the partition to send the message to, optional
         * @return a future that completes when the message is sent
         */
        private CompletableFuture<Void> sendAsync(String key, byte[] payload, Integer partition) {
            long sendTimeNanos = System.nanoTime();
            List<Header> headers = List.of(
                new RecordHeader(HEADER_KEY_SEND_TIME_NANOS, Long.toString(sendTimeNanos).getBytes())
            );
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic.name, partition, key, payload, headers);
            CompletableFuture<Void> future = new CompletableFuture<>();
            producer.send(record, (metadata, exception) -> {
                callback.messageSent(payload, sendTimeNanos, exception);
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(null);
                }
            });
            return future;
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}
