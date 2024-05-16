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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.tools.automq.perf.UniformRateLimiter.uninterruptibleSleepNs;

public class ProducerService implements AutoCloseable {

    public static final Charset HEADER_KEY_CHARSET = StandardCharsets.UTF_8;
    public static final String HEADER_KEY_SEND_TIME_NANOS = "send_time_nanos";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    private final List<Producer> producers = new LinkedList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(ThreadUtils.createThreadFactory("perf-producer", false));

    private UniformRateLimiter rateLimiter = new UniformRateLimiter(1.0);
    private volatile boolean closed = false;

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

    /**
     * Start sending messages using the given payloads at the given rate.
     */
    public void start(List<byte[]> payloads, double rate) {
        adjustRate(rate);
        int processors = Runtime.getRuntime().availableProcessors();
        // shard producers across processors
        int batchSize = Math.max(1, producers.size() / processors);
        for (int i = 0; i < producers.size(); i += batchSize) {
            int from = i;
            int to = Math.min(i + batchSize, producers.size());
            executor.submit(() -> start(producers.subList(from, to), payloads));
        }
    }

    public void adjustRate(double rate) {
        this.rateLimiter = new UniformRateLimiter(rate);
    }

    private void start(List<Producer> producers, List<byte[]> payloads) {
        executor.submit(() -> {
            try {
                sendMessages(producers, payloads);
            } catch (Exception e) {
                LOGGER.error("Failed to send messages", e);
            }
        });
    }

    private void sendMessages(List<Producer> producers, List<byte[]> payloads) {
        Random random = ThreadLocalRandom.current();
        while (!closed) {
            producers.forEach(
                p -> sendMessage(p, payloads.get(random.nextInt(payloads.size())))
            );
        }
    }

    private void sendMessage(Producer producer, byte[] payload) {
        long intendedSendTime = rateLimiter.acquire();
        uninterruptibleSleepNs(intendedSendTime);
        producer.sendAsync(payload).exceptionally(e -> {
            LOGGER.warn("Failed to send message", e);
            return null;
        });
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        producers.forEach(Producer::close);
    }

    @FunctionalInterface
    public interface ProducerCallback {
        /**
         * Called when a message is sent.
         *
         * @param size          the size of the sent message
         * @param sendTimeNanos the time in nanoseconds when the message was sent
         * @param exception     the exception if the message failed to send, or null
         */
        void messageSent(int size, long sendTimeNanos, Exception exception);
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
            Random random = ThreadLocalRandom.current();
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
                new RecordHeader(HEADER_KEY_SEND_TIME_NANOS, Long.toString(sendTimeNanos).getBytes(HEADER_KEY_CHARSET))
            );
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic.name, partition, key, payload, headers);
            int size = payload.length;
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                producer.send(record, (metadata, exception) -> {
                    callback.messageSent(size, sendTimeNanos, exception);
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                });
            } catch (InterruptException e) {
                // ignore, as we are closing
                future.completeExceptionally(e);
            } catch (Exception e) {
                callback.messageSent(size, sendTimeNanos, e);
                future.completeExceptionally(e);
            }
            return future;
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}
