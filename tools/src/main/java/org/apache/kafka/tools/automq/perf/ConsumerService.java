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

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;

import com.google.common.primitives.Longs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.github.bucket4j.BlockingBucket;
import io.github.bucket4j.Bucket;

import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_SEND_TIME_NANOS;

public class ConsumerService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private final Admin admin;
    private final List<Group> groups = new ArrayList<>();
    private final String groupSuffix;

    public ConsumerService(String bootstrapServer, Properties properties) {
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.MINUTES.toMillis(2));
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) TimeUnit.MINUTES.toMillis(10));
        this.admin = Admin.create(properties);
        this.groupSuffix = new SimpleDateFormat("HHmmss").format(System.currentTimeMillis());
    }

    /**
     * Create consumers for the given topics.
     * Note: the created consumers will start polling immediately.
     * NOT thread-safe.
     *
     * @param topics topic names
     * @param config consumer configuration
     * @return the number of consumers created
     */
    public int createConsumers(List<Topic> topics, ConsumersConfig config) {
        int count = 0;
        for (int g = 0; g < config.groupsPerTopic; g++) {
            Group group = new Group(g, config.consumersPerGroup, topics, config);
            groups.add(group);
            count += group.consumerCount();
        }
        return count;
    }

    public void start(ConsumerCallback callback, int pollRate) {
        BlockingBucket bucket = rateLimitBucket(pollRate);
        ConsumerCallback callbackWithRateLimit = (tp, p, st) -> {
            callback.messageReceived(tp, p, st);
            bucket.consume(1);
        };
        CompletableFuture.allOf(
            groups.stream()
                .map(group -> group.start(callbackWithRateLimit))
                .toArray(CompletableFuture[]::new)
        ).join();
    }

    public void pause() {
        groups.forEach(Group::pause);
    }

    public void resume() {
        groups.forEach(Group::resume);
    }

    public void resetOffset(long startMillis, long intervalMillis) {
        AtomicLong timestamp = new AtomicLong(startMillis);
        for (int i = 0, size = groups.size(); i < size; i++) {
            Group group = groups.get(i);
            group.seek(timestamp.getAndAdd(intervalMillis));
            LOGGER.info("Reset consumer group offsets: {}/{}", i + 1, size);
        }
    }

    public int consumerCount() {
        return groups.stream()
            .mapToInt(Group::consumerCount)
            .sum();
    }

    private BlockingBucket rateLimitBucket(int rateLimit) {
        return Bucket.builder()
            .addLimit(limit -> limit
                .capacity(rateLimit / 10)
                .refillGreedy(rateLimit, Duration.ofSeconds(1))
            ).build()
            .asBlocking();
    }

    @Override
    public void close() {
        admin.close();
        groups.forEach(Group::close);
    }

    @FunctionalInterface
    public interface ConsumerCallback {
        /**
         * Called when a message is received.
         *
         * @param topicPartition the topic partition of the received message
         * @param payload        the received message payload
         * @param sendTimeNanos  the time in nanoseconds when the message was sent
         */
        void messageReceived(TopicPartition topicPartition, byte[] payload, long sendTimeNanos) throws InterruptedException;
    }

    public static class ConsumersConfig {
        final String bootstrapServer;
        final int groupsPerTopic;
        final int consumersPerGroup;
        final Properties properties;

        public ConsumersConfig(String bootstrapServer, int groupsPerTopic, int consumersPerGroup,
            Properties properties) {
            this.bootstrapServer = bootstrapServer;
            this.groupsPerTopic = groupsPerTopic;
            this.consumersPerGroup = consumersPerGroup;
            this.properties = properties;
        }
    }

    private class Group implements AutoCloseable {
        private final int index;
        private final Map<Topic, List<Consumer>> consumers = new HashMap<>();

        public Group(int index, int consumersPerGroup, List<Topic> topics, ConsumersConfig config) {
            this.index = index;

            Properties common = toProperties(config);
            for (Topic topic : topics) {
                List<Consumer> topicConsumers = new ArrayList<>();
                for (int c = 0; c < consumersPerGroup; c++) {
                    Consumer consumer = newConsumer(topic, common);
                    topicConsumers.add(consumer);
                }
                consumers.put(topic, topicConsumers);
            }
        }

        public CompletableFuture<Void> start(ConsumerCallback callback) {
            consumers().forEach(consumer -> consumer.start(callback));

            // wait for all consumers to join the group
            return CompletableFuture.allOf(consumers()
                .map(Consumer::started)
                .toArray(CompletableFuture[]::new));
        }

        public void pause() {
            consumers().forEach(Consumer::pause);
        }

        public void resume() {
            consumers().forEach(Consumer::resume);
        }

        public void seek(long timestamp) {
            // assuming all partitions approximately have the same offset at the given timestamp
            TopicPartition firstPartition = consumers.keySet().iterator().next().firstPartition();
            try {
                ListOffsetsResult.ListOffsetsResultInfo offsetInfo = admin.listOffsets(Map.of(firstPartition, OffsetSpec.forTimestamp(timestamp)))
                    .partitionResult(firstPartition)
                    .get();
                KafkaFuture.allOf(consumers.keySet().stream()
                    .map(topic -> admin.alterConsumerGroupOffsets(groupId(topic), resetOffsetsRequest(topic, offsetInfo.offset())))
                    .map(AlterConsumerGroupOffsetsResult::all)
                    .toArray(KafkaFuture[]::new)).get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException("Failed to list or reset consumer offsets", e);
            }
        }

        public int consumerGroupCount() {
            return consumers.size();
        }

        public int consumerCount() {
            return consumers.values().stream()
                .mapToInt(List::size)
                .sum();
        }

        @Override
        public void close() {
            consumers().forEach(Consumer::preClose);
            consumers().forEach(Consumer::close);
        }

        private Properties toProperties(ConsumersConfig config) {
            Properties properties = new Properties();
            properties.putAll(config.properties);
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString());
            return properties;
        }

        private Consumer newConsumer(Topic topic, Properties common) {
            Properties properties = new Properties();
            properties.putAll(common);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId(topic));
            return new Consumer(properties, topic.name);
        }

        private Stream<Consumer> consumers() {
            return consumers.values().stream().flatMap(List::stream);
        }

        private String groupId(Topic topic) {
            return String.format("sub-%s-%03d", topic.name, index);
        }

        private Map<TopicPartition, OffsetAndMetadata> resetOffsetsRequest(Topic topic, long offset) {
            return topic.partitions().stream()
                .collect(Collectors.toMap(
                    partition -> partition,
                    ignore -> new OffsetAndMetadata(offset)
                ));
        }
    }

    private static class Consumer {
        private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
        private static final long PAUSE_INTERVAL = 1000;
        private final KafkaConsumer<String, byte[]> consumer;
        private final ExecutorService executor;
        private Future<?> task;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private boolean paused = false;
        private volatile boolean closing = false;

        public Consumer(Properties properties, String topic) {
            this.consumer = new KafkaConsumer<>(properties);
            this.executor = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("perf-consumer", false));

            consumer.subscribe(List.of(topic), subscribeListener());
        }

        public void start(ConsumerCallback callback) {
            this.task = this.executor.submit(() -> pollRecords(consumer, callback));
        }

        public CompletableFuture<Void> started() {
            return started;
        }

        public void pause() {
            paused = true;
        }

        public void resume() {
            paused = false;
        }

        private ConsumerRebalanceListener subscribeListener() {
            return new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // do nothing
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // once partitions are assigned, it means the consumer has joined the group
                    started.complete(null);
                }
            };
        }

        private void pollRecords(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback) {
            while (!closing) {
                try {
                    while (paused) {
                        Thread.sleep(PAUSE_INTERVAL);
                    }
                    ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        long sendTimeNanos = Longs.fromByteArray(record.headers().lastHeader(HEADER_KEY_SEND_TIME_NANOS).value());
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        callback.messageReceived(topicPartition, record.value(), sendTimeNanos);
                    }
                } catch (InterruptException | InterruptedException e) {
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
            if (task != null) {
                try {
                    task.get();
                } catch (Exception e) {
                    LOGGER.error("exception occur while closing consumer", e);
                }
            }
            consumer.close();
        }
    }
}
