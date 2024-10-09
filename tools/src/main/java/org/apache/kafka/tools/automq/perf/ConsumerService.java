/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_CHARSET;
import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_SEND_TIME_NANOS;

public class ConsumerService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    private final Admin admin;
    private final List<Group> groups = new ArrayList<>();
    private final String groupSuffix;

    public ConsumerService(String bootstrapServer) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(300));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
            count += group.size();
        }
        return count;
    }

    public void start(ConsumerCallback callback) {
        CompletableFuture.allOf(
            groups.stream()
                .map(group -> group.start(callback))
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
        AtomicLong start = new AtomicLong(startMillis);
        CompletableFuture.allOf(
            groups.stream()
                .map(group -> group.seek(start.getAndAdd(intervalMillis)))
                .toArray(CompletableFuture[]::new)
        ).join();
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

        public CompletableFuture<Void> seek(long timestamp) {
            return admin.listOffsets(listOffsetsRequest(timestamp))
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .thenCompose(offsetMap -> CompletableFuture.allOf(consumers.keySet().stream()
                    .map(topic -> admin.alterConsumerGroupOffsets(groupId(topic), resetOffsetsRequest(topic, offsetMap)))
                    .map(AlterConsumerGroupOffsetsResult::all)
                    .map(KafkaFuture::toCompletionStage)
                    .map(CompletionStage::toCompletableFuture)
                    .toArray(CompletableFuture[]::new)));
        }

        public int size() {
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
            properties.putAll(config.consumerConfigs);
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
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
            return String.format("sub-%s-%s-%03d", topic.name, groupSuffix, index);
        }

        private Map<TopicPartition, OffsetSpec> listOffsetsRequest(long timestamp) {
            return consumers.keySet().stream()
                .map(Topic::partitions)
                .flatMap(List::stream)
                .collect(Collectors.toMap(
                    partition -> partition,
                    partition -> OffsetSpec.forTimestamp(timestamp)
                ));
        }

        private Map<TopicPartition, OffsetAndMetadata> resetOffsetsRequest(Topic topic,
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetMap) {
            return offsetMap.entrySet().stream()
                .filter(entry -> topic.containsPartition(entry.getKey()))
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new OffsetAndMetadata(entry.getValue().offset())
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
