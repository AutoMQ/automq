/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.tools.automq.perf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicService.class);
    private final Admin admin;

    public TopicService(String bootstrapServer, Map<String, String> adminConfigs) {
        Map<String, Object> configs = new HashMap<>(adminConfigs);
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.admin = Admin.create(configs);
    }

    /**
     * Create topics with the given configuration.
     * Note: If the topic already exists, it will not be created again.
     */
    public List<Topic> createTopics(TopicsConfig config) {
        List<NewTopic> newTopics = IntStream.range(0, config.topics)
            .mapToObj(i -> generateTopicName(config.topicPrefix, config.partitionsPerTopic, i))
            .map(name -> new NewTopic(name, config.partitionsPerTopic, (short) 1).configs(config.topicConfigs))
            .collect(Collectors.toList());
        CreateTopicsResult topics = admin.createTopics(newTopics);
        topics.values().forEach(this::waitTopicCreated);
        return topics.values().keySet().stream()
            .map(name -> new Topic(name, config.partitionsPerTopic))
            .collect(Collectors.toList());
    }

    /**
     * Delete all topics except internal topics (starting with '__').
     */
    public int deleteTopics() {
        ListTopicsResult result = admin.listTopics();
        try {
            Set<String> topics = result.names().get();
            topics.removeIf(name -> name.startsWith("__"));
            admin.deleteTopics(topics).all().get();
            return topics.size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ignored) {
        }
        return 0;
    }

    /**
     * Wait until the topic is created, or throw an exception if it fails.
     * Note: It is okay if the topic already exists.
     */
    private void waitTopicCreated(String name, KafkaFuture<Void> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOGGER.debug("Topic already exists. name={}", name);
            } else {
                LOGGER.error("Failed to create topic. name={}", name, e);
                throw new RuntimeException(e);
            }
        }
    }

    private String generateTopicName(String topicPrefix, int partitions, int index) {
        return String.format("%s-%04d-%07d", topicPrefix, partitions, index);
    }

    public static class TopicsConfig {
        final String topicPrefix;
        final int topics;
        final int partitionsPerTopic;
        final Map<String, String> topicConfigs;

        public TopicsConfig(String topicPrefix, int topics, int partitionsPerTopic, Map<String, String> topicConfigs) {
            this.topicPrefix = topicPrefix;
            this.topics = topics;
            this.partitionsPerTopic = partitionsPerTopic;
            this.topicConfigs = topicConfigs;
        }
    }

    public static class Topic {
        final String name;
        final int partitions;

        public Topic(String name, int partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public List<TopicPartition> partitions() {
            return IntStream.range(0, partitions)
                .mapToObj(i -> new TopicPartition(name, i))
                .collect(Collectors.toList());
        }

        public boolean containsPartition(TopicPartition partition) {
            return name.equals(partition.topic()) && partition.partition() < partitions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Topic topic = (Topic) o;
            return partitions == topic.partitions && Objects.equals(name, topic.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, partitions);
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
