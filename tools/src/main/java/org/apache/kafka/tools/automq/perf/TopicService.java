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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicService.class);
    private final Admin client;

    public TopicService(String bootstrapServer) {
        this.client = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    }

    /**
     * Create topics with the given configuration.
     * Note: If the topic already exists, it will not be created again.
     */
    public List<String> createTopics(TopicsConfig config) {
        List<NewTopic> newTopics = IntStream.range(0, config.topics)
            .mapToObj(i -> generateTopicName(config.topicPrefix, config.partitionsPerTopic, i))
            .map(name -> new NewTopic(name, config.partitionsPerTopic, (short) 1).configs(config.topicConfigs))
            .collect(Collectors.toList());
        CreateTopicsResult topics = client.createTopics(newTopics);
        topics.values().forEach(this::waitTopicCreated);
        return new ArrayList<>(topics.values().keySet());
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
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOGGER.info("Topic already exists. name={}", name);
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

    @Override
    public void close() {
        client.close();
    }
}
