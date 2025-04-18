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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TopicService implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicService.class);
    /**
     * The maximum number of partitions per batch.
     *
     * @see org.apache.kafka.controller.ReplicationControlManager
     */
    private static final int MAX_PARTITIONS_PER_BATCH = 10_000;
    /**
     * The common prefix for performance test topics.
     */
    private static final String COMMON_TOPIC_PREFIX = "__automq_perf_";

    private final Admin admin;

    public TopicService(String bootstrapServer, Properties properties) {
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.admin = Admin.create(properties);
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

        int topicsPerBatch = MAX_PARTITIONS_PER_BATCH / config.partitionsPerTopic;
        List<List<NewTopic>> requests = Lists.partition(newTopics, topicsPerBatch);

        Map<String, KafkaFuture<Void>> results = requests.stream()
            .map(admin::createTopics)
            .map(CreateTopicsResult::values)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        results.forEach(this::waitTopicCreated);

        return results.keySet().stream()
            .map(name -> new Topic(name, config.partitionsPerTopic))
            .collect(Collectors.toList());
    }

    /**
     * Delete all historical performance test topics.
     */
    public int deleteTopics() {
        ListTopicsResult result = admin.listTopics();
        try {
            List<String> toDelete = result.names().get().stream()
                .filter(name -> name.startsWith(COMMON_TOPIC_PREFIX))
                .collect(Collectors.toList());
            admin.deleteTopics(toDelete).all().get();
            return toDelete.size();
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
        return String.format("%s%s_%04d_%07d", COMMON_TOPIC_PREFIX, topicPrefix, partitions, index);
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

        public String name() {
            return name;
        }

        public List<TopicPartition> partitions() {
            return IntStream.range(0, partitions)
                .mapToObj(i -> new TopicPartition(name, i))
                .collect(Collectors.toList());
        }

        public TopicPartition firstPartition() {
            return new TopicPartition(name, 0);
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
