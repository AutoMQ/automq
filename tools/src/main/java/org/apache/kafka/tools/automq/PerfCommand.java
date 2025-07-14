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

package org.apache.kafka.tools.automq;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.tools.automq.perf.ConsumerService;
import org.apache.kafka.tools.automq.perf.PerfConfig;
import org.apache.kafka.tools.automq.perf.ProducerService;
import org.apache.kafka.tools.automq.perf.Stats;
import org.apache.kafka.tools.automq.perf.StatsCollector.Result;
import org.apache.kafka.tools.automq.perf.StatsCollector.StopCondition;
import org.apache.kafka.tools.automq.perf.TopicService;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;

import com.automq.stream.s3.metrics.TimerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.apache.kafka.tools.automq.perf.StatsCollector.printAndCollectStats;

public class PerfCommand implements AutoCloseable {

    private static final long TOPIC_READY_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(2);

    private static final ObjectWriter JSON = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final Logger LOGGER = LoggerFactory.getLogger(PerfCommand.class);

    private final PerfConfig config;
    private final TopicService topicService;
    private final ProducerService producerService;
    private final ConsumerService consumerService;
    private final Stats stats = new Stats();
    /**
     * Partitions that are ready to be consumed.
     * Only used during the initial topic readiness check, which is, {@link #preparing} is true.
     */
    private final Set<TopicPartition> readyPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private volatile boolean preparing = true;
    private volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        PerfConfig config = new PerfConfig(args);
        try (PerfCommand command = new PerfCommand(config)) {
            command.run();
        }
    }

    private PerfCommand(PerfConfig config) {
        this.config = config;
        this.topicService = new TopicService(config.bootstrapServer(), config.adminConfig());
        this.producerService = new ProducerService();
        this.consumerService = new ConsumerService(config.bootstrapServer(), config.adminConfig());
    }

    private void run() {
        LOGGER.info("Starting perf test with config: {}", jsonStringify(config));
        TimerUtil timer = new TimerUtil();

        if (config.reset) {
            LOGGER.info("Deleting all test topics...");
            int deleted = topicService.deleteTopics();
            LOGGER.info("Deleted all test topics ({} in total), took {} ms", deleted, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));
        }

        List<Topic> topics;
        if (config.catchupTopicPrefix != null && !config.catchupTopicPrefix.isEmpty()) {
            LOGGER.info("Looking for catch-up topics with prefix: {}", config.catchupTopicPrefix);
            topics = topicService.findExistingTopicsByPrefix(config.catchupTopicPrefix);
            if (topics.isEmpty()) {
                throw new RuntimeException("No catch-up topics found with prefix: " + config.catchupTopicPrefix);
            }
            LOGGER.info("Found {} catch-up topics with prefix '{}'.", topics.size(), config.catchupTopicPrefix);
        } else if (config.reuseTopics) {
            LOGGER.info("Reusing existing topics with prefix: {}", config.topicPrefix);
            topics = topicService.findExistingTopicsByPrefix(config.topicPrefix);
            if (topics.isEmpty()) {
                LOGGER.warn("No existing topics found with prefix '{}', creating new topics instead.", config.topicPrefix);
                topics = topicService.createTopics(config.topicsConfig());
                LOGGER.info("Created {} topics, took {} ms", topics.size(), timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));
            } else {
                LOGGER.info("Found {} existing topics with prefix '{}'.", topics.size(), config.topicPrefix);
            }
        } else {
            LOGGER.info("Creating topics...");
            topics = topicService.createTopics(config.topicsConfig());
            LOGGER.info("Created {} topics, took {} ms", topics.size(), timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));
        }

        LOGGER.info("Creating consumers...");
        int consumers = consumerService.createConsumers(topics, config.consumersConfig());
        consumerService.start(this::messageReceived, config.maxConsumeRecordRate);
        LOGGER.info("Created {} consumers, took {} ms", consumers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Creating producers...");
        int producers = producerService.createProducers(topics, config.producersConfig(), this::messageSent);
        LOGGER.info("Created {} producers, took {} ms", producers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        if (config.catchupTopicPrefix != null && !config.catchupTopicPrefix.isEmpty()) {
            LOGGER.info("Using catch-up topics, skipping message accumulation phase");
            preparing = false; // Directly start consuming without accumulation
        } else if (config.awaitTopicReady) {
            LOGGER.info("Waiting for topics to be ready...");
            waitTopicsReady(consumerService.consumerCount() > 0);
            LOGGER.info("Topics are ready, took {} ms", timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

            Function<String, List<byte[]>> payloads = payloads(config, topics);
            producerService.start(payloads, config.sendRate);
            preparing = false;

            if (config.warmupDurationMinutes > 0) {
                LOGGER.info("Warming up for {} minutes...", config.warmupDurationMinutes);
                long warmupStart = System.nanoTime();
                long warmupMiddle = warmupStart + TimeUnit.MINUTES.toNanos(config.warmupDurationMinutes) / 2;
                producerService.adjustRate(warmupStart, ProducerService.MIN_RATE);
                producerService.adjustRate(warmupMiddle, config.sendRate);
                collectStats(Duration.ofMinutes(config.warmupDurationMinutes));
            }
        } else {
            // If not waiting for topic ready and not using catchup topics, start producing immediately.
            Function<String, List<byte[]>> payloads = payloads(config, topics);
            producerService.start(payloads, config.sendRate);
            preparing = false;
        }

        Result result;
        if (config.backlogDurationSeconds > 0) {
            LOGGER.info("Pausing consumers for {} seconds to build up backlog...", config.backlogDurationSeconds);
            consumerService.pause();
            long backlogStart = System.currentTimeMillis();
            collectStats(Duration.ofSeconds(config.backlogDurationSeconds));
            long backlogEnd = System.nanoTime();

            LOGGER.info("Resetting consumer offsets and resuming...");
            consumerService.resetOffset(backlogStart, TimeUnit.SECONDS.toMillis(config.groupStartDelaySeconds));

            // Select topics for catch-up
            int numTopics = topics.size();
            int topicsToResume = (int) Math.ceil(numTopics * (config.consumersDuringCatchupPercentage / 100.0));
            topicsToResume = Math.max(1, Math.min(numTopics, topicsToResume));
            List<String> allTopicNames = topics.stream().map(t -> t.name()).collect(java.util.stream.Collectors.toList());
            java.util.Collections.shuffle(allTopicNames);
            List<String> resumeTopics = allTopicNames.subList(0, topicsToResume);

            // No need to pause all again, they're already paused from line 172
            // Just resume the selected topics
            consumerService.resumeTopics(resumeTopics);
            LOGGER.info("Resuming consumers for topics: {} ({} out of {})", resumeTopics, topicsToResume, numTopics);
            // No need to pause specific topics, they're already paused
            LOGGER.info("Keeping remaining consumers paused ({} topics)", numTopics - topicsToResume);

            stats.reset();
            producerService.adjustRate(config.sendRateDuringCatchup);
            result = collectStats(backlogEnd);
        } else {
            LOGGER.info("Running test for {} minutes...", config.testDurationMinutes);
            stats.reset();
            result = collectStats(Duration.ofMinutes(config.testDurationMinutes));
        }
        LOGGER.info("Saving results to {}", saveResult(result));

        running = false;
    }

    private String jsonStringify(Object obj) {
        try {
            return JSON.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void messageSent(int size, long sendTimeNanos, Exception exception) {
        if (exception != null) {
            stats.messageFailed();
        } else {
            stats.messageSent(size, sendTimeNanos);
        }
    }

    private void messageReceived(TopicPartition topicPartition, byte[] payload, long sendTimeNanos) {
        if (preparing && config.awaitTopicReady && (config.catchupTopicPrefix == null || config.catchupTopicPrefix.isEmpty())) {
            readyPartitions.add(topicPartition);
        }
        stats.messageReceived(payload.length, sendTimeNanos);
    }

    private void waitTopicsReady(boolean hasConsumer) {
        if (config.catchupTopicPrefix != null && !config.catchupTopicPrefix.isEmpty()) {
            LOGGER.info("Using catch-up topics, skipping topic readiness check.");
            return;
        }
        if (hasConsumer) {
            waitTopicsReadyWithConsumer();
        } else {
            waitTopicsReadyWithoutConsumer();
        }
        stats.reset();
    }

    private void waitTopicsReadyWithConsumer() {
        long start = System.nanoTime();
        boolean ready = false;
        int expectPartitionCount = config.topics * config.partitionsPerTopic;
        while (System.nanoTime() < start + TOPIC_READY_TIMEOUT_NANOS) {
            producerService.probe();
            int received = readyPartitions.size();
            LOGGER.info("Waiting for topics to be ready... sent: {}, received: {}", expectPartitionCount, received);
            if (received >= expectPartitionCount) {
                ready = true;
                break;
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (!ready) {
            throw new RuntimeException("Timeout waiting for topics to be ready");
        }
    }

    private void waitTopicsReadyWithoutConsumer() {
        producerService.probe();
        try {
            // If there is no consumer, we can only wait for a fixed time to ensure the topic is ready.
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Function<String, List<byte[]>> payloads(PerfConfig config, List<Topic> topics) {
        if (Strings.isNullOrEmpty(config.valueSchema)) {
            List<byte[]> payloads = randomPayloads(config.recordSize, config.randomRatio, config.randomPoolSize);
            return topic -> payloads;
        } else {
            // The producer configs should contain:
            // - schema.registry.url: http://localhost:8081
            Map<String, List<byte[]>> topic2payloads = new HashMap<>();
            topics.forEach(topic -> {
                topic2payloads.put(topic.name(), schemaPayloads(topic.name(), config.valueSchema, config.valuesFile, config.producerConfigs));
            });
            return topic2payloads::get;
        }
    }

    /**
     * Generates a list of byte arrays with specified size and mix of random and static content.
     *
     * @param size        The size of each byte array.
     * @param randomRatio The fraction of each array that should be random (0.0 to 1.0).
     * @param count       The number of arrays to generate.
     * @return List of byte arrays, each containing a mix of random and static bytes.
     */
    private List<byte[]> randomPayloads(int size, double randomRatio, int count) {
        Random r = ThreadLocalRandom.current();

        int randomBytes = (int) (size * randomRatio);
        int staticBytes = size - randomBytes;
        byte[] staticPayload = new byte[staticBytes];
        r.nextBytes(staticPayload);

        if (randomBytes == 0) {
            // all payloads are the same, no need to create multiple copies
            return List.of(staticPayload);
        }

        List<byte[]> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] payload = new byte[size];
            r.nextBytes(payload);
            System.arraycopy(staticPayload, 0, payload, randomBytes, staticBytes);
            payloads.add(payload);
        }
        return payloads;
    }

    private Result collectStats(Duration duration) {
        StopCondition condition = (startNanos, nowNanos) -> Duration.ofNanos(nowNanos - startNanos).compareTo(duration) >= 0;
        return collectStats(condition);
    }

    private Result collectStats(long consumerTimeNanos) {
        StopCondition condition = (startNanos, nowNanos) -> stats.maxSendTimeNanos.get() >= consumerTimeNanos;
        return collectStats(condition);
    }

    private Result collectStats(StopCondition condition) {
        long intervalNanos = TimeUnit.SECONDS.toNanos(config.reportingIntervalSeconds);
        return printAndCollectStats(stats, condition, intervalNanos, config);
    }

    private String saveResult(Result result) {
        String fileName = String.format("perf-%s.json", new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()));
        try {
            JSON.writeValue(new File(fileName), result);
        } catch (Exception e) {
            LOGGER.error("Failed to write result to file {}", fileName, e);
        }
        return fileName;
    }

    @Override
    public void close() {
        topicService.close();
        producerService.close();
        consumerService.close();
    }

    private static List<byte[]> schemaPayloads(String topic, String schemaJson, String payloadsFile, Map<String, ?> configs) {
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            List<byte[]> payloads = new ArrayList<>();
            AvroSchema schema = new AvroSchema(schemaJson);
            serializer.configure(configs, false);
            for (String payloadStr : Files.readAllLines(Path.of(payloadsFile), StandardCharsets.UTF_8)) {
                Object object = AvroSchemaUtils.toObject(payloadStr, schema);
                byte[] payload = serializer.serialize(topic, object);
                payloads.add(payload);
            }
            return payloads;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
