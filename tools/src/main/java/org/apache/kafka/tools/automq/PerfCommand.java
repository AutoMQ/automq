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

package org.apache.kafka.tools.automq;

import com.automq.stream.s3.metrics.TimerUtil;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.tools.automq.perf.ConsumerService;
import org.apache.kafka.tools.automq.perf.PerfConfig;
import org.apache.kafka.tools.automq.perf.ProducerService;
import org.apache.kafka.tools.automq.perf.Stats;
import org.apache.kafka.tools.automq.perf.TopicService;
import org.apache.kafka.tools.automq.perf.TopicService.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfCommand implements AutoCloseable {

    private static final long TOPIC_READY_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(2);

    private static final Logger LOGGER = LoggerFactory.getLogger(PerfCommand.class);

    private final PerfConfig config;
    private final TopicService topicService;
    private final ProducerService producerService = new ProducerService();
    private final ConsumerService consumerService = new ConsumerService();
    private final Stats stats = new Stats();

    public static void main(String[] args) throws Exception {
        PerfConfig config = new PerfConfig(args);
        try (PerfCommand command = new PerfCommand(config)) {
            command.run();
        }
    }

    private PerfCommand(PerfConfig config) {
        this.config = config;
        this.topicService = new TopicService(config.bootstrapServer());
    }

    private void run() {
        TimerUtil timer = new TimerUtil();

        LOGGER.info("Creating topics...");
        List<Topic> topics = topicService.createTopics(config.topicsConfig());
        LOGGER.info("Created {} topics, took {} ms", topics.size(), timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Creating consumers...");
        int consumers = consumerService.createConsumers(topics, config.consumersConfig(), this::messageReceived);
        LOGGER.info("Created {} consumers, took {} ms", consumers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Creating producers...");
        int producers = producerService.createProducers(topics, config.producersConfig(), this::messageSent);
        LOGGER.info("Created {} producers, took {} ms", producers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Waiting for topics are ready...");
        waitTopicsReady();
        LOGGER.info("Topics are ready, took {} ms", timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        // TODO generate payload
        List<byte[]> payloads = null;
        producerService.start(payloads, config.sendRate);

        // TODO
    }

    private void messageSent(int size, long sendTimeNanos, Exception exception) {
        if (exception != null) {
            stats.messageFailed();
        } else {
            stats.messageSent(size, sendTimeNanos);
        }
    }

    private void messageReceived(byte[] payload, long sendTimeNanos) {
        stats.messageReceived(payload.length, sendTimeNanos);
    }

    private void waitTopicsReady() {
        int sent = producerService.probe();
        long start = System.nanoTime();
        boolean ready = false;
        while (System.nanoTime() < start + TOPIC_READY_TIMEOUT_NANOS) {
            long received = stats.toCumulativeStats().totalMessagesReceived;
            if (received >= sent) {
                ready = true;
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        if (!ready) {
            throw new RuntimeException("Timeout waiting for topics to be ready");
        }
    }

    @Override
    public void close() {
        topicService.close();
        producerService.close();
        consumerService.close();
    }
}
