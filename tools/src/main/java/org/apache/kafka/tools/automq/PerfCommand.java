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
import org.apache.kafka.tools.automq.perf.TopicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfCommand implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerfCommand.class);
    private final PerfConfig config;
    private final TopicService topicService;
    private final ProducerService producerService;
    private final ConsumerService consumerService;

    public static void main(String[] args) throws Exception {
        PerfConfig config = new PerfConfig(args);
        try (PerfCommand command = new PerfCommand(config)) {
            command.run();
        }
    }

    private PerfCommand(PerfConfig config) {
        this.config = config;
        this.topicService = new TopicService(config.bootstrapServer());
        this.producerService = new ProducerService();
        this.consumerService = new ConsumerService();
    }

    private void run() {
        TimerUtil timer = new TimerUtil();

        LOGGER.info("Creating topics...");
        List<String> topics = topicService.createTopics(config.topicsConfig());
        LOGGER.info("Created {} topics, took {} ms", topics.size(), timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Creating consumers...");
        int consumers = consumerService.createConsumers(topics, config.consumersConfig(), (record, timestamp) -> {
            // TODO
        });
        LOGGER.info("Created {} consumers, took {} ms", consumers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));

        LOGGER.info("Creating producers...");
        int producers = producerService.createProducers(topics, config.producersConfig());
        LOGGER.info("Created {} producers, took {} ms", producers, timer.elapsedAndResetAs(TimeUnit.MILLISECONDS));
        // TODO
    }

    @Override
    public void close() {
        topicService.close();
    }
}
