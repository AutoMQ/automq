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

import java.util.List;
import org.apache.kafka.tools.automq.perf.PerfConfig;
import org.apache.kafka.tools.automq.perf.TopicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfCommand implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerfCommand.class);
    private final PerfConfig config;
    private final TopicService topicService;

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
        LOGGER.info("Creating topics...");
        List<String> topics = topicService.createTopics(config.topicsConfig());
        LOGGER.info("Created {} topics", topics.size());
        // TODO
    }

    @Override
    public void close() {
        topicService.close();
    }
}
