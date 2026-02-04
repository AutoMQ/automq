/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.apache.kafka.common.utils.Exit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PerfConfig}.
 */
public class PerfConfigTest {

    @BeforeEach
    void setUp() {
        // Prevent System.exit() from terminating the test process
        Exit.setExitProcedure((statusCode, message) -> {
            throw new RuntimeException("Exit called with status " + statusCode + ": " + message);
        });
    }

    @AfterEach
    void tearDown() {
        Exit.resetExitProcedure();
    }

    @Test
    void testDefaultValues() {
        String[] args = {"--bootstrap-server", "localhost:9092"};
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("localhost:9092", config.bootstrapServer);
        assertEquals(1, config.topics);
        assertEquals(1, config.partitionsPerTopic);
        assertEquals(1, config.producersPerTopic);
        assertEquals(1, config.groupsPerTopic);
        assertEquals(1, config.consumersPerGroup);
        assertEquals(1024, config.recordSize);
        assertEquals(1000, config.sendRate);
        assertEquals(5, config.testDurationMinutes);
        assertEquals(1, config.warmupDurationMinutes);
        assertEquals(0, config.backlogDurationSeconds);
        assertFalse(config.reset);
        assertTrue(config.awaitTopicReady);
    }

    @Test
    void testBootstrapServerDefault() {
        String[] args = {};
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("localhost:9092", config.bootstrapServer);
    }

    @Test
    void testBacklogDurationParsing() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--backlog-duration", "300"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(300, config.backlogDurationSeconds);
    }

    @Test
    void testBacklogDurationMinimumValidation() {
        // backlog-duration must be >= 300 (or 0 for disabled)
        // Values between 1 and 299 should fail
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--backlog-duration", "100"
        };
        
        assertThrows(RuntimeException.class, () -> new PerfConfig(args));
    }

    @Test
    void testBacklogDurationZeroIsAllowed() {
        // 0 means disabled, should be allowed
        String[] args = {
            "--bootstrap-server", "localhost:9092"
            // backlog-duration defaults to 0
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(0, config.backlogDurationSeconds);
    }

    @Test
    void testBacklogDurationVsGroupDelayValidation() {
        // backlog >= groups * delay
        // With 2 groups and 200s delay, backlog must be >= 400
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--backlog-duration", "300",
            "--groups-per-topic", "2",
            "--group-start-delay", "200"
        };
        
        assertThrows(IllegalArgumentException.class, () -> new PerfConfig(args));
    }

    @Test
    void testBacklogDurationVsGroupDelayValid() {
        // backlog >= groups * delay
        // With 2 groups and 100s delay, backlog 300 should be valid
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--backlog-duration", "300",
            "--groups-per-topic", "2",
            "--group-start-delay", "100"
        };
        
        PerfConfig config = new PerfConfig(args);
        assertEquals(300, config.backlogDurationSeconds);
        assertEquals(2, config.groupsPerTopic);
        assertEquals(100, config.groupStartDelaySeconds);
    }

    @Test
    void testTopicConfigsParsing() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "-T", "retention.ms=3600000", "cleanup.policy=delete"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("3600000", config.topicConfigs.get("retention.ms"));
        assertEquals("delete", config.topicConfigs.get("cleanup.policy"));
    }

    @Test
    void testProducerConfigsParsing() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "-P", "acks=all", "linger.ms=5"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("all", config.producerConfigs.get("acks"));
        assertEquals("5", config.producerConfigs.get("linger.ms"));
    }

    @Test
    void testConsumerConfigsParsing() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "-C", "max.poll.records=500", "fetch.min.bytes=1024"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("500", config.consumerConfigs.get("max.poll.records"));
        assertEquals("1024", config.consumerConfigs.get("fetch.min.bytes"));
    }

    @Test
    void testSendRateParsing() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--send-rate", "5000"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(5000, config.sendRate);
    }

    @Test
    void testSendThroughputConversion() {
        // --send-throughput in MB/s should be converted to send-rate
        // With record-size=1024 bytes, 1 MB/s = 1024 msg/s
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--send-throughput", "1.0",
            "--record-size", "1024"
        };
        PerfConfig config = new PerfConfig(args);
        
        // 1 MB/s / 1024 bytes = 1024 msg/s
        assertEquals(1024, config.sendRate);
    }

    @Test
    void testSendThroughputConversionWithDifferentRecordSize() {
        // With record-size=512 bytes, 1 MB/s = 2048 msg/s
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--send-throughput", "1.0",
            "--record-size", "512"
        };
        PerfConfig config = new PerfConfig(args);
        
        // 1 MB/s / 512 bytes = 2048 msg/s
        assertEquals(2048, config.sendRate);
    }

    @Test
    void testTopicPrefixGeneration() {
        String[] args = {"--bootstrap-server", "localhost:9092"};
        PerfConfig config = new PerfConfig(args);
        
        // When no prefix is specified, a random one should be generated
        assertNotNull(config.topicPrefix);
        assertTrue(config.topicPrefix.startsWith("topic_"));
    }

    @Test
    void testTopicPrefixCustom() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--topic-prefix", "my-test-topic"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("my-test-topic", config.topicPrefix);
    }

    @Test
    void testResetFlag() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--reset"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertTrue(config.reset);
    }

    @Test
    void testReuseTopicsFlag() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--reuse-topics"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertTrue(config.reuseTopics);
    }

    @Test
    void testCatchupTopicPrefix() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--catchup-topic-prefix", "existing-topic"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals("existing-topic", config.catchupTopicPrefix);
    }

    @Test
    void testConsumersDuringCatchupPercentage() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--consumers-during-catchup", "50"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(50, config.consumersDuringCatchupPercentage);
    }

    @Test
    void testConsumersDuringCatchupPercentageDefault() {
        String[] args = {"--bootstrap-server", "localhost:9092"};
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(100, config.consumersDuringCatchupPercentage);
    }

    @Test
    void testConsumersDuringCatchupPercentageValidation() {
        // Must be between 0 and 100
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--consumers-during-catchup", "150"
        };
        
        assertThrows(RuntimeException.class, () -> new PerfConfig(args));
    }

    @Test
    void testMultipleTopicsAndPartitions() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--topics", "5",
            "--partitions-per-topic", "10"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(5, config.topics);
        assertEquals(10, config.partitionsPerTopic);
    }

    @Test
    void testSendRateDuringCatchup() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--send-rate", "1000",
            "--send-rate-during-catchup", "500"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(1000, config.sendRate);
        assertEquals(500, config.sendRateDuringCatchup);
    }

    @Test
    void testSendRateDuringCatchupDefaultsToSendRate() {
        String[] args = {
            "--bootstrap-server", "localhost:9092",
            "--send-rate", "2000"
        };
        PerfConfig config = new PerfConfig(args);
        
        assertEquals(2000, config.sendRate);
        assertEquals(2000, config.sendRateDuringCatchup);
    }
}
