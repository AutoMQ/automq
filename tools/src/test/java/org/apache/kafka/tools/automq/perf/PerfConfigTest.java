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

import org.apache.kafka.clients.admin.NewTopic;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
class PerfConfigTest {

    /**
     * Given topic creation arguments, the generated Kafka request should preserve all topic settings.
     */
    @Test
    void shouldBuildNewTopicWithConfiguredReplicationFactorAndTopicConfigs() {
        PerfConfig config = new PerfConfig(new String[] {
            "--topic-prefix", "configured",
            "--topics", "2",
            "--partitions-per-topic", "3",
            "--replication-factor", "2",
            "--topic-configs", "cleanup.policy=compact", "retention.ms=60000"
        });

        List<NewTopic> topics = TopicService.newTopics(config.topicsConfig());

        assertEquals(2, topics.size());
        assertEquals("__automq_perf_configured_0003_0000000", topics.get(0).name());
        assertEquals(3, topics.get(0).numPartitions());
        assertEquals((short) 2, topics.get(0).replicationFactor());
        assertEquals(Map.of("cleanup.policy", "compact", "retention.ms", "60000"), topics.get(0).configs());
    }

    /**
     * When replication factor is omitted, topic creation should remain backward compatible with one replica.
     */
    @Test
    void shouldDefaultReplicationFactorToOne() {
        PerfConfig config = new PerfConfig(new String[] {"--topic-prefix", "default"});

        NewTopic topic = TopicService.newTopics(config.topicsConfig()).get(0);
        NewTopic topicFromCompatibleConstructor = TopicService.newTopics(
            new TopicService.TopicsConfig("default", 1, 1, Map.of())).get(0);

        assertEquals((short) 1, topic.replicationFactor());
        assertEquals((short) 1, topicFromCompatibleConstructor.replicationFactor());
    }

    /**
     * Given an invalid replication factor, configuration parsing should reject it before creating topics.
     */
    @Test
    void shouldRejectInvalidReplicationFactor() {
        assertThrows(ArgumentParserException.class,
            () -> PerfConfig.parser().parseArgs(new String[] {"--replication-factor", "0"}));
        assertThrows(ArgumentParserException.class,
            () -> PerfConfig.parser().parseArgs(new String[] {"--replication-factor", "32768"}));
        assertThrows(ArgumentParserException.class,
            () -> PerfConfig.parser().parseArgs(new String[] {"--replication-factor", "invalid"}));
        assertThrows(IllegalArgumentException.class,
            () -> new TopicService.TopicsConfig("invalid", 1, 1, 32768, Map.of()));
    }
}
