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

package kafka.automq.kafkalinking;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

public interface KafkaLinkingManager {
    void addPartitions(Set<TopicPartition> partitions);

    void removePartitions(Set<TopicPartition> partitions);

    void shutdown();
}
