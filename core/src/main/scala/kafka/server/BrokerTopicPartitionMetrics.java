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

package kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;

import java.util.Map;
import java.util.Optional;

public class BrokerTopicPartitionMetrics extends BrokerTopicMetrics {
    public BrokerTopicPartitionMetrics(TopicPartition topicPartition) {
        super(Optional.of(topicPartition.topic()), Map.of("partition", String.valueOf(topicPartition.partition())), false);
    }
}
