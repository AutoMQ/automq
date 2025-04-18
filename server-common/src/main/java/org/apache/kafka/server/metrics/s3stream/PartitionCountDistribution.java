/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package org.apache.kafka.server.metrics.s3stream;

import java.util.Map;

public class PartitionCountDistribution {
    private final int brokerId;
    private final String rack;
    private final Map<String, Integer> topicPartitionCount;

    public PartitionCountDistribution(int brokerId, String rack, Map<String, Integer> topicPartitionCount) {
        this.brokerId = brokerId;
        this.rack = rack;
        this.topicPartitionCount = topicPartitionCount;
    }

    public int brokerId() {
        return brokerId;
    }

    public String rack() {
        return rack;
    }

    public Map<String, Integer> topicPartitionCount() {
        return topicPartitionCount;
    }
}
