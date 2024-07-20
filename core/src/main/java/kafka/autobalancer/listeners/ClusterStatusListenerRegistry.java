/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.listeners;

import java.util.ArrayList;
import java.util.List;

public class ClusterStatusListenerRegistry {
    private final List<BrokerStatusListener> brokerListeners = new ArrayList<>();
    private final List<TopicPartitionStatusListener> topicPartitionListeners = new ArrayList<>();

    public void register(BrokerStatusListener listener) {
        brokerListeners.add(listener);
    }

    public void register(TopicPartitionStatusListener listener) {
        topicPartitionListeners.add(listener);
    }

    public List<BrokerStatusListener> brokerListeners() {
        return brokerListeners;
    }

    public List<TopicPartitionStatusListener> topicPartitionListeners() {
        return topicPartitionListeners;
    }
}
