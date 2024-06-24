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

package org.apache.kafka.controller.es;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public class ClusterStats {
    public static final double INVALID = -1;
    private static final long EXPIRE_TIME_MS = 60000; // 1 minute
    private volatile static ClusterStats instance = null;
    private volatile Set<Integer> excludedBrokers;
    private volatile Map<Integer, Double> brokerLoads;
    private volatile Map<TopicPartition, Double> partitionLoads;
    private volatile long lastUpdateTime = 0;

    private ClusterStats() {
    }

    public static ClusterStats getInstance() {
        if (instance == null) {
            synchronized (ClusterStats.class) {
                if (instance == null) {
                    instance = new ClusterStats();
                }
            }
        }
        return instance;
    }

    public void updateExcludedBrokers(Set<Integer> excludedBrokers) {
        this.excludedBrokers = excludedBrokers;
    }

    public Set<Integer> excludedBrokers() {
        return excludedBrokers;
    }

    public Map<Integer, Double> brokerLoads() {
        if (System.currentTimeMillis() - lastUpdateTime > EXPIRE_TIME_MS) {
            return null;
        }
        return brokerLoads;
    }

    public double partitionLoad(TopicPartition tp) {
        return partitionLoads.getOrDefault(tp, INVALID);
    }

    public void updateBrokerLoads(Map<Integer, Double> brokerLoads) {
        this.brokerLoads = brokerLoads;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public void updatePartitionLoads(Map<TopicPartition, Double> partitionLoads) {
        this.partitionLoads = partitionLoads;
    }
}
