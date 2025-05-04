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

package org.apache.kafka.controller.es;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClusterStats {
    public static final double INVALID = -1;
    private static final long EXPIRE_TIME_MS = 60000; // 1 minute
    private static volatile ClusterStats instance = null;
    private volatile Set<Integer> excludedBrokers;
    private volatile Map<Integer, Double> brokerLoads = new HashMap<>();
    private volatile Map<TopicPartition, Double> partitionLoads = new HashMap<>();
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
