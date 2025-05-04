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
import org.apache.kafka.common.WeightedRandomList;
import org.apache.kafka.metadata.BrokerRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class LoadAwarePartitionLeaderSelector implements PartitionLeaderSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadAwarePartitionLeaderSelector.class);
    private final WeightedRandomList<Integer> brokerLoads;
    private final RandomPartitionLeaderSelector randomSelector;

    public LoadAwarePartitionLeaderSelector(List<BrokerRegistration> aliveBrokers, BrokerRegistration brokerToRemove) {
        this(new Random(), aliveBrokers, brokerToRemove);
    }

    public LoadAwarePartitionLeaderSelector(Random r, List<BrokerRegistration> aliveBrokers, BrokerRegistration brokerToRemove) {
        Map<Integer, Double> brokerLoadMap = ClusterStats.getInstance().brokerLoads();
        if (brokerLoadMap == null) {
            this.brokerLoads = null;
            LOGGER.warn("No broker loads available, using random partition leader selector");
            this.randomSelector = new RandomPartitionLeaderSelector(aliveBrokers.stream().map(BrokerRegistration::id).collect(Collectors.toList()), brokerId -> brokerId != brokerToRemove.id());
            return;
        }
        Set<Integer> excludedBrokers = ClusterStats.getInstance().excludedBrokers();
        if (excludedBrokers == null) {
            excludedBrokers = new HashSet<>();
        }
        List<BrokerRegistration> availableBrokers = new ArrayList<>();
        for (BrokerRegistration broker : aliveBrokers) {
            if (!excludedBrokers.contains(broker.id()) && broker.id() != brokerToRemove.id()) {
                availableBrokers.add(broker);
            }
        }
        brokerLoads = new WeightedRandomList<>(r);
        for (BrokerRegistration broker : availableBrokers) {
            int brokerId = broker.id();
            if (!broker.rack().equals(brokerToRemove.rack()) || !brokerLoadMap.containsKey(brokerId)) {
                continue;
            }
            double load = Math.max(1, brokerLoadMap.get(brokerId));
            // allocation weight is inversely proportional to the load
            brokerLoads.add(new WeightedRandomList.Entity<>(brokerId, 1 / load));
        }
        brokerLoads.update();
        this.randomSelector = new RandomPartitionLeaderSelector(availableBrokers.stream().map(BrokerRegistration::id).collect(Collectors.toList()), id -> true);
    }

    @Override
    public Optional<Integer> select(TopicPartition tp) {
        try {
            if (this.brokerLoads == null || brokerLoads.size() == 0) {
                return randomSelector.select(tp);
            }
            double tpLoad = ClusterStats.getInstance().partitionLoad(tp);
            if (tpLoad == ClusterStats.INVALID) {
                return randomSelector.select(tp);
            }
            WeightedRandomList.Entity<Integer> candidate = brokerLoads.next();
            if (candidate == null) {
                return randomSelector.select(tp);
            }
            double load = 1 / candidate.weight() + tpLoad;
            candidate.setWeight(1 / load);
            brokerLoads.update();
            return Optional.of(candidate.entity());
        } catch (Throwable t) {
            return randomSelector.select(tp);
        }
    }
}
