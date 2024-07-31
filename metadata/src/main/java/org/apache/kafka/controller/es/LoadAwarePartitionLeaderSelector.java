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

package org.apache.kafka.controller.es;

import java.util.Random;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.WeightedRandomList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class LoadAwarePartitionLeaderSelector implements PartitionLeaderSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadAwarePartitionLeaderSelector.class);
    private final WeightedRandomList<Integer> brokerLoads;
    private final RandomPartitionLeaderSelector randomSelector;

    public LoadAwarePartitionLeaderSelector(List<Integer> aliveBrokers, Predicate<Integer> brokerPredicate) {
        this(new Random(), aliveBrokers, brokerPredicate);
    }

    public LoadAwarePartitionLeaderSelector(Random r, List<Integer> aliveBrokers, Predicate<Integer> brokerPredicate) {
        Map<Integer, Double> brokerLoadMap = ClusterStats.getInstance().brokerLoads();
        if (brokerLoadMap == null) {
            this.brokerLoads = null;
            LOGGER.warn("No broker loads available, using random partition leader selector");
            this.randomSelector = new RandomPartitionLeaderSelector(aliveBrokers, brokerPredicate);
            return;
        }
        Set<Integer> excludedBrokers = ClusterStats.getInstance().excludedBrokers();
        if (excludedBrokers == null) {
            excludedBrokers = new HashSet<>();
        }
        List<Integer> availableBrokers = new ArrayList<>();
        for (int broker : aliveBrokers) {
            if (!excludedBrokers.contains(broker)) {
                availableBrokers.add(broker);
            }
        }
        brokerLoads = new WeightedRandomList<>(r);
        for (int brokerId : availableBrokers) {
            if (!brokerPredicate.test(brokerId) || !brokerLoadMap.containsKey(brokerId)) {
                continue;
            }
            double load = Math.max(1, brokerLoadMap.get(brokerId));
            // allocation weight is inversely proportional to the load
            brokerLoads.add(new WeightedRandomList.Entity<>(brokerId, 1 / load));
        }
        brokerLoads.update();
        this.randomSelector = new RandomPartitionLeaderSelector(availableBrokers, brokerPredicate);
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
