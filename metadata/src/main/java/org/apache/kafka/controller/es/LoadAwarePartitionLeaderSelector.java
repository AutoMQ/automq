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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Predicate;

public class LoadAwarePartitionLeaderSelector implements PartitionLeaderSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadAwarePartitionLeaderSelector.class);
    private final PriorityQueue<BrokerLoad> brokerLoads;
    private final RandomPartitionLeaderSelector randomSelector;
    private final Map<Integer, Double> brokerLoadMap;

    public LoadAwarePartitionLeaderSelector(List<Integer> aliveBrokers, Predicate<Integer> brokerPredicate) {
        brokerLoadMap = ClusterStats.getInstance().brokerLoads();
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
        this.brokerLoads = new PriorityQueue<>();
        for (int brokerId : availableBrokers) {
            if (!brokerPredicate.test(brokerId) || !brokerLoadMap.containsKey(brokerId)) {
                continue;
            }
            brokerLoads.offer(new BrokerLoad(brokerId, brokerLoadMap.get(brokerId)));
        }
        this.randomSelector = new RandomPartitionLeaderSelector(availableBrokers, brokerPredicate);
    }

    @Override
    public Optional<Integer> select(TopicPartition tp) {
        if (this.brokerLoads == null || brokerLoads.isEmpty()) {
            return randomSelector.select(tp);
        }
        double tpLoad = ClusterStats.getInstance().partitionLoad(tp);
        if (tpLoad == ClusterStats.INVALID) {
            return randomSelector.select(tp);
        }
        BrokerLoad candidate = brokerLoads.poll();
        if (candidate == null) {
            return randomSelector.select(tp);
        }
        double load = candidate.load() + tpLoad;
        candidate.setLoad(load);
        brokerLoadMap.put(candidate.brokerId(), load);
        brokerLoads.offer(candidate);
        return Optional.of(candidate.brokerId());
    }

    public static class BrokerLoad implements Comparable<BrokerLoad> {
        private final int brokerId;
        private double load;

        public BrokerLoad(int brokerId, double load) {
            this.brokerId = brokerId;
            this.load = load;
        }

        public int brokerId() {
            return brokerId;
        }

        public double load() {
            return load;
        }

        public void setLoad(double load) {
            this.load = load;
        }

        @Override
        public int compareTo(BrokerLoad o) {
            return Double.compare(load, o.load);
        }
    }
}
