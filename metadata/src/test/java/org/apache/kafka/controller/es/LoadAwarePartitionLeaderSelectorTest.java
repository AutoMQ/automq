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

package org.apache.kafka.controller.es;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.MockRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadAwarePartitionLeaderSelectorTest {

    @Test
    public void testLoadAwarePartitionLeaderSelector() {
        List<Integer> aliveBrokers = List.of(0, 1, 2, 3, 4, 5);
        Set<Integer> brokerSet = new HashSet<>(aliveBrokers);
        int brokerToRemove = 5;
        MockRandom random = new MockRandom();
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random,
            aliveBrokers, broker -> broker != brokerToRemove);

        // fallback to random selector
        setUpCluster();
        Map<Integer, Double> brokerLoads = new HashMap<>();
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove, brokerLoads);
        Assertions.assertEquals(4000, brokerLoads.get(0));
        Assertions.assertEquals(4000, brokerLoads.get(1));
        Assertions.assertEquals(4000, brokerLoads.get(2));
        Assertions.assertEquals(4000, brokerLoads.get(3));
        Assertions.assertEquals(4000, brokerLoads.get(4));

        // load aware selector
        brokerLoads = setUpCluster();
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, broker -> broker != brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove, brokerLoads);
        Assertions.assertEquals(5990, brokerLoads.get(0));
        Assertions.assertEquals(7660, brokerLoads.get(1));
        Assertions.assertEquals(6720, brokerLoads.get(2));
        Assertions.assertEquals(7460, brokerLoads.get(3));
        Assertions.assertEquals(6170, brokerLoads.get(4));

        // test missing broker
        brokerLoads = setUpCluster();
        brokerLoads.remove(1);
        ClusterStats.getInstance().updateBrokerLoads(brokerLoads);
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, broker -> broker != brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove, brokerLoads);
        Assertions.assertEquals(6840, brokerLoads.get(0));
        Assertions.assertEquals(7280, brokerLoads.get(2));
        Assertions.assertEquals(7950, brokerLoads.get(3));
        Assertions.assertEquals(6930, brokerLoads.get(4));

        // tests exclude broker
        brokerLoads = setUpCluster();
        ClusterStats.getInstance().updateExcludedBrokers(Set.of(1));
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, broker -> broker != brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove, brokerLoads);
        Assertions.assertEquals(6970, brokerLoads.get(0));
        Assertions.assertEquals(5000, brokerLoads.get(1));
        Assertions.assertEquals(7210, brokerLoads.get(2));
        Assertions.assertEquals(7820, brokerLoads.get(3));
        Assertions.assertEquals(7000, brokerLoads.get(4));
    }

    private void randomSelect(LoadAwarePartitionLeaderSelector selector, int count, Set<Integer> brokerSet,
        int brokerToRemove, Map<Integer, Double> brokerLoads) {
        for (int i = 0; i < count; i++) {
            TopicPartition tp = new TopicPartition("topic", 0);
            int brokerId = selector.select(tp).orElse(-1);
            double partitionLoad = ClusterStats.getInstance().partitionLoad(tp);
            brokerLoads.compute(brokerId, (k, v) -> {
                if (v == null) {
                    return partitionLoad;
                }
                return v + partitionLoad;
            });
            Assertions.assertTrue(brokerSet.contains(brokerId));
            Assertions.assertTrue(brokerId != brokerToRemove);
        }
    }

    private Map<Integer, Double> setUpCluster() {
        Map<Integer, Double> brokerLoads = new HashMap<>();
        brokerLoads.put(0, 0.0);
        brokerLoads.put(1, 5000.0);
        brokerLoads.put(2, 3000.0);
        brokerLoads.put(3, 4000.0);
        brokerLoads.put(4, 2000.0);
        brokerLoads.put(5, 0.0);
        Map<TopicPartition, Double> partitionLoads = Map.of(
            new TopicPartition("topic", 0), 10.0
        );
        ClusterStats.getInstance().updateBrokerLoads(brokerLoads);
        ClusterStats.getInstance().updatePartitionLoads(partitionLoads);
        return brokerLoads;
    }
}
