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
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, broker -> broker != brokerToRemove);

        // fallback to random selector
        int brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertTrue(brokerId != brokerToRemove);

        // load aware selector
        Map<Integer, Double> brokerLoads = setUpCluster();
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, broker -> broker != brokerToRemove);

        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 1)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 2)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(1, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 3)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);

        Assertions.assertEquals(35.0, brokerLoads.get(0));
        Assertions.assertEquals(25.0, brokerLoads.get(1));
        Assertions.assertEquals(20.0, brokerLoads.get(2));
        Assertions.assertEquals(30.0, brokerLoads.get(3));
        Assertions.assertEquals(40.0, brokerLoads.get(4));
        Assertions.assertEquals(50.0, brokerLoads.get(5));

        // test missing broker
        brokerLoads = setUpCluster();
        brokerLoads.remove(1);
        ClusterStats.getInstance().updateBrokerLoads(brokerLoads);
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, broker -> broker != brokerToRemove);

        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 1)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 2)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 3)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(2, brokerId);

        Assertions.assertEquals(30.0, brokerLoads.get(0));
        Assertions.assertEquals(40.0, brokerLoads.get(2));
        Assertions.assertEquals(30.0, brokerLoads.get(3));
        Assertions.assertEquals(40.0, brokerLoads.get(4));
        Assertions.assertEquals(50.0, brokerLoads.get(5));


        // tests exclude broker
        brokerLoads = setUpCluster();
        ClusterStats.getInstance().updateExcludedBrokers(Set.of(1));
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, broker -> broker != brokerToRemove);

        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 1)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 2)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(0, brokerId);
        brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 3)).orElse(-1);
        Assertions.assertTrue(brokerSet.contains(brokerId));
        Assertions.assertEquals(2, brokerId);

        Assertions.assertEquals(30.0, brokerLoads.get(0));
        Assertions.assertEquals(10.0, brokerLoads.get(1));
        Assertions.assertEquals(40.0, brokerLoads.get(2));
        Assertions.assertEquals(30.0, brokerLoads.get(3));
        Assertions.assertEquals(40.0, brokerLoads.get(4));
        Assertions.assertEquals(50.0, brokerLoads.get(5));
    }

    private Map<Integer, Double> setUpCluster() {
        Map<Integer, Double> brokerLoads = new HashMap<>();
        brokerLoads.put(0, 0.0);
        brokerLoads.put(1, 10.0);
        brokerLoads.put(2, 20.0);
        brokerLoads.put(3, 30.0);
        brokerLoads.put(4, 40.0);
        brokerLoads.put(5, 50.0);
        Map<TopicPartition, Double> partitionLoads = Map.of(
                new TopicPartition("topic", 0), 5.0,
                new TopicPartition("topic", 1), 10.0,
                new TopicPartition("topic", 2), 15.0,
                new TopicPartition("topic", 3), 20.0
        );
        ClusterStats.getInstance().updateBrokerLoads(brokerLoads);
        ClusterStats.getInstance().updatePartitionLoads(partitionLoads);
        return brokerLoads;
    }
}
