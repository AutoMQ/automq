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

import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.metadata.BrokerRegistration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadAwarePartitionLeaderSelectorTest {

    @Test
    public void testLoadAwarePartitionLeaderSelector() {
        List<BrokerRegistration> aliveBrokers = List.of(
            new BrokerRegistration.Builder().setId(0).build(),
            new BrokerRegistration.Builder().setId(1).build(),
            new BrokerRegistration.Builder().setId(2).build(),
            new BrokerRegistration.Builder().setId(3).build(),
            new BrokerRegistration.Builder().setId(4).build(),
            new BrokerRegistration.Builder().setId(5).build());

        Set<Integer> brokerSet = aliveBrokers.stream().map(BrokerRegistration::id).collect(Collectors.toSet());
        BrokerRegistration brokerToRemove = aliveBrokers.get(aliveBrokers.size() - 1);
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, brokerToRemove);

        // fallback to random selector
        for (int i = 0; i < 100; i++) {
            int brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
            Assertions.assertTrue(brokerSet.contains(brokerId));
            Assertions.assertTrue(brokerId != brokerToRemove.id());
        }

        // load aware selector
        Map<Integer, Double> brokerLoads = setUpCluster();
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, brokerToRemove);

        int brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
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

        // tests exclude broker
        brokerLoads = setUpCluster();
        ClusterLoads.getInstance().updateExcludedBrokers(Set.of(1));
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, brokerToRemove);

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

    @Test
    public void testLoadAwarePartitionLeaderSelectorWithRack() {
        String rackA = "rack-a";
        String rackB = "rack-b";
        List<BrokerRegistration> aliveBrokers = List.of(
            new BrokerRegistration.Builder().setId(0).setRack(Optional.of(rackA)).build(),
            new BrokerRegistration.Builder().setId(1).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(2).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(3).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(4).setRack(Optional.of(rackB)).build());

        Set<Integer> brokerSet = aliveBrokers.stream().map(BrokerRegistration::id).collect(Collectors.toSet());
        setUpCluster();
        BrokerRegistration brokerToRemove = aliveBrokers.get(0);
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, brokerToRemove);

        // fallback to random selector
        for (int i = 0; i < 100; i++) {
            int brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
            Assertions.assertTrue(brokerSet.contains(brokerId));
            Assertions.assertTrue(brokerId != brokerToRemove.id());
        }

        // load aware selector
        Map<Integer, Double> brokerLoads = setUpCluster();
        BrokerRegistration brokerToRemove1 = aliveBrokers.get(1);
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(aliveBrokers, brokerToRemove1);

        int brokerId = loadAwarePartitionLeaderSelector.select(new TopicPartition("topic", 0)).orElse(-1);
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
        ClusterLoads.getInstance().updateBrokerLoads(brokerLoads);
        ClusterLoads.getInstance().updatePartitionLoads(partitionLoads);
        return brokerLoads;
    }
}
