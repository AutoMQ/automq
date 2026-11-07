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
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.util.MockRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LoadAwarePartitionLeaderSelectorTest {

    @AfterEach
    public void tearDown() {
        ClusterStats.getInstance().updateExcludedBrokers(Collections.emptySet());
        ClusterStats.getInstance().updateBrokerLoads(Collections.emptyMap());
        ClusterStats.getInstance().updatePartitionLoads(Collections.emptyMap());
    }

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
        MockRandom random = new MockRandom();
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);

        // fallback to random selector
        setUpCluster();
        Map<Integer, Double> brokerLoads = new HashMap<>();
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
        Assertions.assertEquals(4000, brokerLoads.get(0));
        Assertions.assertEquals(4000, brokerLoads.get(1));
        Assertions.assertEquals(4000, brokerLoads.get(2));
        Assertions.assertEquals(4000, brokerLoads.get(3));
        Assertions.assertEquals(4000, brokerLoads.get(4));

        // load aware selector
        brokerLoads = setUpCluster();
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
        Assertions.assertEquals(5990, brokerLoads.get(0));
        Assertions.assertEquals(7660, brokerLoads.get(1));
        Assertions.assertEquals(6720, brokerLoads.get(2));
        Assertions.assertEquals(7460, brokerLoads.get(3));
        Assertions.assertEquals(6170, brokerLoads.get(4));

        // test missing broker
        brokerLoads = setUpCluster();
        brokerLoads.remove(1);
        ClusterStats.getInstance().updateBrokerLoads(brokerLoads);
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
        Assertions.assertEquals(6840, brokerLoads.get(0));
        Assertions.assertEquals(7280, brokerLoads.get(2));
        Assertions.assertEquals(7950, brokerLoads.get(3));
        Assertions.assertEquals(6930, brokerLoads.get(4));

        // tests exclude broker
        brokerLoads = setUpCluster();
        ClusterStats.getInstance().updateExcludedBrokers(Set.of(1));
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
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

    @Test
    public void testLoadAwarePartitionLeaderSelectorWithRack() {
        String rackA = "rack-a";
        String rackB = "rack-b";
        List<BrokerRegistration> aliveBrokers = List.of(
            new BrokerRegistration.Builder().setId(0).setRack(Optional.of(rackA)).build(),
            new BrokerRegistration.Builder().setId(1).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(2).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(3).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(4).setRack(Optional.of(rackB)).build(),
            new BrokerRegistration.Builder().setId(5).setRack(Optional.of(rackB)).build());

        Set<Integer> brokerSet = aliveBrokers.stream().map(BrokerRegistration::id).collect(Collectors.toSet());
        setUpCluster();
        BrokerRegistration brokerToRemove = aliveBrokers.get(0);
        MockRandom random = new MockRandom();
        LoadAwarePartitionLeaderSelector loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);

        // fallback to random selector
        Map<Integer, Double> brokerLoads = new HashMap<>();
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
        Assertions.assertEquals(4000, brokerLoads.get(1));
        Assertions.assertEquals(4000, brokerLoads.get(2));
        Assertions.assertEquals(4000, brokerLoads.get(3));
        Assertions.assertEquals(4000, brokerLoads.get(4));
        Assertions.assertEquals(4000, brokerLoads.get(5));

        // load aware selector
        brokerLoads = setUpCluster();
        brokerToRemove = aliveBrokers.get(1);
        loadAwarePartitionLeaderSelector = new LoadAwarePartitionLeaderSelector(random, aliveBrokers, brokerToRemove);
        randomSelect(loadAwarePartitionLeaderSelector, 2000, brokerSet, brokerToRemove.id(), brokerLoads);
        Assertions.assertEquals(0, brokerLoads.get(0));
        Assertions.assertEquals(5000, brokerLoads.get(1));
        Assertions.assertEquals(7330, brokerLoads.get(2));
        Assertions.assertEquals(7840, brokerLoads.get(3));
        Assertions.assertEquals(7120, brokerLoads.get(4));
        Assertions.assertEquals(6710, brokerLoads.get(5));
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
