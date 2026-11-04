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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tag("S3Unit")
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

    /**
     * Given two brokers per rack, enabling cross-rack placement makes every eligible rack usable.
     * Disabling it retains the single same-rack destination.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCrossRackPlacement(boolean enabled) {
        setUpCluster();
        List<BrokerRegistration> brokers = multiRackBrokers();
        LoadAwarePartitionLeaderSelector selector =
            new LoadAwarePartitionLeaderSelector(new MockRandom(), brokers, brokers.get(5), enabled);
        Set<Integer> selected = selectDestinations(selector, new TopicPartition("topic", 0), 2000);
        Assertions.assertEquals(enabled ? Set.of(0, 1, 2, 3, 4) : Set.of(2), selected);
    }

    /**
     * Given cross-rack placement, departing, excluded and unmeasured brokers are not weighted candidates.
     */
    @Test
    public void testCrossRackEligibility() {
        Map<Integer, Double> loads = setUpCluster();
        loads.remove(4);
        ClusterStats.getInstance().updateExcludedBrokers(Set.of(1));
        List<BrokerRegistration> brokers = multiRackBrokers();
        LoadAwarePartitionLeaderSelector selector =
            new LoadAwarePartitionLeaderSelector(new MockRandom(), brokers, brokers.get(5), true);
        Assertions.assertEquals(Set.of(0, 2, 3),
            selectDestinations(selector, new TopicPartition("topic", 0), 2000));
    }

    /**
     * Given missing broker or partition metrics, both modes retain the existing random fallback.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCrossRackFallback(boolean enabled) {
        List<BrokerRegistration> brokers = multiRackBrokers();
        for (Map<Integer, Double> loads : List.of(Map.<Integer, Double>of(), setUpCluster())) {
            ClusterStats.getInstance().updateBrokerLoads(loads);
            ClusterStats.getInstance().updateExcludedBrokers(Set.of(1));
            LoadAwarePartitionLeaderSelector selector =
                new LoadAwarePartitionLeaderSelector(new MockRandom(), brokers, brokers.get(5), enabled);
            Assertions.assertEquals(Set.of(0, 2, 3, 4),
                selectDestinations(selector, new TopicPartition("missing-metrics", 0), 20));
        }
        ClusterStats.getInstance().updateBrokerLoads(null);
        LoadAwarePartitionLeaderSelector selector =
            new LoadAwarePartitionLeaderSelector(new MockRandom(), brokers, brokers.get(5), enabled);
        // With unavailable broker metrics, preserve the legacy fallback's exclusion semantics.
        Assertions.assertEquals(Set.of(0, 1, 2, 3, 4),
            selectDestinations(selector, new TopicPartition("topic", 0), 20));
    }

    /**
     * Given no eligible destinations, neither placement mode selects a departing or excluded broker.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testNoEligibleDestination(boolean enabled) {
        setUpCluster();
        ClusterStats.getInstance().updateExcludedBrokers(Set.of(0, 1, 2, 3, 4));
        List<BrokerRegistration> brokers = multiRackBrokers();
        LoadAwarePartitionLeaderSelector selector =
            new LoadAwarePartitionLeaderSelector(new MockRandom(), brokers, brokers.get(5), enabled);
        Assertions.assertEquals(Optional.empty(), selector.select(new TopicPartition("topic", 0)));
    }

    private List<BrokerRegistration> multiRackBrokers() {
        return List.of(
            new BrokerRegistration.Builder().setId(0).setRack(Optional.of("rack-a")).build(),
            new BrokerRegistration.Builder().setId(1).setRack(Optional.of("rack-c")).build(),
            new BrokerRegistration.Builder().setId(2).setRack(Optional.of("rack-b")).build(),
            new BrokerRegistration.Builder().setId(3).setRack(Optional.of("rack-a")).build(),
            new BrokerRegistration.Builder().setId(4).setRack(Optional.of("rack-c")).build(),
            new BrokerRegistration.Builder().setId(5).setRack(Optional.of("rack-b")).build());
    }

    private Set<Integer> selectDestinations(LoadAwarePartitionLeaderSelector selector, TopicPartition tp, int count) {
        Set<Integer> selected = new HashSet<>();
        for (int i = 0; i < count; i++) {
            selected.add(selector.select(tp).orElseThrow());
        }
        return selected;
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
