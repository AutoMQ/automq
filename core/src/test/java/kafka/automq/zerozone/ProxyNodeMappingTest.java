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

package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdMetadata;
import kafka.automq.zerozone.ProxyNodeMapping.ProxyNode;
import kafka.server.MetadataCache;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.BrokerRegistration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class ProxyNodeMappingTest {
    static final String LISTENER_NAME = "BROKER";
    MetadataCache metadataCache;
    ProxyNodeMapping proxyNodeMapping;

    @BeforeEach
    public void setup() {
        metadataCache = mock(MetadataCache.class);
        proxyNodeMapping = new ProxyNodeMapping(new Node(1, "127.0.0.1", 9092), "az1", LISTENER_NAME, metadataCache);

        Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack = new HashMap<>();
        HashMap<Integer, BrokerRegistration> az1 = new HashMap<>();
        az1.put(3, brokerRegistration(2, "az1", "127.0.0.2", 9092));
        az1.put(4, brokerRegistration(1, "az1", "127.0.0.1", 9092));
        main2proxyByRack.put("az1", az1);
        HashMap<Integer, BrokerRegistration> az2 = new HashMap<>();
        az2.put(1, brokerRegistration(3, "az2", "127.0.0.3", 9092));
        az2.put(2, brokerRegistration(3, "az2", "127.0.0.3", 9092));
        main2proxyByRack.put("az2", az2);

        proxyNodeMapping.main2proxyByRack = main2proxyByRack;
    }

    @Test
    public void testGetRouteOutNode() {
        Node node;
        // case1
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az1"));
        assertEquals(-1, node.id());

        // case2
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(2, "az1", "127.0.0.2", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az1"));
        assertEquals(-1, node.id());

        // case3
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(1, "az1", "127.0.0.1", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az1"));
        assertEquals(1, node.id());

        // case4
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(1, "az1", "127.0.0.1", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az3"));
        assertEquals(1, node.id());

        // case5
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az3"));
        assertEquals(-1, node.id());

        // case6
        when(metadataCache.getPartitionLeaderNode(eq("TP"), eq(1)))
            .thenReturn(brokerRegistration(4, "az2", "127.0.0.4", 9092));
        node = proxyNodeMapping.getRouteOutNode("TP", 1, ClientIdMetadata.of("automq_az=az1"));
        assertEquals(4, node.id());
    }

    @Test
    public void testGetLeaderNode() {
        Node node;
        // case1
        when(metadataCache.getNode(eq(3)))
            .thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        node = proxyNodeMapping.getLeaderNode(3, ClientIdMetadata.of(""), LISTENER_NAME).get();
        assertEquals(3, node.id());

        // case2
        when(metadataCache.getNode(eq(3)))
            .thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        node = proxyNodeMapping.getLeaderNode(3, ClientIdMetadata.of("automq_az=az1"), LISTENER_NAME).get();
        assertEquals(2, node.id());

        // case3
        when(metadataCache.getNode(eq(3)))
            .thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        node = proxyNodeMapping.getLeaderNode(3, ClientIdMetadata.of("automq_az=az3"), LISTENER_NAME).get();
        assertEquals(3, node.id());

        // case4
        when(metadataCache.getNode(eq(2)))
            .thenReturn(brokerRegistration(2, "az1", "127.0.0.2", 9092));
        node = proxyNodeMapping.getLeaderNode(2, ClientIdMetadata.of("automq_az=az1"), LISTENER_NAME).get();
        assertEquals(2, node.id());
    }

    @Test
    public void testCalMain2proxyByRack() {
        Map<String, List<BrokerRegistration>> main2proxyByRack = new HashMap<>();
        List<BrokerRegistration> az1 = new ArrayList<>();
        az1.add(brokerRegistration(1, "az1", "127.0.0.1", 9092));
        az1.add(brokerRegistration(2, "az1", "127.0.0.2", 9092));
        main2proxyByRack.put("az1", az1);
        List<BrokerRegistration> az2 = new ArrayList<>();
        az2.add(brokerRegistration(3, "az2", "127.0.0.3", 9092));
        az2.add(brokerRegistration(4, "az2", "127.0.0.4", 9092));
        az2.add(brokerRegistration(5, "az2", "127.0.0.5", 9092));
        main2proxyByRack.put("az2", az2);

        Map<String, Map<Integer, BrokerRegistration>> rst = ProxyNodeMapping.calMain2proxyByRack(main2proxyByRack);
        assertEquals(2, rst.size());
        assertEquals(3, rst.get("az1").size());
        assertEquals(List.of(3, 4, 5), rst.get("az1").keySet().stream().sorted().toList());
        assertEquals(2, rst.get("az2").size());
        assertEquals(List.of(1, 2), rst.get("az2").keySet().stream().sorted().toList());
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testCalMain2proxyByRackV1Data")
    public void testCalMain2proxyByRackV1(String name, Map<String, Integer> rack2nodeCount) {
        AtomicInteger nodeIdAlloc = new AtomicInteger(0);
        Map<String, List<BrokerRegistration>> nodesByRack = new HashMap<>();
        rack2nodeCount.forEach((rack, count) -> {
            List<BrokerRegistration> brokers = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int nodeId = nodeIdAlloc.getAndIncrement();
                brokers.add(brokerRegistration(nodeId, rack, "127.0.0." + nodeId, 9092));
            }
            nodesByRack.put(rack, brokers);
        });
        Map<String, Map<Integer, BrokerRegistration>> rst = ProxyNodeMapping.calMain2proxyByRackV1(nodesByRack);
        Map<String, Map<Integer, Integer>> rstConvert = new HashMap<>();
        rst.forEach((r, m) ->
            m.forEach((k, v) ->
                rstConvert.computeIfAbsent(r, rack -> new HashMap<>()).put(k, v.id())));
        verifyCalMain2proxyByRackV1(nodesByRack, rst);
    }

    static Stream<Arguments> testCalMain2proxyByRackV1Data() {
        return Stream.of(
            Arguments.of(
                "basic",
                Map.of("az1", 3, "az2", 3, "az3", 3)
            ),
            Arguments.of(
                "rolling",
                Map.of("az1", 10, "az2", 9, "az3", 10)
            ),
            Arguments.of(
                "not balanced",
                Map.of("az1", 4, "az2", 5, "az3", 6)
            )
        );
    }

    static void verifyCalMain2proxyByRackV1(Map<String, List<BrokerRegistration>> nodesByRack,
        Map<String, Map<Integer, BrokerRegistration>> rst) {
        List<String> racks = nodesByRack.keySet().stream().toList();
        // Verify the main node has proxy node in other racks
        nodesByRack.forEach((mainRack, brokers) ->
            brokers.forEach(broker -> {
                for (String proxyRack : racks) {
                    if (proxyRack.equals(mainRack)) {
                        continue;
                    }
                    assertTrue(rst.get(proxyRack).containsKey(broker.id()));
                }
            }));

        // Verify the proxy workload is balanced.
        rst.forEach((proxyRack, main2proxy) -> {
            Map<Integer, Integer> proxyNodeId2Count = new HashMap<>();
            main2proxy.values().forEach(proxyBroker -> proxyNodeId2Count.compute(proxyBroker.id(), (k, v) -> v == null ? 1 : v + 1));
            int min = proxyNodeId2Count.values().stream().min(Integer::compareTo).get();
            int max = proxyNodeId2Count.values().stream().max(Integer::compareTo).get();
            assertTrue(max - min <= 1, "Proxy workload is not balanced in rack " + proxyRack + ": " + proxyNodeId2Count);
        });

        // Verify main-proxy dual
        List<String> sortedRacks = nodesByRack.entrySet().stream()
            .sorted(Comparator.comparingInt(o -> o.getValue().size()))
            .map(Map.Entry::getKey).toList();
        for (int i = 0; i < sortedRacks.size(); i++) {
            String mainRack = sortedRacks.get(i);
            for (int j = i + 1; j < sortedRacks.size(); j++) {
                String proxyRack = sortedRacks.get(j);
                nodesByRack.get(mainRack).forEach(mainNode -> {
                    BrokerRegistration proxyNode = rst.get(proxyRack).get(mainNode.id());
                    assertEquals(mainNode.id(), rst.get(mainRack).get(proxyNode.id()).id());
                });
            }
        }
    }

    @Test
    public void testTryFreeController() {
        List<ProxyNode> proxyNodes = new ArrayList<>();
        ProxyNode node1 = new ProxyNode(brokerRegistration(1, "az1", "127.0.0.1", 9092));
        node1.mainNodeIds.addAll(List.of(1000, 1001));
        proxyNodes.add(node1);
        ProxyNode node2 = new ProxyNode(brokerRegistration(2000, "az1", "127.0.0.1", 9092));
        node2.mainNodeIds.add(0);
        proxyNodes.add(node2);
        ProxyNodeMapping.tryFreeController(proxyNodes, 2);
        assertEquals(List.of(0), node1.mainNodeIds);
        assertEquals(List.of(1001, 1000), node2.mainNodeIds);
    }

    private static BrokerRegistration brokerRegistration(int id, String rack, String host, int port) {
        return new BrokerRegistration.Builder()
            .setId(id).setRack(Optional.of(rack))
            .setListeners(List.of(new Endpoint(LISTENER_NAME, SecurityProtocol.PLAINTEXT, host, port)))
            .build();
    }

}
