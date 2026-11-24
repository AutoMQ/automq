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
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.ClusterDelta;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class ProxyNodeMappingTest {
    static final String LISTENER_NAME = "BROKER";
    MetadataCache metadataCache;
    ZeroZoneConfig zeroZoneConfig;
    ProxyNodeMapping proxyNodeMapping;

    @BeforeEach
    public void setup() {
        metadataCache = mock(MetadataCache.class);
        zeroZoneConfig = mock(ZeroZoneConfig.class);
        when(zeroZoneConfig.excludeZones()).thenReturn(Set.of());
        proxyNodeMapping = new ProxyNodeMapping(new Node(1, "127.0.0.1", 9092), "az1", LISTENER_NAME, metadataCache, zeroZoneConfig);

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

    /**
     * Given the current node (1, az1) proxies main node 4 and node 3 (az2) proxies main node 1,
     * when the mapping is withdrawn,
     * then leader hints and metadata responses return the main nodes for clients of every rack.
     */
    @Test
    public void testWithdraw() {
        when(metadataCache.getNode(eq(4))).thenReturn(brokerRegistration(4, "az2", "127.0.0.4", 9092));
        assertEquals(1, proxyNodeMapping.getLeaderNode(4, ClientIdMetadata.of("automq_az=az1"), LISTENER_NAME).get().id());

        proxyNodeMapping.withdraw();

        // The current node no longer offers itself as the proxy of main node 4.
        assertEquals(4, proxyNodeMapping.getLeaderNode(4, ClientIdMetadata.of("automq_az=az1"), LISTENER_NAME).get().id());
        // The az2 client is no longer sent to proxy 3 either.
        when(metadataCache.getNode(eq(1))).thenReturn(brokerRegistration(1, "az1", "127.0.0.1", 9092));
        Optional<Node> leader = proxyNodeMapping.getLeaderNode(1, ClientIdMetadata.of("automq_az=az2"), LISTENER_NAME);
        assertEquals(1, leader.get().id());

        MetadataResponseData.MetadataResponsePartition partition = new MetadataResponseData.MetadataResponsePartition()
            .setPartitionIndex(0).setLeaderId(1);
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
            .setName("TP").setPartitions(List.of(partition));
        proxyNodeMapping.handleMetadataResponse(ClientIdMetadata.of("automq_az=az2"), List.of(topic));
        assertEquals(1, partition.leaderId());
        assertEquals(List.of(1), partition.isrNodes());
        assertEquals(List.of(1), partition.replicaNodes());
    }

    /**
     * Given a withdrawn mapping,
     * when a broker change arrives, as from a metadata update admitted before the interceptor closed,
     * then the mapping stays empty.
     */
    @Test
    public void testWithdrawIgnoresMetadataChange() {
        proxyNodeMapping.onChange(mock(MetadataDelta.class), image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092)));
        assertEquals(Set.of("az1", "az2"), proxyNodeMapping.main2proxyByRack.keySet());

        proxyNodeMapping.withdraw();
        assertTrue(proxyNodeMapping.main2proxyByRack.isEmpty());

        proxyNodeMapping.onChange(brokerChange(4), image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092),
            brokerRegistration(4, "az2", "127.0.0.4", 9092)));
        assertTrue(proxyNodeMapping.main2proxyByRack.isEmpty());
    }

    /**
     * Given a withdrawn mapping,
     * when the exclude zones config changes,
     * then the config listener does not rebuild the mapping.
     */
    @Test
    public void testWithdrawIgnoresExcludeZonesChange() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<Set<String>>> listener = ArgumentCaptor.forClass(Consumer.class);
        verify(zeroZoneConfig).registerListener(listener.capture());
        proxyNodeMapping.onChange(mock(MetadataDelta.class), image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092)));

        listener.getValue().accept(Set.of());
        assertEquals(Set.of("az1", "az2"), proxyNodeMapping.main2proxyByRack.keySet());

        proxyNodeMapping.withdraw();
        listener.getValue().accept(Set.of());
        assertTrue(proxyNodeMapping.main2proxyByRack.isEmpty());
    }

    /**
     * Given a metadata update that is rebuilding the mapping,
     * when the mapping is withdrawn concurrently,
     * then the withdrawal waits for the update and the mapping ends up empty.
     */
    @Test
    public void testWithdrawWaitsForMetadataChange() throws Exception {
        MetadataImage image = image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092));
        ClusterImage cluster = image.cluster();
        CountDownLatch updating = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        // Park the metadata update inside the mapping lock.
        when(image.cluster()).thenAnswer(invocation -> {
            updating.countDown();
            resume.await();
            return cluster;
        });
        MetadataDelta delta = mock(MetadataDelta.class);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> update = executor.submit(() -> proxyNodeMapping.onChange(delta, image));
            assertTrue(updating.await(10, TimeUnit.SECONDS));
            CountDownLatch withdrawing = new CountDownLatch(1);
            AtomicReference<Thread> withdrawThread = new AtomicReference<>();
            Future<?> withdraw = executor.submit(() -> {
                withdrawThread.set(Thread.currentThread());
                withdrawing.countDown();
                proxyNodeMapping.withdraw();
            });
            assertTrue(withdrawing.await(10, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (withdrawThread.get().getState() != Thread.State.BLOCKED) {
                assertTrue(System.nanoTime() < deadline, "withdraw should wait for the mapping update");
                Thread.sleep(1);
            }
            resume.countDown();
            update.get(10, TimeUnit.SECONDS);
            withdraw.get(10, TimeUnit.SECONDS);
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }

        // The update publishes its mapping first, then the withdrawal clears it.
        assertTrue(proxyNodeMapping.main2proxyByRack.isEmpty());
    }

    private static MetadataImage image(BrokerRegistration... brokers) {
        ClusterImage cluster = mock(ClusterImage.class);
        Map<Integer, BrokerRegistration> brokerMap = new HashMap<>();
        for (BrokerRegistration broker : brokers) {
            brokerMap.put(broker.id(), broker);
        }
        when(cluster.brokers()).thenReturn(brokerMap);
        FeaturesImage features = mock(FeaturesImage.class);
        when(features.autoMQVersion()).thenReturn(AutoMQVersion.LATEST);
        MetadataImage image = mock(MetadataImage.class);
        when(image.cluster()).thenReturn(cluster);
        when(image.features()).thenReturn(features);
        return image;
    }

    private static MetadataDelta brokerChange(int nodeId) {
        ClusterDelta clusterDelta = mock(ClusterDelta.class);
        HashMap<Integer, Optional<BrokerRegistration>> changedBrokers = new HashMap<>();
        changedBrokers.put(nodeId, Optional.empty());
        when(clusterDelta.changedBrokers()).thenReturn(changedBrokers);
        MetadataDelta delta = mock(MetadataDelta.class);
        when(delta.clusterDelta()).thenReturn(clusterDelta);
        return delta;
    }

    private static BrokerRegistration brokerRegistration(int id, String rack, String host, int port) {
        return new BrokerRegistration.Builder()
            .setId(id).setRack(Optional.of(rack))
            .setListeners(List.of(new Endpoint(LISTENER_NAME, SecurityProtocol.PLAINTEXT, host, port)))
            .build();
    }

}
