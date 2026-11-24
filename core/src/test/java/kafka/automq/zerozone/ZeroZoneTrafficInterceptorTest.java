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

import kafka.automq.AutoMQConfig;
import kafka.automq.interceptor.ClientIdMetadata;
import kafka.automq.interceptor.ProduceRequestArgs;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.image.ClusterDelta;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.MemoryObjectStorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class ZeroZoneTrafficInterceptorTest {
    static final String LISTENER_NAME = "BROKER";
    static final ClientIdMetadata AZ1_CLIENT = ClientIdMetadata.of("automq_az=az1");

    MetadataCache metadataCache;
    RouterChannelProvider routerChannelProvider;
    List<MockedConstruction<?>> constructions;
    MockedConstruction<AsyncSender.BrokersAsyncSender> asyncSenders;
    MockedConstruction<SnapshotReadPartitionsManager> snapshotReadManagers;
    MockedConstruction<RouterOutV2> routerOutV2s;
    ZeroZoneTrafficInterceptor interceptor;

    /**
     * The interceptor runs on node 1 in az1. Node 3 in az2 is the main node of the partitions,
     * so node 1 is the proxy for az1 clients.
     */
    @BeforeEach
    public void setup() {
        constructions = new ArrayList<>();
        asyncSenders = track(mockConstruction(AsyncSender.BrokersAsyncSender.class));
        snapshotReadManagers = track(mockConstruction(SnapshotReadPartitionsManager.class));
        routerOutV2s = track(mockConstruction(RouterOutV2.class));
        track(mockConstruction(RouterOut.class));
        track(mockConstruction(RouterIn.class));
        track(mockConstruction(RouterInV2.class));
        // No storage behavior is under test; this avoids the executors and metrics of a real object storage.
        track(mockConstruction(MemoryObjectStorage.class));

        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        when(kafkaConfig.rack()).thenReturn(Option.apply("az1"));
        when(kafkaConfig.nodeId()).thenReturn(1);
        ListenerName listenerName = ListenerName.normalised(LISTENER_NAME);
        when(kafkaConfig.interBrokerListenerName()).thenReturn(listenerName);
        when(kafkaConfig.effectiveAdvertisedBrokerListeners()).thenReturn(CollectionConverters.asScala(
            List.of(new EndPoint("127.0.0.1", 9092, listenerName, SecurityProtocol.PLAINTEXT))).toSeq());
        AutoMQConfig autoMQConfig = mock(AutoMQConfig.class);
        when(autoMQConfig.zoneRouterChannels()).thenReturn(Optional.of(List.of(BucketURI.parse("0@mem://zonerouter"))));
        when(autoMQConfig.zoneRouterLocalWriteMode()).thenReturn(LocalWriteMode.DIRECT);
        when(kafkaConfig.automq()).thenReturn(autoMQConfig);

        metadataCache = mock(MetadataCache.class);
        when(metadataCache.autoMQVersion()).thenReturn(AutoMQVersion.LATEST);
        when(metadataCache.getNode(3)).thenReturn(brokerRegistration(3, "az2", "127.0.0.3", 9092));

        ZeroZoneConfig zeroZoneConfig = mock(ZeroZoneConfig.class);
        when(zeroZoneConfig.excludeZones()).thenReturn(Set.of());

        routerChannelProvider = mock(RouterChannelProvider.class);
        when(routerChannelProvider.channel()).thenReturn(mock(RouterChannel.class));

        interceptor = new ZeroZoneTrafficInterceptor(routerChannelProvider, mock(ConfirmWALProvider.class),
            mock(ElasticKafkaApis.class), metadataCache, zeroZoneConfig, kafkaConfig);
        interceptor.onMetadataUpdate(mock(MetadataDelta.class), image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092)),
            mock(LoaderManifest.class));
    }

    @AfterEach
    public void teardown() {
        List<Executable> cleanup = new ArrayList<>();
        if (interceptor != null) {
            cleanup.add(interceptor::close);
        }
        if (constructions != null) {
            for (int i = constructions.size() - 1; i >= 0; i--) {
                cleanup.add(constructions.get(i)::close);
            }
        }
        assertAll("close interceptor and construction scopes", cleanup);
    }

    /**
     * Given an interceptor that proxies for the clients in its zone,
     * when it closes,
     * then leader hints and metadata responses return the main node, and produce requests are rejected.
     */
    @Test
    public void testCloseWithdrawsProxyRouting() {
        assertEquals(1, interceptor.getLeaderNode(3, AZ1_CLIENT, LISTENER_NAME).map(Node::id).orElse(-1));
        assertEquals(1, metadataLeader());

        interceptor.close();

        assertEquals(3, interceptor.getLeaderNode(3, AZ1_CLIENT, LISTENER_NAME).map(Node::id).orElse(-1));
        assertEquals(3, metadataLeader());

        TopicPartition tp = new TopicPartition("TP", 0);
        ProduceRequestArgs args = mock(ProduceRequestArgs.class);
        when(args.entriesPerPartition()).thenReturn(Map.of(tp, MemoryRecords.EMPTY));
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> response = new AtomicReference<>();
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> callback = response::set;
        when(args.responseCallback()).thenReturn(callback);
        interceptor.handleProduceRequest(args);
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.get().get(tp).error);
        verify(routerOutV2s.constructed().get(0), never()).handleProduceAppendProxy(any());
    }

    /**
     * Given an interceptor that proxies a remote leader,
     * when it is closed twice sequentially,
     * then both calls complete, the snapshot manager, V2 router and async sender each close once,
     * and the hint remains at the real leader.
     */
    @Test
    public void testCloseIsIdempotent() {
        interceptor.close();
        assertDoesNotThrow(() -> interceptor.close());

        verify(snapshotReadManagers.constructed().get(0), times(1)).close();
        verify(routerOutV2s.constructed().get(0), times(1)).close();
        verify(asyncSenders.constructed().get(0), times(1)).close();
        assertEquals(3, interceptor.getLeaderNode(3, AZ1_CLIENT, LISTENER_NAME).map(Node::id).orElse(-1));
    }

    /**
     * Given a closed interceptor,
     * when a metadata update with a broker change arrives,
     * then the proxy routing is not rebuilt and no further calls reach the snapshot manager or channel provider.
     */
    @Test
    public void testMetadataUpdateAfterCloseIsIgnored() throws Exception {
        interceptor.close();
        SnapshotReadPartitionsManager snapshotReadManager = snapshotReadManagers.constructed().get(0);
        clearInvocations(snapshotReadManager, routerChannelProvider);

        interceptor.onMetadataUpdate(brokerChange(4), image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092),
            brokerRegistration(4, "az2", "127.0.0.4", 9092)), mock(LoaderManifest.class));

        assertEquals(3, interceptor.getLeaderNode(3, AZ1_CLIENT, LISTENER_NAME).map(Node::id).orElse(-1));
        assertEquals(3, metadataLeader());
        verifyNoInteractions(snapshotReadManager, routerChannelProvider);
    }

    /**
     * Given a metadata update that passed the closed check and is rebuilding the mapping,
     * when the interceptor closes concurrently,
     * then close waits for that update and the proxy routing still ends up withdrawn.
     */
    @Test
    public void testCloseWaitsForAdmittedMetadataUpdate() throws Exception {
        MetadataImage image = image(
            brokerRegistration(1, "az1", "127.0.0.1", 9092), brokerRegistration(3, "az2", "127.0.0.3", 9092),
            brokerRegistration(4, "az2", "127.0.0.4", 9092));
        ClusterImage cluster = image.cluster();
        CountDownLatch updating = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        // Park the admitted metadata update while it rebuilds the mapping.
        when(image.cluster()).thenAnswer(invocation -> {
            updating.countDown();
            resume.await();
            return cluster;
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> update = executor.submit(() -> interceptor.onMetadataUpdate(brokerChange(4), image, mock(LoaderManifest.class)));
            assertTrue(updating.await(10, TimeUnit.SECONDS));
            CountDownLatch closing = new CountDownLatch(1);
            AtomicReference<Thread> closeThread = new AtomicReference<>();
            Future<?> close = executor.submit(() -> {
                closeThread.set(Thread.currentThread());
                closing.countDown();
                interceptor.close();
            });
            assertTrue(closing.await(10, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (closeThread.get().getState() != Thread.State.BLOCKED) {
                assertTrue(System.nanoTime() < deadline, "close should wait for the mapping update");
                Thread.sleep(1);
            }
            resume.countDown();
            update.get(10, TimeUnit.SECONDS);
            close.get(10, TimeUnit.SECONDS);
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "metadata/close workers did not terminate");
        }

        assertEquals(3, interceptor.getLeaderNode(3, AZ1_CLIENT, LISTENER_NAME).map(Node::id).orElse(-1));
        assertEquals(3, metadataLeader());
    }

    private int metadataLeader() {
        MetadataResponseData.MetadataResponsePartition partition = new MetadataResponseData.MetadataResponsePartition()
            .setPartitionIndex(0).setLeaderId(3).setIsrNodes(List.of(3)).setReplicaNodes(List.of(3));
        MetadataResponseData.MetadataResponseTopic topic = new MetadataResponseData.MetadataResponseTopic()
            .setName("TP").setPartitions(List.of(partition));
        interceptor.handleMetadataResponse(ClientIdMetadata.of("automq_az=az1"), List.of(topic));
        return partition.leaderId();
    }

    private <T> MockedConstruction<T> track(MockedConstruction<T> construction) {
        constructions.add(construction);
        return construction;
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
