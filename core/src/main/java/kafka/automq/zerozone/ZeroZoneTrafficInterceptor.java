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

import kafka.automq.interceptor.ClientIdKey;
import kafka.automq.interceptor.ClientIdMetadata;
import kafka.automq.interceptor.ProduceRequestArgs;
import kafka.automq.interceptor.TrafficInterceptor;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticKafkaApis;
import kafka.server.streamaspect.ElasticReplicaManager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ZeroZoneTrafficInterceptor implements TrafficInterceptor, MetadataPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroZoneTrafficInterceptor.class);
    private final ElasticKafkaApis kafkaApis;
    private final ClientRackProvider clientRackProvider;
    private final List<BucketURI> config;
    private final BucketURI bucketURI;

    private final ProxyNodeMapping mapping;

    private final RouterOut routerOut;
    private final RouterIn routerIn;

    private final RouterChannelProvider routerChannelProvider;
    private final RouterOutV2 routerOutV2;
    private final RouterInV2 routerInV2;
    private final CommittedEpochManager committedEpochManager;

    private final SnapshotReadPartitionsManager snapshotReadPartitionsManager;
    private volatile AutoMQVersion version;
    private volatile boolean closed = false;

    public ZeroZoneTrafficInterceptor(
        RouterChannelProvider routerChannelProvider,
        ConfirmWALProvider confirmWALProvider,
        ElasticKafkaApis kafkaApis,
        MetadataCache metadataCache,
        ClientRackProvider clientRackProvider,
        KafkaConfig kafkaConfig) {
        this.routerChannelProvider = routerChannelProvider;
        this.kafkaApis = kafkaApis;

        if (kafkaConfig.rack().isEmpty()) {
            throw new IllegalArgumentException("The node rack should be set when enable cross available zone router");
        }


        String interBrokerListenerName = kafkaConfig.interBrokerListenerName().value();
        int nodeId = kafkaConfig.nodeId();
        Node currentNode = kafkaConfig.effectiveAdvertisedBrokerListeners()
            .find(endpoint -> Objects.equals(interBrokerListenerName, endpoint.listenerName().value()))
            .map(endpoint -> new Node(nodeId, endpoint.host(), endpoint.port()))
            .get();

        this.mapping = new ProxyNodeMapping(currentNode, kafkaConfig.rack().get(), interBrokerListenerName, metadataCache);

        Time time = Time.SYSTEM;

        AsyncSender asyncSender = new AsyncSender.BrokersAsyncSender(kafkaConfig, kafkaApis.metrics(), "zone_router", time, ZoneRouterPack.ZONE_ROUTER_CLIENT_ID, new LogContext());

        this.config = kafkaConfig.automq().zoneRouterChannels().get();

        //noinspection OptionalGetWithoutIsPresent
        this.bucketURI = kafkaConfig.automq().zoneRouterChannels().get().get(0);
        this.clientRackProvider = clientRackProvider;
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(bucketURI)
            .readWriteIsolate(true)
            .inboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND))
            .outboundLimiter(GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.OUTBOUND))
            .build();
        this.routerOut = new RouterOut(currentNode, bucketURI, objectStorage, mapping::getRouteOutNode, kafkaApis, asyncSender, time);
        this.routerIn = new RouterIn(objectStorage, kafkaApis, kafkaConfig.rack().get());

        // Zero Zone V2
        this.routerInV2 = new RouterInV2(routerChannelProvider, kafkaApis, kafkaConfig.rack().get(), time);
        this.routerOutV2 = new RouterOutV2(currentNode, routerChannelProvider.channel(), mapping::getRouteOutNode, routerInV2, asyncSender, time);
        this.committedEpochManager = new CommittedEpochManager(nodeId);
        this.routerChannelProvider.addEpochListener(committedEpochManager);
        DefaultReplayer replayer = new DefaultReplayer();

        this.version = metadataCache.autoMQVersion();

        this.snapshotReadPartitionsManager = new SnapshotReadPartitionsManager(kafkaConfig, kafkaApis.metrics(), time, confirmWALProvider,
            (ElasticReplicaManager) kafkaApis.replicaManager(), kafkaApis.metadataCache(), replayer);
        this.snapshotReadPartitionsManager.setVersion(version);
        replayer.setCacheEventListener(this.snapshotReadPartitionsManager.cacheEventListener());
        mapping.registerListener(snapshotReadPartitionsManager);


        LOGGER.info("start zero zone traffic interceptor with config={}", bucketURI);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void handleProduceRequest(ProduceRequestArgs args) {
        ClientIdMetadata clientId = args.clientId();
        fillRackIfMissing(clientId);
        if (version.isZeroZoneV2Supported()) {
            routerOutV2.handleProduceAppendProxy(args);
        } else {
            if (clientId.rack() != null) {
                routerOut.handleProduceAppendProxy(args);
            } else {
                MismatchRecorder.instance().record(args.entriesPerPartition().entrySet().iterator().next().getKey().topic(), clientId);
                // If the client rack isn't set, then try to handle the request in the current node.
                kafkaApis.handleProduceAppendJavaCompatible(args);
            }
        }
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(AutomqZoneRouterRequestData request) {
        if (request.version() == 0) {
            return routerIn.handleZoneRouterRequest(request.metadata());
        } else {
            ReentrantReadWriteLock.ReadLock readLock = committedEpochManager.readLock();
            readLock.lock();
            AtomicLong inflight = committedEpochManager.epochInflight(request.routeEpoch());
            inflight.incrementAndGet();
            try {
                return routerInV2.handleZoneRouterRequest(request)
                    .whenComplete((resp, ex) -> inflight.decrementAndGet());
            } finally {
                readLock.unlock();
            }
        }
    }

    @Override
    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(ClientIdMetadata clientId,
        List<MetadataResponseData.MetadataResponseTopic> topics) {
        fillRackIfMissing(clientId);
        return mapping.handleMetadataResponse(clientId, topics);
    }

    @Override
    public Optional<Node> getLeaderNode(int leaderId, ClientIdMetadata clientId,
        String listenerName) {
        fillRackIfMissing(clientId);
        return mapping.getLeaderNode(leaderId, clientId, listenerName);
    }

    @Override
    public String name() {
        return "ObjectCrossZoneProduceRouter";
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (closed) {
            return;
        }
        try {
            mapping.onChange(delta, newImage);
            snapshotReadPartitionsManager.onChange(delta, newImage);
            version = newImage.features().autoMQVersion();
            this.snapshotReadPartitionsManager.setVersion(version);
            routerChannelProvider.onChange(delta, newImage);
        } catch (Throwable e) {
            LOGGER.error("Failed to handle metadata update", e);
        }
    }

    public void setRouterInProduceHandler(RouterInProduceHandler routerInProduceHandler) {
        routerIn.setRouterInProduceHandler(routerInProduceHandler);
        routerInV2.setRouterInProduceHandler(routerInProduceHandler);
    }

    private AutoMQVersion version() {
        return version;
    }

    @Override
    public String toString() {
        return "ZeroZoneTrafficInterceptor{config=" + config + '}';
    }

    private void fillRackIfMissing(ClientIdMetadata clientId) {
        if (clientId.rack() == null) {
            String rack = clientRackProvider.rack(clientId);
            if (rack != null) {
                clientId.metadata(ClientIdKey.AVAILABILITY_ZONE, List.of(rack));
            }
        }
    }

    static Map<TopicPartition, MemoryRecords> produceRequestToMap(ProduceRequestData data) {
        Map<TopicPartition, MemoryRecords> realEntriesPerPartition = new HashMap<>();
        data.topicData().forEach(topicData ->
            topicData.partitionData().forEach(partitionData ->
                realEntriesPerPartition.put(
                    new TopicPartition(topicData.name(), partitionData.index()),
                    (MemoryRecords) partitionData.records()
                )));
        return realEntriesPerPartition;
    }

}
