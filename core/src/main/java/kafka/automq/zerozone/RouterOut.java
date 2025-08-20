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
import kafka.automq.interceptor.ProduceRequestArgs;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterRequest;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.netty.buffer.Unpooled;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

public class RouterOut {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterOut.class);
    private static final String BATCH_INTERVAL_CONFIG = "batchInterval";
    private static final String MAX_BYTES_IN_BATCH_CONFIG = "maxBytesInBatch";

    private final int batchIntervalMs;
    private final int batchSizeThreshold;
    private final Node currentNode;

    private final Map<Node, BlockingQueue<ProxyRequest>> pendingRequests = new ConcurrentHashMap<>();
    private CompletableFuture<Void> lastRouterCf = CompletableFuture.completedFuture(null);
    private final AtomicInteger batchSize = new AtomicInteger();
    private final AtomicLong nextObjectId = new AtomicLong();
    private long lastUploadTimestamp = 0;

    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("object-cross-zone-produce-router-out", true, LOGGER);
    private final HashedWheelTimer timeoutDetect = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("object-cross-zone-produce-router-out-timeout-detect", true),
        1, TimeUnit.SECONDS, 100
    );

    private final AsyncSender asyncSender;
    private final BucketURI bucketURI;
    private final ObjectStorage objectStorage;
    private final GetRouterOutNode mapping;
    private final ElasticKafkaApis kafkaApis;
    private final Time time;
    private final NetworkBandwidthLimiter inboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND);

    private final PerfMode perfMode = new PerfMode();

    public RouterOut(Node currentNode, BucketURI bucketURI, ObjectStorage objectStorage,
        GetRouterOutNode mapping, ElasticKafkaApis kafkaApis, AsyncSender asyncSender, Time time) {
        this.currentNode = currentNode;
        this.bucketURI = bucketURI;
        this.batchIntervalMs = Integer.parseInt(bucketURI.extensionString(BATCH_INTERVAL_CONFIG, "250"));
        this.batchSizeThreshold = Integer.parseInt(bucketURI.extensionString(MAX_BYTES_IN_BATCH_CONFIG, "8388608"));
        this.objectStorage = objectStorage;
        this.mapping = mapping;
        this.kafkaApis = kafkaApis;
        this.asyncSender = asyncSender;
        this.time = time;
        cleanup();
        scheduler.scheduleWithFixedDelay(this::proxy, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void handleProduceAppendProxy(ProduceRequestArgs args) {
        short flag = new ZoneRouterProduceRequest.Flag().internalTopicsAllowed(args.internalTopicsAllowed()).value();
        Map<Node, ProxyRequest> requests = split(args.apiVersion(), args.clientId(), args.timeout(), flag, args.requiredAcks(), args.transactionId(), args.entriesPerPartition());

        boolean forceRoute = perfMode.isRouteInPerfMode(args.entriesPerPartition());
        requests.forEach((node, request) -> {
            if (node.id() == Node.noNode().id()) {
                request.completeWithNotLeaderNotFollower();
                return;
            }
            if (node.id() == currentNode.id() && !forceRoute) {
                kafkaApis.handleProduceAppendJavaCompatible(
                    args.toBuilder()
                        .entriesPerPartition(ZeroZoneTrafficInterceptor.produceRequestToMap(request.data))
                        .responseCallback(responseCallbackRst -> {
                            request.cf.complete(responseCallbackRst);
                        })
                        .build()
                );
            } else {
                ZeroZoneMetricsManager.recordRouterOutBytes(node.id(), request.size);
                pendingRequests.compute(node, (n, queue) -> {
                    if (queue == null) {
                        queue = new LinkedBlockingQueue<>();
                    }
                    queue.add(request);
                    batchSize.addAndGet(request.size);
                    return queue;
                });
            }
        });

        Timeout timeout = timeoutDetect.newTimeout(t -> LOGGER.error("[POTENTIAL_BUG] router out timeout, {}", t), 1, TimeUnit.MINUTES);
        // Group the request result
        Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> proxyRequestCfList = requests.values().stream().map(
            request -> request.cf
                .thenAccept(rst::putAll)
                .exceptionally(ex -> {
                    LOGGER.error("[UNEXPECTED],request={}", request.topicPartitions, ex);
                    request.topicPartitions.forEach(topicPartition ->
                        rst.put(topicPartition, new ProduceResponse.PartitionResponse(Errors.UNKNOWN_SERVER_ERROR)));
                    return null;
                })
        ).collect(Collectors.toList());
        CompletableFuture.allOf(proxyRequestCfList.toArray(CompletableFuture[]::new))
            .thenAccept(nil -> args.responseCallback().accept(rst))
            .whenComplete((nil, ex) -> {
                if (timeout.isExpired()) {
                    LOGGER.error("[POTENTIAL_BUG_RECOVERED] router out timeout recover, {}", timeout);
                } else {
                    timeout.cancel();
                }
            });

        if (batchSize.get() >= batchSizeThreshold || time.milliseconds() - batchIntervalMs >= lastUploadTimestamp) {
            scheduler.submit(this::proxy);
        }
    }

    private void proxy() {
        try {
            proxy0();
        } catch (Throwable e) {
            LOGGER.error("[UNEXPECTED],[BUG],proxy failed", e);
        }
    }

    private void proxy0() {
        if (batchSize.get() < batchSizeThreshold && time.milliseconds() - batchIntervalMs < lastUploadTimestamp) {
            return;
        }

        lastUploadTimestamp = time.milliseconds();

        // 1. Batch the request by destination node.
        Map<Node, List<ProxyRequest>> node2requests = new HashMap<>();
        pendingRequests.forEach((node, queue) -> {
            List<ProxyRequest> requests = new ArrayList<>();
            queue.drainTo(requests);
            if (!requests.isEmpty()) {
                node2requests.put(node, requests);
                batchSize.addAndGet(-requests.stream().mapToInt(r -> r.size).sum());
            }
        });
        if (node2requests.isEmpty()) {
            return;
        }

        // 2. Write the request to object and get the relative position.
        long objectId = nextObjectId.incrementAndGet();
        ZoneRouterPackWriter writer = new ZoneRouterPackWriter(currentNode.id(), objectId, objectStorage);
        Map<Node, Position> node2position = new HashMap<>();
        node2requests.forEach((node, requests) -> {
            Position position = writer
                .addProduceRequests(
                    requests
                        .stream()
                        .map(r -> new ZoneRouterProduceRequest(r.apiVersion, r.flag, r.data))
                        .collect(Collectors.toList())
                );
            requests.forEach(ProxyRequest::afterRouter);
            node2position.put(node, position);
        });

        // 3. send ZoneRouterRequest to the route out nodes.
        CompletableFuture<Void> writeCf = writer.close();
        CompletableFuture<Void> prevLastRouterCf = lastRouterCf;
        lastRouterCf = writeCf
            // Orderly send the router request.
            .thenCompose(nil -> prevLastRouterCf)
            .thenAccept(nil -> {
                // TODO: in / out metrics and type (要经过限流器 + 另外一个 type 类型统计)
                List<CompletableFuture<Void>> sendCfList = new ArrayList<>();
                node2position.forEach((destNode, position) -> {
                    List<ProxyRequest> proxyRequests = node2requests.get(destNode);
                    CompletableFuture<Void> sendCf = sendRouterRequest(destNode, objectId, position, proxyRequests);
                    sendCfList.add(sendCf);
                });
                CompletableFuture.allOf(sendCfList.toArray(new CompletableFuture[0])).whenComplete((nil2, ex) -> {
                    ObjectStorage.ObjectPath path = writer.objectPath();
                    objectStorage.delete(List.of(path)).exceptionally(ex2 -> {
                        LOGGER.error("delete {} fail", path, ex);
                        return null;
                    });
                });
            })
            .exceptionally(ex -> {
                LOGGER.error("[UNEXPECTED],[PROXY]", ex);
                return null;
            });
    }

    private CompletableFuture<Void> sendRouterRequest(Node destNode, long objectId, Position position,
        List<ProxyRequest> requests) {
        RouterRecord routerRecord = new RouterRecord(currentNode.id(), objectStorage.bucketId(), objectId, position.position(), position.size());

        AutomqZoneRouterRequest.Builder builder = new AutomqZoneRouterRequest.Builder(
            new AutomqZoneRouterRequestData().setMetadata(routerRecord.encode().array())
        );
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ROUTER_OUT],node={},objectId={},position={},partitions={}", destNode, objectId, position, requests.stream().map(r -> r.topicPartitions).collect(Collectors.toList()));
        }
        return asyncSender.sendRequest(destNode, builder).thenAccept(clientResponse -> {
            if (!clientResponse.hasResponse()) {
                LOGGER.error("[ROUTER_OUT],[NO_RESPONSE],response={}", clientResponse);
                requests.forEach(ProxyRequest::completeWithUnknownError);
                return;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[ROUTER_OUT],[RESPONSE],response={}", clientResponse);
            }
            AutomqZoneRouterResponse zoneRouterResponse = (AutomqZoneRouterResponse) clientResponse.responseBody();
            handleRouterResponse(zoneRouterResponse, requests);
        }).exceptionally(ex -> {
            LOGGER.error("[ROUTER_OUT],[REQUEST_FAIL]", ex);
            requests.forEach(ProxyRequest::completeWithUnknownError);
            return null;
        });
    }

    private void handleRouterResponse(AutomqZoneRouterResponse zoneRouterResponse,
        List<ProxyRequest> proxyRequests) {
        List<AutomqZoneRouterResponseData.Response> responses = zoneRouterResponse.data().responses();
        for (int i = 0; i < proxyRequests.size(); i++) {
            ProxyRequest proxyRequest = proxyRequests.get(i);
            AutomqZoneRouterResponseData.Response response = responses.get(i);
            ProduceResponseData produceResponseData = ZoneRouterResponseCodec.decode(Unpooled.wrappedBuffer(response.data()));
            response.setData(null); // gc the data
            Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new HashMap<>();
            produceResponseData.responses().forEach(topicData -> {
                topicData.partitionResponses().forEach(partitionData -> {
                    ProduceResponse.PartitionResponse partitionResponse = new ProduceResponse.PartitionResponse(
                        Errors.forCode(partitionData.errorCode()),
                        partitionData.baseOffset(),
                        0, // last offset , the network layer don't need
                        partitionData.logAppendTimeMs(),
                        partitionData.logStartOffset(),
                        partitionData.recordErrors().stream().map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage())).collect(Collectors.toList()),
                        partitionData.errorMessage(),
                        partitionData.currentLeader()
                    );
                    rst.put(new TopicPartition(topicData.name(), partitionData.index()), partitionResponse);
                });
            });
            proxyRequest.cf.complete(rst);
        }
    }

    /**
     * Split the produce request to different nodes
     */
    private Map<Node, ProxyRequest> split(
        short apiVersion,
        ClientIdMetadata clientId,
        int timeout,
        short requiredAcks,
        short flag,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        Map<Node, List<Map.Entry<TopicPartition, MemoryRecords>>> node2Entries = new HashMap<>();
        entriesPerPartition.forEach((tp, records) -> {
            Node node = mapping.getRouteOutNode(tp.topic(), tp.partition(), clientId);
            node2Entries.compute(node, (n, list) -> {
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(Map.entry(tp, records));
                return list;
            });
        });
        Map<Node, ProxyRequest> rst = new HashMap<>();
        node2Entries.forEach((node, entries) -> {
            AtomicInteger size = new AtomicInteger();
            ProduceRequestData data = new ProduceRequestData();
            data.setTransactionalId(transactionId);
            data.setAcks(requiredAcks);
            data.setTimeoutMs(timeout);

            Map<String, Map<Integer, MemoryRecords>> topicData = new HashMap<>();
            entries.forEach(e -> {
                TopicPartition tp = e.getKey();
                MemoryRecords records = e.getValue();
                topicData.compute(tp.topic(), (topicName, map) -> {
                    if (map == null) {
                        map = new HashMap<>();
                    }
                    map.put(tp.partition(), records);
                    size.addAndGet(records.sizeInBytes());
                    return map;
                });
            });
            ProduceRequestData.TopicProduceDataCollection list = new ProduceRequestData.TopicProduceDataCollection();
            topicData.forEach((topicName, partitionData) -> {
                list.add(
                    new ProduceRequestData.TopicProduceData()
                        .setName(topicName)
                        .setPartitionData(
                            partitionData.entrySet()
                                .stream()
                                .map(e -> new ProduceRequestData.PartitionProduceData().setIndex(e.getKey()).setRecords(e.getValue()))
                                .collect(Collectors.toList())
                        )
                );
            });
            data.setTopicData(list);
            rst.put(node, new ProxyRequest(apiVersion, flag, data, size.get()));
        });
        return rst;
    }

    private void cleanup() {
        try {
            List<ObjectStorage.ObjectInfo> objects = objectStorage.list(ZoneRouterPack.getObjectPathPrefix(currentNode.id())).get();
            objectStorage.delete(objects.stream().map(o -> (ObjectStorage.ObjectPath) o).collect(Collectors.toList())).get();
        } catch (Throwable e) {
            LOGGER.error("cleanup fail", e);
        }
    }

    static class ProxyRequest {
        short apiVersion;
        short flag;
        ProduceRequestData data;
        int size;
        List<TopicPartition> topicPartitions;
        CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> cf;

        public ProxyRequest(short apiVersion, short flag, ProduceRequestData data, int size) {
            this.apiVersion = apiVersion;
            this.flag = flag;
            this.data = data;
            this.size = size;
            this.cf = new CompletableFuture<>();
            this.topicPartitions = new ArrayList<>();
            this.data.topicData().forEach(topicData -> topicData.partitionData().forEach(partitionData -> {
                topicPartitions.add(new TopicPartition(topicData.name(), partitionData.index()));
            }));
        }

        public void afterRouter() {
            data = null; // gc the data
        }

        public void completeWithNotLeaderNotFollower() {
            completeWithError(Errors.NOT_LEADER_OR_FOLLOWER);
        }

        public void completeWithUnknownError() {
            completeWithError(Errors.UNKNOWN_SERVER_ERROR);
        }

        private void completeWithError(Errors errors) {
            Map<TopicPartition, ProduceResponse.PartitionResponse> rst = new HashMap<>();
            topicPartitions.forEach(tp -> rst.put(tp, new ProduceResponse.PartitionResponse(errors, -1, -1, -1, -1, Collections.emptyList(), "")));
            cf.complete(rst);
        }
    }

    static class PerfMode {
        private final boolean enable = Systems.getEnvBool("AUTOMQ_PERF_MODE", false); // force route 2 / 3 traffic
        private final int availableZoneCount = Systems.getEnvInt("AUTOMQ_PERF_MODE_AZ_COUNT", 3);
        private final Map<Long, Boolean> routerMap = new ConcurrentHashMap<>();
        private final AtomicInteger routerIndex = new AtomicInteger();

        public boolean isRouteInPerfMode(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
            if (!enable) {
                return false;
            }
            long producerId = entriesPerPartition.entrySet().iterator().next().getValue().batches().iterator().next().producerId();
            return routerMap.computeIfAbsent(producerId, k -> {
                int index = routerIndex.incrementAndGet();
                return index % availableZoneCount != 0;
            });
        }
    }

}
