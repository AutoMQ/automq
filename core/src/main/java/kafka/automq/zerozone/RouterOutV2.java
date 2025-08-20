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

import kafka.automq.interceptor.ProduceRequestArgs;

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
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RouterOutV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterOutV2.class);
    private final Node currentNode;
    private final RouterChannel routerChannel;
    private final Map<Node, Proxy> proxies = new ConcurrentHashMap<>();

    private final LocalProxy localProxy;
    private final GetRouterOutNode mapping;
    private final AsyncSender asyncSender;
    private final Time time;
    private final NetworkBandwidthLimiter inboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND);

    public RouterOutV2(Node currentNode, RouterChannel routerChannel, GetRouterOutNode mapping,
        NonBlockingLocalRouterHandler localRouterHandler, AsyncSender asyncSender, Time time) {
        this.currentNode = currentNode;
        this.routerChannel = routerChannel;
        this.mapping = mapping;
        this.localProxy = new LocalProxy(localRouterHandler);
        this.asyncSender = asyncSender;
        this.time = time;
    }

    public void handleProduceAppendProxy(ProduceRequestArgs args) {
        long timeoutMillis = time.milliseconds() + args.timeout();
        short flag = new ZoneRouterProduceRequest.Flag().internalTopicsAllowed(args.internalTopicsAllowed()).value();
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> cfList = new ArrayList<>(args.entriesPerPartition().size());
        long startNanos = time.nanoseconds();
        for (Map.Entry<TopicPartition, MemoryRecords> entry : args.entriesPerPartition().entrySet()) {
            TopicPartition tp = entry.getKey();
            MemoryRecords records = entry.getValue();
            Node node = mapping.getRouteOutNode(tp.topic(), tp.partition(), args.clientId());
            if (node.id() == Node.noNode().id()) {
                responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER));
                continue;
            }
            short orderHint = orderHint(tp, args.clientId().connectionId());
            int recordSize = records.sizeInBytes();
            ZoneRouterProduceRequest zoneRouterProduceRequest = zoneRouterProduceRequest(args, flag, tp, records);
            CompletableFuture<RouterChannel.AppendResult> channelCf = routerChannel.append(node.id(), orderHint, ZoneRouterPackWriter.encodeDataBlock(List.of(zoneRouterProduceRequest)));
            CompletableFuture<Void> proxyCf = channelCf.thenCompose(channelRst -> {
                long timeNanos = time.nanoseconds();
                ZeroZoneMetricsManager.APPEND_CHANNEL_LATENCY.record(timeNanos - startNanos);
                ProxyRequest proxyRequest = new ProxyRequest(tp, channelRst.epoch(), channelRst.channelOffset(), zoneRouterProduceRequest, recordSize, timeoutMillis);
                sendProxyRequest(node, proxyRequest);
                return proxyRequest.cf.thenAccept(response -> {
                    responseMap.put(tp, response);
                    ZeroZoneMetricsManager.PROXY_REQUEST_LATENCY.record(time.nanoseconds() - startNanos);
                });
            });
            cfList.add(proxyCf);
        }
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback = args.responseCallback();
        CompletableFuture<Void> cf = CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0]));
        cf.thenAccept(nil -> responseCallback.accept(responseMap)).exceptionally(ex -> {
            LOGGER.error("[UNEXPECTED],[ROUTE_FAIL]", ex);
            return null;
        });
    }

    private static short orderHint(TopicPartition tp, String connectionId) {
        return (short) (Objects.hash(tp.topic(), tp.partition(), connectionId) % Short.MAX_VALUE);
    }

    private void sendProxyRequest(Node node, ProxyRequest proxyRequest) {
        if (node.id() == currentNode.id()) {
            localProxy.send(proxyRequest);
        } else {
            Proxy proxy = proxies.computeIfAbsent(node, RemoteProxy::new);
            proxy.send(proxyRequest);
        }
    }

    interface Proxy {
        void send(ProxyRequest request);
    }

    static class LocalProxy implements Proxy {
        private final NonBlockingLocalRouterHandler localRouterHandler;

        LocalProxy(NonBlockingLocalRouterHandler handler) {
            localRouterHandler = handler;
        }

        @Override
        public void send(ProxyRequest request) {
            localRouterHandler.append(ChannelOffset.of(request.channelOffset), request.zoneRouterProduceRequest)
                .whenComplete((resp, ex) -> {
                    if (ex != null) {
                        request.completeWithError(Errors.forException(ex));
                    } else {
                        ProduceResponseData produceResponseData = ZoneRouterResponseCodec.decode(Unpooled.wrappedBuffer(resp.data()));
                        resp.setData(null); // gc the data
                        ProduceResponseData.PartitionProduceResponse partitionData = produceResponseData.responses().iterator().next()
                            .partitionResponses().get(0);
                        request.cf.complete(partitionResponse(partitionData));
                    }
                });
        }
    }

    class RemoteProxy implements Proxy {
        private static final int MAX_INFLIGHT_SIZE = 64;
        private final Node node;
        private final Semaphore inflightLimiter = new Semaphore(MAX_INFLIGHT_SIZE);
        private final Queue<RequestBatch> requestBatchQueue = new ConcurrentLinkedQueue<>();
        private RequestBatch requestBatch = null;

        public RemoteProxy(Node node) {
            this.node = node;
        }

        public synchronized void send(ProxyRequest request) {
            ZeroZoneMetricsManager.recordRouterOutBytes(node.id(), request.recordSize);
            synchronized (this) {
                if (requestBatch == null) {
                    requestBatch = new RequestBatch(time, 1, 8192);
                    Threads.COMMON_SCHEDULER.schedule(() -> trySendRequestBatch(requestBatch), 1, TimeUnit.MILLISECONDS);
                }
                if (requestBatch.add(request)) {
                    requestBatchQueue.add(requestBatch);
                    requestBatch = null;
                    trySendRequestBatch(null);
                }
            }
        }

        private synchronized void trySendRequestBatch(RequestBatch forceSend) {
            if (inflightLimiter.availablePermits() == 0) {
                return;
            }
            if ((requestBatch != null && requestBatch.lingerTimeout())
                || (forceSend != null && requestBatch == forceSend)) {
                requestBatchQueue.add(requestBatch);
                requestBatch = null;
            }
            for (; ; ) {
                RequestBatch waitingSend = requestBatchQueue.peek();
                if (waitingSend == null) {
                    break;
                }
                if (!inflightLimiter.tryAcquire()) {
                    break;
                }
                requestBatchQueue.poll();
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                waitingSend.getRequests().forEach((epoch, requests) -> futures.add(batchSend(epoch, requests)));
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((nil, ex) -> {
                        synchronized (RemoteProxy.this) {
                            inflightLimiter.release();
                            trySendRequestBatch(null);
                        }
                    });
            }
        }

        private CompletableFuture<Void> batchSend(long epoch, List<ProxyRequest> requests) {
            RouterRecordV2 routerRecord = new RouterRecordV2(
                currentNode.id(),
                requests.stream().map(r -> r.channelOffset).collect(Collectors.toList())
            );
            AutomqZoneRouterRequest.Builder builder = new AutomqZoneRouterRequest.Builder(
                new AutomqZoneRouterRequestData().setMetadata(routerRecord.encode().array())
                    .setVersion((short) 1)
                    .setRouteEpoch(epoch)
            );
            return asyncSender.sendRequest(node, builder).thenAccept(clientResponse -> {
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

        private void handleRouterResponse(AutomqZoneRouterResponse zoneRouterResponse, List<ProxyRequest> requests) {
            if (zoneRouterResponse.data().errorCode() != Errors.NONE.code()) {
                Errors error = Errors.forCode(zoneRouterResponse.data().errorCode());
                requests.forEach(r -> r.completeWithError(error));
                return;
            }
            List<AutomqZoneRouterResponseData.Response> responses = zoneRouterResponse.data().responses();
            Iterator<ProxyRequest> requestIt = requests.iterator();
            Iterator<AutomqZoneRouterResponseData.Response> responseIt = responses.iterator();
            while (requestIt.hasNext()) {
                ProxyRequest request = requestIt.next();
                AutomqZoneRouterResponseData.Response response = responseIt.next();
                ProduceResponseData produceResponseData = ZoneRouterResponseCodec.decode(Unpooled.wrappedBuffer(response.data()));
                response.setData(null); // gc the data
                ProduceResponseData.PartitionProduceResponse partitionData = produceResponseData.responses().iterator().next()
                    .partitionResponses().get(0);
                request.cf.complete(partitionResponse(partitionData));
            }

        }
    }

    static class RequestBatch {
        private final long batchStartNanos;

        private final Time time;
        private final long lingerNanos;
        private final long batchSize;
        private final Map<Long, List<ProxyRequest>> requests = new TreeMap<>();

        public RequestBatch(Time time, long lingerMs, int batchSize) {
            this.time = time;
            this.lingerNanos = TimeUnit.MILLISECONDS.toNanos(lingerMs);
            this.batchSize = batchSize;
            this.batchStartNanos = System.nanoTime();
        }

        /**
         * Add request to batch
         *
         * @param request {@link ProxyRequest}
         * @return whether the batch is full
         */
        public boolean add(ProxyRequest request) {
            requests.computeIfAbsent(request.epoch, key -> new ArrayList<>()).add(request);
            return requests.size() > batchSize || time.nanoseconds() - batchStartNanos >= lingerNanos;
        }

        public boolean lingerTimeout() {
            return time.nanoseconds() - batchStartNanos >= lingerNanos;
        }

        public Map<Long, List<ProxyRequest>> getRequests() {
            return requests;
        }
    }

    static class ProxyRequest {
        final TopicPartition topicPartition;
        final long epoch;
        final ByteBuf channelOffset;
        final ZoneRouterProduceRequest zoneRouterProduceRequest;
        final int recordSize;
        final long timeoutMillis;
        final CompletableFuture<ProduceResponse.PartitionResponse> cf = new CompletableFuture<>();

        public ProxyRequest(TopicPartition topicPartition, long epoch, ByteBuf channelOffset, ZoneRouterProduceRequest zoneRouterProduceRequest, int recordSize, long timeoutMillis) {
            this.topicPartition = topicPartition;
            this.epoch = epoch;
            this.channelOffset = channelOffset;
            this.zoneRouterProduceRequest = zoneRouterProduceRequest;
            this.recordSize = recordSize;
            this.timeoutMillis = timeoutMillis;
        }

        public void completeWithUnknownError() {
            completeWithError(Errors.UNKNOWN_SERVER_ERROR);
        }

        private void completeWithError(Errors errors) {
            ProduceResponse.PartitionResponse rst = new ProduceResponse.PartitionResponse(errors, -1, -1, -1, -1, Collections.emptyList(), "");
            cf.complete(rst);
        }
    }

    private static ZoneRouterProduceRequest zoneRouterProduceRequest(ProduceRequestArgs args, short flag, TopicPartition tp,
        MemoryRecords records) {
        ProduceRequestData data = new ProduceRequestData();
        data.setTransactionalId(args.transactionId());
        data.setAcks(args.requiredAcks());
        data.setTimeoutMs(args.timeout());
        ProduceRequestData.TopicProduceDataCollection topics = new ProduceRequestData.TopicProduceDataCollection();
        ProduceRequestData.TopicProduceData topic = new ProduceRequestData.TopicProduceData();
        topic.setName(tp.topic());
        topic.setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(tp.partition()).setRecords(records)));
        topics.add(topic);
        data.setTopicData(topics);
        return new ZoneRouterProduceRequest(args.apiVersion(), flag, data);
    }

    private static ProduceResponse.PartitionResponse partitionResponse(
        ProduceResponseData.PartitionProduceResponse partitionData) {
        return new ProduceResponse.PartitionResponse(
            Errors.forCode(partitionData.errorCode()),
            partitionData.baseOffset(),
            0, // last offset , the network layer don't need
            partitionData.logAppendTimeMs(),
            partitionData.logStartOffset(),
            partitionData.recordErrors().stream().map(e -> new ProduceResponse.RecordError(e.batchIndex(), e.batchIndexErrorMessage())).collect(Collectors.toList()),
            partitionData.errorMessage(),
            partitionData.currentLeader()
        );
    }

}
