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
import kafka.server.KafkaRequestHandler;
import kafka.server.RequestLocal;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

public class RouterInV2 implements NonBlockingLocalRouterHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterInV2.class);

    static {
        // RouterIn will parallel append the records from one AutomqZoneRouterRequest.
        // So the append thread isn't the KafkaRequestHandler
        KafkaRequestHandler.setBypassThreadCheck(true);
    }

    private final RouterChannelProvider channelProvider;
    private final ElasticKafkaApis kafkaApis;
    private final String rack;
    private final RouterInProduceHandler localAppendHandler;
    private RouterInProduceHandler routerInProduceHandler;
    private final BlockingQueue<PartitionProduceRequest> unpackLinkQueue = new ArrayBlockingQueue<>(Systems.CPU_CORES * 8192);
    private final EventLoop[] appendEventLoops;
    private final FastThreadLocal<RequestLocal> requestLocals = new FastThreadLocal<>() {
        @Override
        protected RequestLocal initialValue() {
            // The same as KafkaRequestHandler.requestLocal
            return RequestLocal.withThreadConfinedCaching();
        }
    };
    private final Time time;

    public RouterInV2(RouterChannelProvider channelProvider, ElasticKafkaApis kafkaApis, String rack, Time time) {
        this.channelProvider = channelProvider;
        this.kafkaApis = kafkaApis;
        this.rack = rack;
        this.localAppendHandler = kafkaApis::handleProduceAppendJavaCompatible;
        this.routerInProduceHandler = this.localAppendHandler;
        this.time = time;

        this.appendEventLoops = new EventLoop[Systems.CPU_CORES];
        for (int i = 0; i < appendEventLoops.length; i++) {
            this.appendEventLoops[i] = new EventLoop("ROUTER_IN_V2_APPEND_" + i);
        }

    }

    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(AutomqZoneRouterRequestData request) {
        long requestEpoch = request.routeEpoch();
        if (requestEpoch <= channelProvider.epoch().getFenced()) {
            String str = String.format("The router request epoch %s is less than fenced epoch %s.", requestEpoch, channelProvider.epoch().getFenced());
            return CompletableFuture.failedFuture(new IllegalStateException(str));
        }
        return handleZoneRouterRequest0(request);
    }

    private CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest0(AutomqZoneRouterRequestData request) {
        RouterRecordV2 routerRecord = RouterRecordV2.decode(Unpooled.wrappedBuffer(request.metadata()));
        RouterChannel routerChannel = channelProvider.readOnlyChannel(routerRecord.nodeId());
        List<CompletableFuture<AutomqZoneRouterResponseData.Response>> subResponseList = new ArrayList<>(routerRecord.channelOffsets().size());
        AtomicInteger size = new AtomicInteger(0);
        long startNanos = time.nanoseconds();
        for (ByteBuf channelOffset : routerRecord.channelOffsets()) {
            PartitionProduceRequest partitionProduceRequest = new PartitionProduceRequest(ChannelOffset.of(channelOffset));
            partitionProduceRequest.unpackLinkCf = routerChannel.get(channelOffset);
            addToUnpackLinkQueue(partitionProduceRequest);
            partitionProduceRequest.unpackLinkCf.whenComplete((rst, ex) -> {
                if (ex == null) {
                    size.addAndGet(rst.readableBytes());
                }
                handleUnpackLink();
                ZeroZoneMetricsManager.GET_CHANNEL_LATENCY.record(time.nanoseconds() - startNanos);
            });
            subResponseList.add(partitionProduceRequest.responseCf);
        }
        return CompletableFuture.allOf(subResponseList.toArray(new CompletableFuture[0])).thenApply(nil -> {
            AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData();
            response.setResponses(subResponseList.stream().map(CompletableFuture::join).collect(Collectors.toList()));
            ZeroZoneMetricsManager.recordRouterInBytes(routerRecord.nodeId(), size.get());
            return new AutomqZoneRouterResponse(response);
        });
    }

    private void handleUnpackLink() {
        if (unpackLinkQueue.isEmpty()) {
            return;
        }
        synchronized (unpackLinkQueue) {
            while (!unpackLinkQueue.isEmpty()) {
                PartitionProduceRequest req = unpackLinkQueue.peek();
                if (req.unpackLinkCf.isDone()) {
                    EventLoop eventLoop = appendEventLoops[Math.abs(req.channelOffset.orderHint() % appendEventLoops.length)];
                    req.unpackLinkCf.thenComposeAsync(buf -> {
                        ZoneRouterProduceRequest zoneRouterProduceRequest = ZoneRouterPackReader.decodeDataBlock(buf).get(0);
                        try {
                            return append0(req.channelOffset, zoneRouterProduceRequest, false);
                        } finally {
                            buf.release();
                        }
                    }, eventLoop).whenComplete((resp, ex) -> {
                        if (ex != null) {
                            LOGGER.error("[ROUTER_IN],[FAILED]", ex);
                            req.responseCf.completeExceptionally(ex);
                            return;
                        }
                        req.responseCf.complete(resp);
                    });
                    unpackLinkQueue.poll();
                } else {
                    break;
                }
            }
        }
    }

    private void addToUnpackLinkQueue(PartitionProduceRequest req) {
        for (;;) {
            try {
                unpackLinkQueue.put(req);
                return;
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponseData.Response> append(
        ChannelOffset channelOffset,
        ZoneRouterProduceRequest zoneRouterProduceRequest
    ) {
        CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
        appendEventLoops[Math.abs(channelOffset.orderHint() % appendEventLoops.length)].execute(() ->
            FutureUtil.propagate(append0(channelOffset, zoneRouterProduceRequest, true), cf));
        return cf;
    }

    private CompletableFuture<AutomqZoneRouterResponseData.Response> append0(
        ChannelOffset channelOffset,
        ZoneRouterProduceRequest zoneRouterProduceRequest,
        boolean local
    ) {
        ZoneRouterProduceRequest.Flag flag = new ZoneRouterProduceRequest.Flag(zoneRouterProduceRequest.flag());
        ProduceRequestData data = zoneRouterProduceRequest.data();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ROUTER_IN],[APPEND],data={}", data);
        }

        ZeroZoneThreadLocalContext.writeContext().channelOffset = channelOffset;
        Map<TopicPartition, MemoryRecords> realEntriesPerPartition = ZeroZoneTrafficInterceptor.produceRequestToMap(data);
        short apiVersion = zoneRouterProduceRequest.apiVersion();
        CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
        RouterInProduceHandler handler = local ? localAppendHandler : routerInProduceHandler;
        // We should release the request after append completed.
        cf.whenComplete((resp, ex) -> FutureUtil.suppress(zoneRouterProduceRequest::close, LOGGER));
        handler.handleProduceAppend(
            ProduceRequestArgs.builder()
                .clientId(buildClientId(realEntriesPerPartition))
                .timeout(data.timeoutMs())
                // The CommittedEpochManager requires the data to be persisted prior to bumping the committed epoch.
                .requiredAcks((short) -1)
                .internalTopicsAllowed(flag.internalTopicsAllowed())
                .transactionId(data.transactionalId())
                .entriesPerPartition(realEntriesPerPartition)
                .responseCallback(rst -> {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ROUTER_IN],[RESPONSE],response={}", rst);
                    }
                    @SuppressWarnings("deprecation")
                    ProduceResponse produceResponse = new ProduceResponse(rst, 0, Collections.emptyList());
                    AutomqZoneRouterResponseData.Response response = new AutomqZoneRouterResponseData.Response()
                        .setData(ZoneRouterResponseCodec.encode(produceResponse.data()).array());
                    cf.complete(response);
                })
                .recordValidationStatsCallback(rst -> {
                })
                .apiVersion(apiVersion)
                .requestLocal(requestLocals.get())
                .build()
        );
        return cf;
    }

    public void setRouterInProduceHandler(RouterInProduceHandler routerInProduceHandler) {
        this.routerInProduceHandler = routerInProduceHandler;
    }

    protected ClientIdMetadata buildClientId(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        String clientId = String.format("%s=%s", ClientIdKey.AVAILABILITY_ZONE, rack);
        String connectionId = getConnectionIdFrom(entriesPerPartition);
        return ClientIdMetadata.of(clientId, null, connectionId);
    }

    protected String getConnectionIdFrom(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        for (Map.Entry<TopicPartition, MemoryRecords> entry : entriesPerPartition.entrySet()) {
            for (MutableRecordBatch batch : entry.getValue().batches()) {
                if (batch.hasProducerId()) {
                    return String.valueOf(batch.producerId());
                }
            }
        }
        return null;
    }

    static class PartitionProduceRequest {
        final ChannelOffset channelOffset;
        CompletableFuture<ByteBuf> unpackLinkCf;
        final CompletableFuture<AutomqZoneRouterResponseData.Response> responseCf = new CompletableFuture<>();

        public PartitionProduceRequest(ChannelOffset channelOffset) {
            this.channelOffset = channelOffset;
        }
    }

}
