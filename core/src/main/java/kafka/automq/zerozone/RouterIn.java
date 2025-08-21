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
import kafka.server.RequestLocal;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

class RouterIn {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterIn.class);

    private final Semaphore inflightAppendLimiter = new Semaphore(Math.min(50 * 1024 * 1024 * Systems.CPU_CORES, Integer.MAX_VALUE));
    private CompletableFuture<Void> lastRouterCf = CompletableFuture.completedFuture(null);

    private final ExecutorService executor = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(1, "object-cross-zone-produce-router-in", true, LOGGER);
    private final FastThreadLocal<RequestLocal> requestLocals = new FastThreadLocal<>() {
        @Override
        protected RequestLocal initialValue() {
            // The same as KafkaRequestHandler.requestLocal
            return RequestLocal.withThreadConfinedCaching();
        }
    };

    private RouterInProduceHandler routerInProduceHandler;

    private final ObjectStorage objectStorage;
    private final ElasticKafkaApis kafkaApis;
    private final String rack;

    public RouterIn(ObjectStorage objectStorage, ElasticKafkaApis kafkaApis, String rack) {
        this.objectStorage = objectStorage;
        this.kafkaApis = kafkaApis;
        this.rack = rack;
        this.routerInProduceHandler = kafkaApis::handleProduceAppendJavaCompatible;
    }

    public synchronized CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
        RouterRecord routerRecord = RouterRecord.decode(Unpooled.wrappedBuffer(metadata));
        try {
            if (inflightAppendLimiter.tryAcquire(routerRecord.size())) {
                return handleZoneRouterRequest0(routerRecord).whenComplete((rst, ex) -> {
                    inflightAppendLimiter.release(routerRecord.size());
                });
            } else {
                return FutureUtil.failedFuture(new ThrottlingQuotaExceededException("inflight append limit exceeded"));
            }
        } catch (Throwable e) {
            inflightAppendLimiter.release(routerRecord.size());
            LOGGER.error("[UNEXPECTED] handleZoneRouterRequest failed", e);
            return FutureUtil.failedFuture(e);
        }
    }

    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest0(RouterRecord routerRecord) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ROUTER_IN],data={}", routerRecord);
        }
        // inbound is consumed by the object storage
        ZeroZoneMetricsManager.recordRouterInBytes(routerRecord.nodeId(), routerRecord.size());
        CompletableFuture<List<ZoneRouterProduceRequest>> readCf = new ZoneRouterPackReader(routerRecord.nodeId(), routerRecord.bucketId(), routerRecord.objectId(), objectStorage)
            .readProduceRequests(new Position(routerRecord.position(), routerRecord.size()));
        // Orderly handle the request
        CompletableFuture<Void> prevLastRouterCf = lastRouterCf;
        CompletableFuture<AutomqZoneRouterResponse> appendCf = readCf
            .thenCompose(rst -> prevLastRouterCf.thenApply(nil -> rst))
            .thenComposeAsync(produces -> {
                List<CompletableFuture<AutomqZoneRouterResponseData.Response>> cfList = new ArrayList<>();
                produces.stream().map(request -> {
                    try (request) {
                        return append(request);
                    }
                }).forEach(cfList::add);
                return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).thenApply(nil -> {
                    AutomqZoneRouterResponseData response = new AutomqZoneRouterResponseData();
                    cfList.forEach(cf -> response.responses().add(cf.join()));
                    return new AutomqZoneRouterResponse(response);
                });
            }, executor);
        this.lastRouterCf = appendCf.thenAccept(rst -> {
        }).exceptionally(ex -> null);
        return appendCf;
    }

    public void setRouterInProduceHandler(RouterInProduceHandler routerInProduceHandler) {
        this.routerInProduceHandler = routerInProduceHandler;
    }

    CompletableFuture<AutomqZoneRouterResponseData.Response> append(
        ZoneRouterProduceRequest zoneRouterProduceRequest) {
        ZoneRouterProduceRequest.Flag flag = new ZoneRouterProduceRequest.Flag(zoneRouterProduceRequest.flag());
        ProduceRequestData data = zoneRouterProduceRequest.data();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[ROUTER_IN],[APPEND],data={}", data);
        }

        Map<TopicPartition, MemoryRecords> realEntriesPerPartition = ZeroZoneTrafficInterceptor.produceRequestToMap(data);
        short apiVersion = zoneRouterProduceRequest.apiVersion();
        CompletableFuture<AutomqZoneRouterResponseData.Response> cf = new CompletableFuture<>();
        // TODO: parallel request for different partitions
        routerInProduceHandler.handleProduceAppend(
            ProduceRequestArgs.builder()
                .clientId(buildClientId(realEntriesPerPartition))
                .timeout(10000)
                .requiredAcks(data.acks())
                .internalTopicsAllowed(flag.internalTopicsAllowed())
                .transactionId(data.transactionalId())
                .entriesPerPartition(realEntriesPerPartition)
                .responseCallback(rst -> {
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
}
