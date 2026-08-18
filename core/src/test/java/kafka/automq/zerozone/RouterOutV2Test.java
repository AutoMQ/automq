/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.AutomqZoneRouterResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies how RouterOut V2 selects and aggregates local write paths.
 */
@Tag("S3Unit")
public class RouterOutV2Test {
    private static final Node CURRENT_NODE = new Node(1, "localhost", 9092);
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);
    private static final TopicPartition REMOTE_TOPIC_PARTITION = new TopicPartition("topic", 1);

    /**
     * Given DIRECT mode and a partition routed to the current Broker, the request writes the Partition without
     * appending RouterChannel.
     */
    @Test
    public void testDirectModeWritesLocalPartitionWithoutRouterChannel() {
        StubRouterChannel routerChannel = new StubRouterChannel();
        AtomicInteger localAppendCount = new AtomicInteger();
        AtomicReference<ChannelOffset> channelOffset = new AtomicReference<>();
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            routerChannel,
            (topic, partition, clientId) -> CURRENT_NODE,
            (offset, request) -> {
                localAppendCount.incrementAndGet();
                channelOffset.set(offset);
                return CompletableFuture.completedFuture(successResponse());
            },
            LocalWriteMode.DIRECT,
            new FailedAsyncSender(),
            new MockTime()
        );

        routerOut.handleProduceAppendProxy(produceArgs(result::set, (short) -1));

        assertEquals(0, routerChannel.appendCount.get());
        assertEquals(1, localAppendCount.get());
        assertNull(channelOffset.get());
        assertEquals(Errors.NONE, result.get().get(TOPIC_PARTITION).error);
    }

    /**
     * Given ROUTER_CHANNEL mode and a partition routed to the current Broker, the request keeps the existing linked
     * local append path.
     */
    @Test
    public void testRouterChannelModeKeepsLinkedLocalWrite() {
        StubRouterChannel routerChannel = new StubRouterChannel();
        AtomicInteger linkedAppendCount = new AtomicInteger();
        AtomicReference<ChannelOffset> channelOffset = new AtomicReference<>();
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            routerChannel,
            (topic, partition, clientId) -> CURRENT_NODE,
            (offset, request) -> {
                linkedAppendCount.incrementAndGet();
                channelOffset.set(offset);
                return CompletableFuture.completedFuture(successResponse());
            },
            LocalWriteMode.ROUTER_CHANNEL,
            new FailedAsyncSender(),
            new MockTime()
        );

        routerOut.handleProduceAppendProxy(produceArgs(result::set, (short) -1));

        assertEquals(1, routerChannel.appendCount.get());
        assertEquals(1, linkedAppendCount.get());
        assertEquals(Errors.NONE, result.get().get(TOPIC_PARTITION).error);
        assertSame(routerChannel.offset, channelOffset.get().byteBuf());
    }

    /**
     * Given DIRECT mode with local and remote partitions, only the local partition bypasses RouterChannel.
     */
    @Test
    public void testDirectModeSplitsLocalAndRemotePartitions() {
        StubRouterChannel routerChannel = new StubRouterChannel();
        AtomicReference<ChannelOffset> directChannelOffset = new AtomicReference<>();
        CompletableFuture<AutomqZoneRouterResponseData.Response> directResponse = new CompletableFuture<>();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        CompletableFuture<ClientResponse> remoteResponse = new CompletableFuture<>();
        Node remoteNode = new Node(2, "remote", 9092);
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            routerChannel,
            (topic, partition, clientId) -> partition == TOPIC_PARTITION.partition() ? CURRENT_NODE : remoteNode,
            (channelOffset, request) -> {
                directChannelOffset.set(channelOffset);
                return directResponse;
            },
            LocalWriteMode.DIRECT,
            new StubAsyncSender(remoteResponse),
            new MockTime()
        );
        Map<TopicPartition, MemoryRecords> entries = Map.of(
            TOPIC_PARTITION, records(),
            REMOTE_TOPIC_PARTITION, records()
        );

        routerOut.handleProduceAppendProxy(produceArgs(entries, response -> {
            callbackCount.incrementAndGet();
            result.set(response);
        }, (short) -1));

        assertNull(directChannelOffset.get());
        assertEquals(1, routerChannel.appendCount.get());
        assertEquals(0, callbackCount.get());

        directResponse.complete(successResponse());
        assertEquals(0, callbackCount.get());

        remoteResponse.complete(successClientResponse(REMOTE_TOPIC_PARTITION));
        waitFor(() -> callbackCount.get() == 1);

        assertEquals(1, callbackCount.get());
        assertEquals(Errors.NONE, result.get().get(TOPIC_PARTITION).error);
        assertEquals(Errors.NONE, result.get().get(REMOTE_TOPIC_PARTITION).error);
    }

    /**
     * Given a DIRECT local write with acks=0, the client callback returns once without waiting for persistence.
     */
    @Test
    public void testDirectModeAcksZeroReturnsOnceWithoutWaiting() {
        StubRouterChannel routerChannel = new StubRouterChannel();
        AtomicInteger directAppendCount = new AtomicInteger();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            routerChannel,
            (topic, partition, clientId) -> CURRENT_NODE,
            (channelOffset, request) -> {
                directAppendCount.incrementAndGet();
                return new CompletableFuture<>();
            },
            LocalWriteMode.DIRECT,
            new FailedAsyncSender(),
            new MockTime()
        );

        routerOut.handleProduceAppendProxy(produceArgs(response -> {
            callbackCount.incrementAndGet();
            result.set(response);
        }, (short) 0));

        assertEquals(1, directAppendCount.get());
        assertEquals(1, callbackCount.get());
        assertEquals(Errors.NONE, result.get().get(TOPIC_PARTITION).error);
        assertEquals(0, routerChannel.appendCount.get());
    }

    /**
     * Given no route target, neither local persistence path starts and the response remains retriable.
     */
    @Test
    public void testNoRouteReturnsNotLeaderOrFollower() {
        StubRouterChannel routerChannel = new StubRouterChannel();
        AtomicInteger localAppendCount = new AtomicInteger();
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            routerChannel,
            (topic, partition, clientId) -> Node.noNode(),
            (channelOffset, request) -> {
                localAppendCount.incrementAndGet();
                return CompletableFuture.completedFuture(successResponse());
            },
            LocalWriteMode.DIRECT,
            new FailedAsyncSender(),
            new MockTime()
        );

        routerOut.handleProduceAppendProxy(produceArgs(result::set, (short) -1));

        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, result.get().get(TOPIC_PARTITION).error);
        assertEquals(0, routerChannel.appendCount.get());
        assertEquals(0, localAppendCount.get());
    }

    /**
     * Given a failed direct append, the response preserves its Kafka protocol error.
     */
    @Test
    public void testDirectModePreservesAppendError() {
        AtomicReference<Map<TopicPartition, ProduceResponse.PartitionResponse>> result = new AtomicReference<>();
        RouterOutV2 routerOut = new RouterOutV2(
            CURRENT_NODE,
            new StubRouterChannel(),
            (topic, partition, clientId) -> CURRENT_NODE,
            (channelOffset, request) -> CompletableFuture.failedFuture(
                Errors.INVALID_PRODUCER_ID_MAPPING.exception()),
            LocalWriteMode.DIRECT,
            new FailedAsyncSender(),
            new MockTime()
        );

        routerOut.handleProduceAppendProxy(produceArgs(result::set, (short) -1));

        assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, result.get().get(TOPIC_PARTITION).error);
    }

    private static ProduceRequestArgs produceArgs(
        java.util.function.Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> callback,
        short acks
    ) {
        return produceArgs(Map.of(TOPIC_PARTITION, records()), callback, acks);
    }

    private static ProduceRequestArgs produceArgs(
        Map<TopicPartition, MemoryRecords> entries,
        java.util.function.Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> callback,
        short acks
    ) {
        return ProduceRequestArgs.builder()
            .apiVersion((short) 11)
            .clientId(ClientIdMetadata.of("client", null, "connection"))
            .timeout(10_000)
            .requiredAcks(acks)
            .entriesPerPartition(entries)
            .responseCallback(callback)
            .recordValidationStatsCallback(ignored -> { })
            .build();
    }

    private static MemoryRecords records() {
        MemoryRecords records = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord("value".getBytes(StandardCharsets.UTF_8))
        );
        return records;
    }

    private static AutomqZoneRouterResponseData.Response successResponse() {
        return successResponse(TOPIC_PARTITION);
    }

    private static AutomqZoneRouterResponseData.Response successResponse(TopicPartition topicPartition) {
        ProduceResponseData.PartitionProduceResponse partition =
            new ProduceResponseData.PartitionProduceResponse().setIndex(topicPartition.partition());
        ProduceResponseData.TopicProduceResponse topic = new ProduceResponseData.TopicProduceResponse()
            .setName(topicPartition.topic())
            .setPartitionResponses(List.of(partition));
        ProduceResponseData data = new ProduceResponseData().setResponses(
            new ProduceResponseData.TopicProduceResponseCollection(List.of(topic).iterator())
        );
        return new AutomqZoneRouterResponseData.Response().setData(ZoneRouterResponseCodec.encode(data).array());
    }

    private static ClientResponse successClientResponse(TopicPartition topicPartition) {
        AutomqZoneRouterResponse response = new AutomqZoneRouterResponse(new AutomqZoneRouterResponseData()
            .setResponses(List.of(successResponse(topicPartition))));
        return new ClientResponse(null, null, "remote", 0, 0, false, null, null, response);
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) {
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
            Thread.onSpinWait();
        }
    }

    private static final class StubRouterChannel implements RouterChannel {
        private final AtomicInteger appendCount = new AtomicInteger();
        private final ByteBuf offset = Unpooled.wrappedBuffer(new byte[] {1});
        private AppendResult lastAppendResult;

        @Override
        public CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data) {
            appendCount.incrementAndGet();
            lastAppendResult = new AppendResult(1L, offset);
            return CompletableFuture.completedFuture(lastAppendResult);
        }

        @Override
        public CompletableFuture<ByteBuf> get(ByteBuf channelOffset) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void nextEpoch(long epoch) {
        }

        @Override
        public void trim(long epoch) {
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class FailedAsyncSender implements AsyncSender {
        @Override
        public <T extends AbstractRequest> CompletableFuture<org.apache.kafka.clients.ClientResponse> sendRequest(
            Node node,
            AbstractRequest.Builder<T> requestBuilder
        ) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void initiateClose() {
        }

        @Override
        public void close() {
        }
    }

    private static final class StubAsyncSender implements AsyncSender {
        private final CompletableFuture<ClientResponse> response;

        private StubAsyncSender(CompletableFuture<ClientResponse> response) {
            this.response = response;
        }

        @Override
        public <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
            Node node,
            AbstractRequest.Builder<T> requestBuilder
        ) {
            return response;
        }

        @Override
        public void initiateClose() {
        }

        @Override
        public void close() {
        }
    }
}
