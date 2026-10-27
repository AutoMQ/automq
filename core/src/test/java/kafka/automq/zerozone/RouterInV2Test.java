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
import kafka.log.streamaspect.ElasticLogFileRecords;
import kafka.log.streamaspect.ElasticStreamSlice;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.stream.RouterChannelEpoch;

import com.automq.stream.Context;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.KVClient;
import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.utils.threads.EventLoop;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the ordering and progress guarantees of RouterIn V2 read completion queues.
 */
@Tag("S3Unit")
public class RouterInV2Test {

    /**
     * Given reads in one ordering stripe complete out of order, the queue must release them in arrival order.
     */
    @Test
    public void testOrderQueuePreservesArrivalOrder() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest first = request((short) 1);
        RouterInV2.PartitionProduceRequest second = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        queue.add(first);
        queue.add(second);

        second.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.drain(drained::add);
        assertEquals(List.of(), drained);

        first.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.drain(drained::add);
        assertEquals(List.of(first, second), drained);
    }

    /**
     * Given the head read fails, the completed failure must not prevent later requests from being released.
     */
    @Test
    public void testOrderQueueAdvancesAfterReadFailure() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest failed = request((short) 1);
        RouterInV2.PartitionProduceRequest succeeded = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        queue.add(failed);
        queue.add(succeeded);

        succeeded.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        failed.unpackLinkCf.completeExceptionally(new IllegalStateException("read failed"));
        queue.drain(drained::add);

        assertEquals(List.of(failed, succeeded), drained);
    }

    /**
     * Given independent ordering stripes, a completed read must not wait for another stripe's head read.
     */
    @Test
    public void testOrderQueuesDrainIndependently() {
        RouterInV2.OrderQueue blockedQueue = new RouterInV2.OrderQueue();
        RouterInV2.OrderQueue readyQueue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest blocked = request((short) 1);
        RouterInV2.PartitionProduceRequest ready = request((short) 2);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        blockedQueue.add(blocked);
        readyQueue.add(ready);

        ready.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        readyQueue.drain(drained::add);

        assertEquals(List.of(ready), drained);
    }

    /**
     * Given scheduling one completed read fails, the queue must fail that request and continue draining.
     */
    @Test
    public void testOrderQueueContinuesAfterSchedulingFailure() {
        RouterInV2.OrderQueue queue = new RouterInV2.OrderQueue();
        RouterInV2.PartitionProduceRequest failed = request((short) 1);
        RouterInV2.PartitionProduceRequest succeeded = request((short) 1);
        List<RouterInV2.PartitionProduceRequest> drained = new ArrayList<>();
        failed.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        succeeded.unpackLinkCf.complete(Unpooled.EMPTY_BUFFER);
        queue.add(failed);
        queue.add(succeeded);

        queue.drain(request -> {
            if (request == failed) {
                throw new IllegalStateException("scheduling failed");
            }
            drained.add(request);
        });

        assertInstanceOf(IllegalStateException.class, failed.responseCf.handle((rst, ex) -> ex).join());
        assertEquals(List.of(succeeded), drained);
    }

    /**
     * Given signed short order hints, indexing must use the complete unsigned 16-bit key space.
     */
    @Test
    public void testOrderHintIndexUsesUnsignedValue() {
        assertEquals(65535 % 3072, RouterInV2.index((short) -1, 3072));
        assertEquals(32768 % 3072, RouterInV2.index(Short.MIN_VALUE, 3072));
    }

    /** A timeout response must not release the epoch or request data while the storage append is pending. */
    @Test
    public void testLocalTimeoutRetainsEpochUntilAppendCompletes() {
        CompletableFuture<AppendResult> append = new CompletableFuture<>();
        ElasticStreamSlice slice = mock(ElasticStreamSlice.class);
        when(slice.append(any(), any())).thenReturn(append);
        ElasticLogFileRecords records = new ElasticLogFileRecords(slice, 0, 0);
        ConfirmWAL wal = mock(ConfirmWAL.class);
        KVClient kv = mock(KVClient.class);
        CompletableFuture<Void> commit = new CompletableFuture<>();
        when(wal.commit(anyLong())).thenReturn(commit);
        when(kv.putKV(any())).thenReturn(CompletableFuture.completedFuture(null));
        Context context = new Context();
        context.confirmWAL(wal);
        context.kvClient(kv);
        try (MockedStatic<Context> contextMock = mockStatic(Context.class);
            Fixture fixture = new Fixture(args -> {
                try {
                    records.append(args.entriesPerPartition().values().iterator().next(), 1);
                } catch (java.io.IOException ex) {
                    throw new AssertionError(ex);
                }
                respond(args, Errors.REQUEST_TIMED_OUT);
            })) {
            contextMock.when(Context::instance).thenReturn(context);
            ZoneRouterProduceRequest request = produceRequest();
            var previous = ZeroZoneThreadLocalContext.writeContext();
            var response = fixture.router.append(1, offset(), request);
            assertTrue(response.isDone());
            assertEquals(Errors.REQUEST_TIMED_OUT.code(), ZoneRouterResponseCodec.decode(
                Unpooled.wrappedBuffer(response.join().data())).responses().iterator().next().partitionResponses().get(0).errorCode());
            assertNotSame(previous, ZeroZoneThreadLocalContext.writeContext());
            assertNull(ZeroZoneThreadLocalContext.writeContext().channelOffset());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            assertEquals(1, request.refCnt());
            fixture.manager.onNewEpoch(new RouterChannelEpoch(-1, 1, 3, 0));
            fixture.manager.tryBumpCommittedEpoch();
            verify(wal, never()).commit(anyLong());
            append.complete(() -> 0L);
            assertEquals(0, fixture.manager.epochInflight(1).get());
            assertEquals(0, request.refCnt());
            fixture.manager.tryBumpCommittedEpoch();
            verify(wal).commit(anyLong());
            verify(kv, never()).putKV(any());
            commit.complete(null);
            verify(kv).putKV(any());
        }
    }

    /** Remote response completion must preserve registration until every underlying append has terminated. */
    @Test
    public void testRemoteTimeoutWaitsForAllAppendsEvenAfterFailure() {
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        AtomicInteger calls = new AtomicInteger();
        try (Fixture fixture = new Fixture(args -> {
            ZeroZoneThreadLocalContext.writeContext().detach().trackAppend(calls.getAndIncrement() == 0 ? first : second);
            respond(args, Errors.REQUEST_TIMED_OUT);
        })) {
            ByteBuf block = ZoneRouterPackWriter.encodeDataBlock(List.of(produceRequest()));
            ByteBuf secondBlock = ZoneRouterPackWriter.encodeDataBlock(List.of(produceRequest()));
            when(fixture.channel.get(any())).thenReturn(CompletableFuture.completedFuture(block),
                CompletableFuture.completedFuture(secondBlock));
            assertTrue(fixture.router.handleZoneRouterRequest(remoteRequest(2)).isDone());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            first.completeExceptionally(new IllegalStateException("append failed"));
            assertEquals(1, fixture.manager.epochInflight(1).get());
            second.complete(null);
            assertEquals(0, fixture.manager.epochInflight(1).get());
            assertEquals(0, block.refCnt());
            assertEquals(0, secondBlock.refCnt());
        }
    }

    /** Completed appends must not release a request while asynchronous verification can still register more work. */
    @Test
    public void testDetachedContextKeepsRequestRegisteredUntilResponse() {
        AtomicReference<ZeroZoneThreadLocalContext.WriteContext> detached = new AtomicReference<>();
        AtomicReference<ProduceRequestArgs> pending = new AtomicReference<>();
        try (Fixture fixture = new Fixture(args -> {
            detached.set(ZeroZoneThreadLocalContext.writeContext().detach());
            pending.set(args);
        })) {
            var response = fixture.router.append(1, offset(), produceRequest());
            assertFalse(response.isDone());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            ZeroZoneThreadLocalContext.attach(detached.get());
            ZeroZoneThreadLocalContext.writeContext().detach().trackAppend(CompletableFuture.completedFuture(null));
            assertEquals(1, fixture.manager.epochInflight(1).get());
            respond(pending.get(), Errors.NONE);
            assertTrue(response.isDone());
            assertEquals(0, fixture.manager.epochInflight(1).get());
        }
    }

    /** A fenced local or remote request is rejected before append, while DIRECT writes remain independent of epochs. */
    @Test
    public void testFencedRequestsAndDirectBypass() {
        try (Fixture fixture = new Fixture(args -> respond(args, Errors.NONE))) {
            fixture.manager.onNewEpoch(new RouterChannelEpoch(-1, 1, 3, 0));
            ZoneRouterProduceRequest request = produceRequest();
            assertTrue(fixture.router.append(1, offset(), request).isCompletedExceptionally());
            assertEquals(0, request.refCnt());
            assertTrue(fixture.router.handleZoneRouterRequest(remoteRequest()).isCompletedExceptionally());
            assertFalse(fixture.router.append(1, null, produceRequest()).isCompletedExceptionally());
            assertEquals(0, fixture.manager.epochInflight(1).get());
        }
    }

    /** A synchronous handler failure releases registration only after any append already started has finished. */
    @Test
    public void testSynchronousFailureAfterAppendRetainsEpoch() {
        CompletableFuture<Void> append = new CompletableFuture<>();
        try (Fixture fixture = new Fixture(args -> {
            ZeroZoneThreadLocalContext.writeContext().detach().trackAppend(append);
            throw new IllegalStateException("handler failed");
        })) {
            ZoneRouterProduceRequest request = produceRequest();
            assertTrue(fixture.router.append(1, offset(), request).isCompletedExceptionally());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            append.completeExceptionally(new IllegalStateException("storage failed"));
            assertEquals(0, fixture.manager.epochInflight(1).get());
            assertEquals(0, request.refCnt());
        }
    }

    /** A remote read failure cannot leave an epoch registration behind. */
    @Test
    public void testRemoteReadFailureReleasesEpoch() {
        try (Fixture fixture = new Fixture(args -> respond(args, Errors.NONE))) {
            when(fixture.channel.get(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("missing")));
            assertTrue(fixture.router.handleZoneRouterRequest(remoteRequest()).isCompletedExceptionally());
            assertEquals(0, fixture.manager.epochInflight(1).get());
        }
    }

    /** Scheduling rejection has no storage work and must release both the request and epoch registration. */
    @Test
    public void testSchedulingFailureReleasesRequest() {
        try (Fixture fixture = new Fixture(args -> respond(args, Errors.NONE))) {
            fixture.eventLoops.constructed().forEach(loop -> doAnswer(invocation -> {
                throw new IllegalStateException("executor stopped");
            }).when(loop).execute(any()));
            ZoneRouterProduceRequest request = produceRequest();
            assertTrue(fixture.router.append(1, offset(), request).isCompletedExceptionally());
            assertEquals(0, fixture.manager.epochInflight(1).get());
            assertEquals(0, request.refCnt());
        }
    }

    /** Failure while preparing a remote batch must not release subrequests that were already scheduled. */
    @Test
    public void testPartialRemoteSetupFailureRetainsEarlierRequest() {
        CompletableFuture<ByteBuf> read = new CompletableFuture<>();
        CompletableFuture<Void> append = new CompletableFuture<>();
        try (Fixture fixture = new Fixture(args -> {
            ZeroZoneThreadLocalContext.writeContext().detach().trackAppend(append);
            respond(args, Errors.NONE);
        })) {
            when(fixture.channel.get(any())).thenReturn(read).thenThrow(new IllegalStateException("read setup failed"));
            ByteBuf encoded = new RouterRecordV2(1, List.of(offset().byteBuf(), offset().byteBuf())).encode();
            byte[] bytes = new byte[encoded.readableBytes()];
            encoded.readBytes(bytes);
            encoded.release();
            var response = fixture.router.handleZoneRouterRequest(
                new AutomqZoneRouterRequestData().setRouteEpoch(1).setMetadata(bytes));
            assertFalse(response.isDone());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            read.complete(ZoneRouterPackWriter.encodeDataBlock(List.of(produceRequest())));
            assertTrue(response.isCompletedExceptionally());
            assertEquals(1, fixture.manager.epochInflight(1).get());
            append.complete(null);
            assertEquals(0, fixture.manager.epochInflight(1).get());
        }
    }

    /** A request rejected before storage leaves an unconsumed context that the next RouterIn request replaces. */
    @Test
    public void testNewRequestReplacesUnconsumedContext() {
        AtomicReference<ZeroZoneThreadLocalContext.WriteContext> failedContext = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        ChannelOffset nextOffset = offset();
        CompletableFuture<Void> append = new CompletableFuture<>();
        try (Fixture fixture = new Fixture(args -> {
            if (calls.getAndIncrement() == 0) {
                failedContext.set(ZeroZoneThreadLocalContext.writeContext());
                respond(args, Errors.NOT_LEADER_OR_FOLLOWER);
            } else {
                var context = ZeroZoneThreadLocalContext.writeContext().detach();
                assertNotSame(failedContext.get(), context);
                assertSame(nextOffset, context.channelOffset());
                context.trackAppend(append);
                respond(args, Errors.REQUEST_TIMED_OUT);
            }
        })) {
            fixture.router.append(1, offset(), produceRequest()).join();
            assertEquals(0, fixture.manager.epochInflight(1).get());
            assertSame(failedContext.get(), ZeroZoneThreadLocalContext.writeContext());
            fixture.router.append(1, nextOffset, produceRequest()).join();
            assertEquals(1, fixture.manager.epochInflight(1).get());
            assertNull(ZeroZoneThreadLocalContext.writeContext().channelOffset());
            append.complete(null);
            assertEquals(0, fixture.manager.epochInflight(1).get());
        }
    }

    private static void respond(ProduceRequestArgs args, Errors error) {
        args.responseCallback().accept(Map.of(new TopicPartition("topic", 0), new ProduceResponse.PartitionResponse(error)));
    }

    private static ChannelOffset offset() {
        return ChannelOffset.of((short) 0, (short) 1, 1, 1, Unpooled.EMPTY_BUFFER);
    }

    private static AutomqZoneRouterRequestData remoteRequest() {
        return remoteRequest(1);
    }

    private static AutomqZoneRouterRequestData remoteRequest(int count) {
        List<ByteBuf> offsets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            offsets.add(offset().byteBuf());
        }
        ByteBuf encoded = new RouterRecordV2(1, offsets).encode();
        byte[] bytes = new byte[encoded.readableBytes()];
        encoded.readBytes(bytes);
        encoded.release();
        return new AutomqZoneRouterRequestData().setRouteEpoch(1).setMetadata(bytes);
    }

    private static ZoneRouterProduceRequest produceRequest() {
        ProduceRequestData data = new ProduceRequestData().setTimeoutMs(1).setAcks((short) -1);
        data.topicData().add(new ProduceRequestData.TopicProduceData().setName("topic")
            .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[] {1}))))));
        return new ZoneRouterProduceRequest((short) 11, (short) 0, data);
    }

    private static class Fixture implements AutoCloseable {
        final CommittedEpochManager manager = new CommittedEpochManager(1);
        final RouterChannel channel = mock(RouterChannel.class);
        final MockedConstruction<EventLoop> eventLoops;
        final RouterInV2 router;

        Fixture(RouterInProduceHandler handler) {
            manager.onNewEpoch(new RouterChannelEpoch(-1, 0, 1, 0));
            manager.close();
            eventLoops = mockConstruction(EventLoop.class, (loop, context) ->
                doAnswer(invocation -> {
                    invocation.<Runnable>getArgument(0).run();
                    return null;
                }).when(loop).execute(any()));
            ElasticKafkaApis apis = mock(ElasticKafkaApis.class);
            doAnswer(invocation -> {
                handler.handleProduceAppend(invocation.getArgument(0));
                return null;
            }).when(apis).handleProduceAppendJavaCompatible(any());
            RouterChannelProvider provider = mock(RouterChannelProvider.class);
            when(provider.readOnlyChannel(1)).thenReturn(channel);
            router = new RouterInV2(provider, apis, "rack", new MockTime(), manager);
        }

        @Override
        public void close() {
            ZeroZoneThreadLocalContext.writeContext().detach();
            eventLoops.close();
            manager.close();
        }
    }

    private static RouterInV2.PartitionProduceRequest request(short orderHint) {
        ChannelOffset channelOffset = ChannelOffset.of((short) 0, orderHint, 0, 0, Unpooled.EMPTY_BUFFER);
        RouterInV2.PartitionProduceRequest request = new RouterInV2.PartitionProduceRequest(channelOffset);
        request.unpackLinkCf = new CompletableFuture<>();
        return request;
    }
}
