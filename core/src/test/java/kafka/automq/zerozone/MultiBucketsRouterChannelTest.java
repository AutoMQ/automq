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

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.impl.object.ObjectWALConfig;
import com.automq.stream.s3.wal.impl.object.ObjectWALService;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Protects batching, routing, completion order, trimming and buffer ownership across buckets. */
@Tag("S3Unit")
public class MultiBucketsRouterChannelTest {
    /** Given two small appends, size flushing submits the whole batch to one bucket. */
    @Test
    public void testSizeBatchUsesOneBucketAndFirstBucketOptions() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=100&batchInterval=60000", "1@s3://b?maxBytesInBatch=1");
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append((short) 1);
        assertEquals(0, fixture.flushed.size());
        CompletableFuture<RouterChannel.AppendResult> second = fixture.append((short) 2);
        Wal wal = fixture.nextFlush();
        assertEquals(2, wal.appends.size());
        wal.succeed(0, 1);
        wal.succeed(1, 2);
        assertEquals(first.join().channelOffset().getShort(1), second.join().channelOffset().getShort(1));
        fixture.close();
    }

    /** A partial batch is flushed when the first bucket's interval expires. */
    @Test
    public void testIntervalFlush() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?batchInterval=10&maxBytesInBatch=1000");
        CompletableFuture<RouterChannel.AppendResult> result = fixture.append((short) 1);
        fixture.nextFlush().succeed(0, 1);
        assertEquals(0, ChannelOffset.of(result.join().channelOffset()).channelId());
        fixture.close();
    }

    /** A partial batch uses the epoch current when it is flushed. */
    @Test
    public void testBatchUsesFlushEpoch() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?batchInterval=60000");
        fixture.router.nextEpoch(5);
        CompletableFuture<RouterChannel.AppendResult> result = fixture.append((short) 1);
        fixture.router.nextEpoch(6);
        assertEquals(0, fixture.flushed.size());
        fixture.router.flushBatch();
        Wal wal = fixture.nextFlush();
        fixture.router.nextEpoch(7);
        wal.succeed(0, 1);
        assertEquals(6, result.join().epoch());
        fixture.close();
    }

    /** Cross-bucket callbacks preserve one hint's order while other hints complete independently. */
    @Test
    public void testOrderedCallbacksAcrossBucketsAndFailures() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1", "1@s3://b");
        fixture.router.nextEpoch(5);
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append((short) 7);
        Wal a = fixture.nextFlush();
        CompletableFuture<RouterChannel.AppendResult> second = fixture.append((short) 7);
        Wal b = fixture.nextFlush();
        assertNotEquals(a.id, b.id);
        b.succeed(0, 2);
        assertFalse(second.isDone());
        CompletableFuture<RouterChannel.AppendResult> independent = fixture.append((short) 8);
        Wal c = fixture.nextFlush();
        c.succeed(c.appends.size() - 1, 3);
        assertTrue(independent.isDone());
        fixture.router.trim(5);
        verify(b.mock, never()).trim(any());
        fixture.router.nextEpoch(8);
        a.appends.get(0).completeExceptionally(new IllegalStateException("upload failed"));
        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isDone());
        assertEquals(5, second.join().epoch());
        fixture.router.trim(5);
        verify(b.mock, timeout(5000)).trim(DefaultRecordOffset.of(1, 3, 1));
        fixture.close();
    }

    /** Trimming waits for every append callback in the committed epoch. */
    @Test
    public void testTrimWaitsForBatchAndCallbacks() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=100&batchInterval=60000");
        fixture.router.nextEpoch(5);
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append((short) 1);
        fixture.append((short) 2);
        Wal wal = fixture.nextFlush();
        fixture.router.nextEpoch(6);
        fixture.router.trim(5);
        verify(fixture.wals.get((short) 0).mock, never()).trim(any());
        fixture.router.nextEpoch(7);
        wal.succeed(0, 10);
        fixture.router.trim(5);
        verify(wal.mock, never()).trim(any());
        wal.succeed(1, 20);
        assertEquals(5, first.join().epoch());
        fixture.router.trim(7);
        verify(wal.mock, timeout(5000)).trim(DefaultRecordOffset.of(1, 20, 1));
        fixture.close();
    }

    /** An idle router trim persists its marker without requiring another append. */
    @Test
    public void testIdleTrimWithRealManualWAL() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(42)
            .withEpoch(1).withType("rc").withManualMode(true).build();
        ObjectWALService wal = spy(new ObjectWALService(Time.SYSTEM, storage, config));
        wal.start();
        CompletableFuture<CompletableFuture<Void>> trimming = new CompletableFuture<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<Void> result = (CompletableFuture<Void>) invocation.callRealMethod();
            trimming.complete(result);
            return result;
        }).when(wal).trim(any());
        MultiBucketsRouterChannel router = new MultiBucketsRouterChannel(42,
            List.of(BucketURI.parse("0@s3://a?maxBytesInBatch=1")), false,
            (bucket, readOnly) -> wal);
        try {
            router.append(43, (short) 1, Unpooled.buffer().writeByte(42)).get(5, TimeUnit.SECONDS);
            router.trim(0);
            trimming.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            assertEquals(1, storage.list("").join().size());
        } finally {
            router.close().get(5, TimeUnit.SECONDS);
            storage.close();
        }
    }

    /** Advancing epoch separates batches so trimming cannot delete a newer target-node record. */
    @Test
    public void testEpochTransitionKeepsNewerData() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();
        ObjectWALConfig config = ObjectWALConfig.builder().withClusterId("cluster").withNodeId(42)
            .withEpoch(1).withType("rc").withManualMode(true).build();
        ObjectWALService wal = spy(new ObjectWALService(Time.SYSTEM, storage, config));
        wal.start();
        CompletableFuture<CompletableFuture<Void>> trimming = new CompletableFuture<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<Void> result = (CompletableFuture<Void>) invocation.callRealMethod();
            trimming.complete(result);
            return result;
        }).when(wal).trim(any());
        MultiBucketsRouterChannel router = new MultiBucketsRouterChannel(42,
            List.of(BucketURI.parse("0@s3://a?batchInterval=60000")), false,
            (bucket, readOnly) -> wal);
        try {
            router.nextEpoch(5);
            CompletableFuture<RouterChannel.AppendResult> older = router.append(2, (short) 1, Unpooled.buffer().writeByte(10));
            router.flushBatch();
            assertEquals(5, older.get(5, TimeUnit.SECONDS).epoch());
            router.nextEpoch(6);
            CompletableFuture<RouterChannel.AppendResult> newer = router.append(1, (short) 2, Unpooled.buffer().writeByte(20));
            router.flushBatch();
            RouterChannel.AppendResult result = newer.get(5, TimeUnit.SECONDS);
            router.trim(5);
            trimming.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            ByteBuf data = router.get(result.channelOffset()).get(5, TimeUnit.SECONDS);
            assertEquals(20, data.readByte());
            data.release();
        } finally {
            router.close().get(5, TimeUnit.SECONDS);
            storage.close();
        }
    }

    /** A batch holds the Router write lock until its append and flush calls have been submitted. */
    @Test
    public void testConcurrentFlushSubmissionOrder() throws Exception {
        ObjectWALService wal = mock(ObjectWALService.class);
        CountDownLatch firstAppendEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstAppend = new CountDownLatch(1);
        AtomicInteger appendCount = new AtomicInteger();
        List<Long> submittedOffsets = Collections.synchronizedList(new ArrayList<>());
        when(wal.append(any(), any())).thenAnswer(invocation -> {
            StreamRecordBatch record = invocation.getArgument(1);
            int index = appendCount.getAndIncrement();
            if (index == 0) {
                firstAppendEntered.countDown();
                assertTrue(releaseFirstAppend.await(5, TimeUnit.SECONDS));
            }
            submittedOffsets.add(record.getBaseOffset());
            record.release();
            com.automq.stream.s3.wal.AppendResult result = mock(com.automq.stream.s3.wal.AppendResult.class);
            when(result.recordOffset()).thenReturn(DefaultRecordOffset.of(1, index, 1));
            return CompletableFuture.completedFuture(result);
        });
        when(wal.flush()).thenReturn(CompletableFuture.completedFuture(null));
        MultiBucketsRouterChannel router = new MultiBucketsRouterChannel(42,
            List.of(BucketURI.parse("0@s3://a?batchInterval=60000")), false,
            (bucket, readOnly) -> wal);
        try {
            router.nextEpoch(5);
            CompletableFuture<RouterChannel.AppendResult> first = router.append(42, (short) 1,
                Unpooled.buffer().writeByte(1));
            CompletableFuture<Void> firstFlush = CompletableFuture.runAsync(router::flushBatch);
            assertTrue(firstAppendEntered.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> nextEpoch = CompletableFuture.runAsync(() -> router.nextEpoch(6));
            assertFalse(nextEpoch.isDone());
            releaseFirstAppend.countDown();
            firstFlush.get(5, TimeUnit.SECONDS);
            nextEpoch.get(5, TimeUnit.SECONDS);

            CompletableFuture<RouterChannel.AppendResult> second = router.append(42, (short) 2,
                Unpooled.buffer().writeByte(2));
            router.flushBatch();
            assertEquals(List.of(1L, 2L), submittedOffsets);
            assertEquals(5, first.get(5, TimeUnit.SECONDS).epoch());
            assertEquals(6, second.get(5, TimeUnit.SECONDS).epoch());
        } finally {
            releaseFirstAppend.countDown();
            router.close().get(5, TimeUnit.SECONDS);
        }
    }

    /** A full WAL rejects one request while the same batch still flushes records it accepted. */
    @Test
    public void testOverCapacityFailsRequestAndFlushesBatch() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();
        BucketURI bucket = BucketURI.parse(
            "0@s3://a?batchInterval=60000&maxBytesInBatch=1000&maxUnflushedBytes=1");
        ObjectWALConfig config = ObjectWALConfig.builder().withURI(bucket.toIdURI()).withClusterId("cluster")
            .withNodeId(42).withEpoch(1).withType("rc").withManualMode(true).build();
        ObjectWALService wal = new ObjectWALService(Time.SYSTEM, storage, config);
        wal.start();
        MultiBucketsRouterChannel router = new MultiBucketsRouterChannel(42, List.of(bucket), false,
            (ignored, readOnly) -> wal);
        try {
            CompletableFuture<RouterChannel.AppendResult> first = router.append(42, (short) 1,
                Unpooled.buffer().writeByte(1));
            CompletableFuture<RouterChannel.AppendResult> second = router.append(42, (short) 2,
                Unpooled.buffer().writeByte(2));
            router.flushBatch();
            first.get(5, TimeUnit.SECONDS);
            assertTrue(second.isCompletedExceptionally());
        } finally {
            router.close().get(5, TimeUnit.SECONDS);
            storage.close();
        }
    }

    /** Concurrent WAL callbacks cannot complete a later request while the preceding callback is still running. */
    @Test
    public void testConcurrentCallbacksCompleteInOrder() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1", "1@s3://b");
        CountDownLatch firstCallbackEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCallback = new CountDownLatch(1);
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append((short) 1);
        Wal firstWal = fixture.nextFlush();
        CompletableFuture<Void> firstCallback = first.thenRun(() -> {
            firstCallbackEntered.countDown();
            try {
                assertTrue(releaseFirstCallback.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        CompletableFuture<RouterChannel.AppendResult> second = fixture.append((short) 1);
        Wal secondWal = fixture.nextFlush();
        try {
            CompletableFuture<Void> firstCompletion = CompletableFuture.runAsync(() -> firstWal.succeed(0, 1));
            assertTrue(firstCallbackEntered.await(5, TimeUnit.SECONDS));
            CompletableFuture<Void> secondCompletion = CompletableFuture.runAsync(() -> secondWal.succeed(0, 2));
            assertFalse(second.isDone());
            releaseFirstCallback.countDown();
            firstCompletion.get(5, TimeUnit.SECONDS);
            secondCompletion.get(5, TimeUnit.SECONDS);
            firstCallback.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            releaseFirstCallback.countDown();
            fixture.close();
        }
    }

    /** An ordered completion callback can advance the epoch without upgrading a held router read lock. */
    @Test
    public void testCompletionCallbackCanAdvanceEpoch() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1");
        CompletableFuture<RouterChannel.AppendResult> append = fixture.append((short) 1);
        Wal wal = fixture.nextFlush();
        CompletableFuture<Void> callback = append.thenRun(() -> fixture.router.nextEpoch(1));
        CompletableFuture<Void> completion = CompletableFuture.runAsync(() -> wal.succeed(0, 1));
        completion.get(5, TimeUnit.SECONDS);
        callback.get(5, TimeUnit.SECONDS);
        fixture.close();
    }

    /** Read-only buckets remain readable but never receive writes. */
    @Test
    public void testReadRoutingAndReadOnlyBuckets() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?mode=r&maxBytesInBatch=1", "1@s3://b?mode=r", "2@s3://c");
        fixture.append((short) 1);
        Wal writer = fixture.nextFlush();
        assertEquals(2, writer.id);
        writer.succeed(0, 1);
        StreamRecordBatch record = StreamRecordBatch.of(1, 0, 0, 1, Unpooled.wrappedBuffer(new byte[] {42}), DefaultByteBufSupplier.INSTANCE);
        when(fixture.wals.get((short) 0).mock.get(any(RecordOffset.class))).thenReturn(CompletableFuture.completedFuture(record));
        ByteBuf offset = ChannelOffset.of((short) 0, (short) 1, 42, 1, DefaultRecordOffset.of(1, 1, 1).buffer()).byteBuf();
        ByteBuf payload = fixture.router.get(offset).join();
        assertEquals(42, payload.readByte());
        payload.release();
        ByteBuf unknown = ChannelOffset.of((short) 9, (short) 1, 42, 1, DefaultRecordOffset.of(1, 1, 1).buffer()).byteBuf();
        assertTrue(fixture.router.get(unknown).isCompletedExceptionally());
        fixture.close();
    }

    /** A recently appended remote payload is read from the router cache without accessing the WAL reader. */
    @Test
    public void testReadRecentlyAppendedPayloadFromCache() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1");
        CompletableFuture<RouterChannel.AppendResult> appended = fixture.append((short) 1);
        Wal wal = fixture.nextFlush();
        wal.succeed(0, 1);
        ByteBuf payload = fixture.router.get(appended.get(5, TimeUnit.SECONDS).channelOffset())
            .get(5, TimeUnit.SECONDS);
        assertEquals(1, payload.readByte());
        payload.release();
        verify(wal.mock, never()).get(any(RecordOffset.class));
        fixture.close();
    }

    /** Close drains the last batch, shuts down all WALs once and consumes rejected input buffers. */
    @Test
    public void testCloseDrainsBatchAndRejectsWrites() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?batchInterval=60000");
        CompletableFuture<RouterChannel.AppendResult> append = fixture.append((short) 1);
        CompletableFuture<Void> close = fixture.router.close();
        assertSame(close, fixture.router.close());
        fixture.nextFlush().succeed(0, 1);
        close.get(5, TimeUnit.SECONDS);
        assertTrue(append.isDone());
        verify(fixture.wals.get((short) 0).mock).shutdownGracefully();
        ByteBuf data = Unpooled.buffer().writeByte(1);
        assertTrue(fixture.router.append(42, (short) 1, data).isCompletedExceptionally());
        assertEquals(0, data.refCnt());
    }

    /** Bad routing options fail before any storage is opened. */
    @Test
    public void testInvalidOptions() {
        for (List<BucketURI> buckets : List.of(
            List.of(BucketURI.parse("0@s3://a"), BucketURI.parse("0@s3://b")),
            List.of(BucketURI.parse("0@s3://a?mode=r")),
            List.of(BucketURI.parse("0@s3://a?mode=invalid")),
            List.of(BucketURI.parse("0@s3://a?maxBytesInBatch=0")))) {
            assertThrows(IllegalArgumentException.class, () -> new MultiBucketsRouterChannel(42, buckets, false,
                (bucket, readOnly) -> {
                    throw new AssertionError("Factory must not be called");
                }));
        }
    }

    /** Every writable bucket participates, including configurations with more than eight buckets. */
    @Test
    public void testAllWritableBucketsAndReadOnlyRouter() {
        List<BucketURI> buckets = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            buckets.add(BucketURI.parse(i + "@s3://bucket" + i));
        }
        List<Short> writers = new ArrayList<>();
        MultiBucketsRouterChannel router = new MultiBucketsRouterChannel(42, buckets, false, (bucket, readOnly) -> {
            if (!readOnly) {
                writers.add(bucket.bucketId());
            }
            return idleWal();
        });
        assertEquals(buckets.stream().map(BucketURI::bucketId).toList(), writers);
        router.close().join();
        MultiBucketsRouterChannel reader = new MultiBucketsRouterChannel(42,
            List.of(BucketURI.parse("0@s3://a?mode=r")), true,
            (bucket, readOnly) -> {
                assertTrue(readOnly);
                return idleWal();
            });
        assertTrue(reader.append(42, (short) 1, Unpooled.buffer().writeByte(1)).isCompletedExceptionally());
        reader.close().join();
    }

    /** Requests for different target nodes contribute to one size threshold and flush together. */
    @Test
    public void testSizeFlushIncludesAllTargetNodes() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=100&batchInterval=60000");
        CompletableFuture<RouterChannel.AppendResult> first = fixture.router.append(1, (short) 1, Unpooled.buffer().writeByte(1));
        assertEquals(0, fixture.flushed.size());
        CompletableFuture<RouterChannel.AppendResult> second = fixture.router.append(2, (short) 2, Unpooled.buffer().writeByte(2));
        Wal wal = fixture.nextFlush();
        assertEquals(2, wal.appends.size());
        wal.succeed(0, 10);
        wal.succeed(1, 20);
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        fixture.close();
    }

    /** After a size flush, a new partial batch is flushed by its own linger timer. */
    @Test
    public void testLingerTimerAfterSizeFlush() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=100&batchInterval=1000");
        fixture.router.append(1, (short) 1, Unpooled.buffer().writeByte(1));
        fixture.router.append(2, (short) 2, Unpooled.buffer().writeByte(2));
        Wal wal = fixture.nextFlush();
        assertEquals(2, wal.appends.size());
        wal.succeed(0, 10);
        wal.succeed(1, 20);
        CompletableFuture<RouterChannel.AppendResult> partial = fixture.router.append(3, (short) 3, Unpooled.buffer().writeByte(3));
        fixture.nextFlush().succeed(2, 30);
        partial.get(5, TimeUnit.SECONDS);
        fixture.close();
    }

    /** Negative order hints map into the fixed queue array and retain their ordering across buckets. */
    @Test
    public void testNegativeOrderHints() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1", "1@s3://b");
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append(Short.MIN_VALUE);
        Wal a = fixture.nextFlush();
        CompletableFuture<RouterChannel.AppendResult> second = fixture.append(Short.MIN_VALUE);
        Wal b = fixture.nextFlush();
        b.succeed(0, 20);
        assertFalse(second.isDone());
        a.succeed(0, 10);
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        fixture.close();
    }

    /** Distinct hints that map to one fixed queue share ordered completion across buckets. */
    @Test
    public void testOrderHintQueueCollision() throws Exception {
        Fixture fixture = new Fixture("0@s3://a?maxBytesInBatch=1", "1@s3://b");
        short firstHint = -1;
        short secondHint = 1;
        CompletableFuture<RouterChannel.AppendResult> first = fixture.append(firstHint);
        Wal a = fixture.nextFlush();
        CompletableFuture<RouterChannel.AppendResult> second = fixture.append(secondHint);
        Wal b = fixture.nextFlush();
        b.succeed(0, 20);
        assertFalse(second.isDone());
        a.succeed(0, 10);
        assertEquals(firstHint, ChannelOffset.of(first.get(5, TimeUnit.SECONDS).channelOffset()).orderHint());
        assertEquals(secondHint, ChannelOffset.of(second.get(5, TimeUnit.SECONDS).channelOffset()).orderHint());
        fixture.close();
    }

    private static ObjectWALService idleWal() {
        ObjectWALService wal = mock(ObjectWALService.class);
        when(wal.flush()).thenReturn(CompletableFuture.completedFuture(null));
        return wal;
    }

    private static class Fixture {
        final Map<Short, Wal> wals = new HashMap<>();
        final LinkedBlockingQueue<Wal> flushed = new LinkedBlockingQueue<>();
        final MultiBucketsRouterChannel router;

        Fixture(String... uris) {
            router = new MultiBucketsRouterChannel(42,
                java.util.Arrays.stream(uris).map(BucketURI::parse).toList(), false,
                (bucket, readOnly) -> {
                    Wal wal = new Wal(bucket.bucketId());
                    when(wal.mock.flush()).thenAnswer(invocation -> {
                        flushed.add(wal);
                        return CompletableFuture.completedFuture(null);
                    });
                    when(wal.mock.trim(any())).thenReturn(CompletableFuture.completedFuture(null));
                    wals.put(bucket.bucketId(), wal);
                    return wal.mock;
                });
        }

        CompletableFuture<RouterChannel.AppendResult> append(short hint) {
            return router.append(43, hint, Unpooled.buffer().writeByte(1));
        }

        Wal nextFlush() throws InterruptedException {
            Wal wal = flushed.poll(5, TimeUnit.SECONDS);
            assertNotNull(wal);
            return wal;
        }

        void close() throws Exception {
            router.close().get(5, TimeUnit.SECONDS);
        }
    }

    private static class Wal {
        final short id;
        final ObjectWALService mock = mock(ObjectWALService.class);
        final List<CompletableFuture<com.automq.stream.s3.wal.AppendResult>> appends = new ArrayList<>();

        Wal(short id) {
            this.id = id;
            try {
                when(mock.append(any(), any())).thenAnswer(invocation -> {
                    StreamRecordBatch record = invocation.getArgument(1);
                    record.release();
                    CompletableFuture<com.automq.stream.s3.wal.AppendResult> future = new CompletableFuture<>();
                    appends.add(future);
                    return future;
                });
            } catch (OverCapacityException error) {
                throw new AssertionError(error);
            }
        }

        void succeed(int index, long offset) {
            com.automq.stream.s3.wal.AppendResult result = mock(com.automq.stream.s3.wal.AppendResult.class);
            when(result.recordOffset()).thenReturn(DefaultRecordOffset.of(1, offset, 1));
            appends.get(index).complete(result);
        }
    }
}
