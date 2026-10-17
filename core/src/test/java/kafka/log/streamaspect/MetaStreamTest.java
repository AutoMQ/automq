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

package kafka.log.streamaspect;

import kafka.log.streamaspect.reassignment.FastPartitionReassignmentManager;
import kafka.log.streamaspect.reassignment.MetaStreamHandoff;
import kafka.log.streamaspect.reassignment.MetaStreamHandoffRecord;
import kafka.log.streamaspect.reassignment.PartitionHandoff;
import kafka.log.streamaspect.reassignment.PartitionHandoffCache;
import kafka.log.streamaspect.reassignment.PartitionHandoffSendException;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.DefaultRecordBatch;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies MetaStream freeze, snapshot, and replay lifecycle behavior. */
@Tag("S3Unit")
public class MetaStreamTest {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    public void tearDown() {
        scheduler.shutdownNow();
    }

    /**
     * Given a closed MetaStream, when append or trim is attempted, then each attempt fails visibly.
     */
    @Test
    public void testFrozenMetaStreamRejectsMutations() {
        MetaStream metaStream = ordinaryMetaStream(new MemoryClient.StreamImpl(1L));

        metaStream.close().join();

        assertFrozenFailure(() -> metaStream.append(MetaKeyValue.of("key", ByteBuffer.wrap(new byte[] {1}))).join());
        assertFrozenFailure(() -> metaStream.trim(1L).join());
    }

    /**
     * Given overwritten and unknown metadata keys, when freezing, then the handoff keeps only each latest raw record.
     */
    @Test
    public void testFreezeBuildsLatestKeyValueHandoffWithOriginalRecords() {
        AtomicReference<PartitionHandoff> sent = new AtomicReference<>();
        MetaStream metaStream = fastMetaStream(new MemoryClient.StreamImpl(1L), sent, true);
        MetaKeyValue oldLog = MetaKeyValue.of(MetaStream.LOG_META_KEY, ByteBuffer.wrap(new byte[] {1}));
        MetaKeyValue unknown = MetaKeyValue.of("UNKNOWN_FUTURE_KEY", ByteBuffer.wrap(new byte[] {2, 3}));
        MetaKeyValue latestLog = MetaKeyValue.of(MetaStream.LOG_META_KEY, ByteBuffer.wrap(new byte[] {4, 5, 6}));
        metaStream.append(oldLog).join();
        metaStream.append(unknown).join();
        metaStream.append(latestLog).join();

        metaStream.close(true).join();
        MetaStreamHandoff handoff = sent.get().metaStreamHandoff();

        assertEquals(3L, handoff.endOffset());
        List<MetaStreamHandoffRecord> records = handoff.records();
        assertEquals(2, records.size());
        assertEquals(1L, records.get(0).baseOffset());
        assertArrayEquals(bytes(MetaKeyValue.encode(unknown)), bytes(records.get(0).encodedMetaKeyValue()));
        assertEquals(2L, records.get(1).baseOffset());
        assertArrayEquals(bytes(MetaKeyValue.encode(latestLog)), bytes(records.get(1).encodedMetaKeyValue()));
    }

    /**
     * Given replayed metadata history, when freezing, then reduction does not rewrite selected offsets or advance end.
     */
    @Test
    public void testFreezeReducesReplayedRecordsWithoutRewritingOffsets() throws Exception {
        MemoryClient.StreamImpl innerStream = new MemoryClient.StreamImpl(1L);
        MetaKeyValue oldA = MetaKeyValue.of("A", ByteBuffer.wrap(new byte[] {1}));
        MetaKeyValue latestB = MetaKeyValue.of("B", ByteBuffer.wrap(new byte[] {2}));
        MetaKeyValue latestA = MetaKeyValue.of("A", ByteBuffer.wrap(new byte[] {3}));
        appendRaw(innerStream, oldA);
        appendRaw(innerStream, latestB);
        appendRaw(innerStream, latestA);
        AtomicReference<PartitionHandoff> sent = new AtomicReference<>();
        MetaStream metaStream = fastMetaStream(innerStream, sent, true);
        metaStream.replay();

        metaStream.close(true).join();
        MetaStreamHandoff handoff = sent.get().metaStreamHandoff();

        assertEquals(3L, handoff.endOffset());
        assertEquals(2, handoff.records().size());
        assertEquals(1L, handoff.records().get(0).baseOffset());
        assertArrayEquals(bytes(MetaKeyValue.encode(latestB)),
            bytes(handoff.records().get(0).encodedMetaKeyValue()));
        assertEquals(2L, handoff.records().get(1).baseOffset());
        assertArrayEquals(bytes(MetaKeyValue.encode(latestA)),
            bytes(handoff.records().get(1).encodedMetaKeyValue()));
    }

    /**
     * Given a complete handoff with a known and an unknown key, when replay succeeds, then both values are published
     * together and the unknown value remains opaque.
     */
    @Test
    public void testHandoffReplayPublishesCompleteTemporaryState() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        ElasticPartitionMeta partitionMeta = new ElasticPartitionMeta(11L, 12L, 13L);
        MetaKeyValue known = MetaKeyValue.of(
            MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta));
        MetaKeyValue unknown = MetaKeyValue.of("UNKNOWN_FUTURE_KEY", ByteBuffer.wrap(new byte[] {7, 8}));
        MetaStreamHandoff handoff = new MetaStreamHandoff(9L, List.of(
            new MetaStreamHandoffRecord(4L, MetaKeyValue.encode(known)),
            new MetaStreamHandoffRecord(8L, MetaKeyValue.encode(unknown))));
        PartitionHandoffCache cache = new PartitionHandoffCache();
        FastPartitionReassignmentManager manager = manager(cache);
        manager.receive(List.of(new PartitionHandoff(topicId, 0, handoff)));
        MemoryClient.StreamImpl innerStream = new MemoryClient.StreamImpl(1L);
        innerStream.confirmOffset(handoff.endOffset());
        MetaStream metaStream = new MetaStream(innerStream, scheduler, "test", topicId, 0, manager);

        Map<String, Object> replayed = metaStream.replay();

        ElasticPartitionMeta replayedPartition = (ElasticPartitionMeta) replayed.get(MetaStream.PARTITION_META_KEY);
        assertEquals(11L, replayedPartition.getStartOffset());
        assertEquals(12L, replayedPartition.getCleanerOffset());
        assertEquals(13L, replayedPartition.getRecoverOffset());
        assertArrayEquals(new byte[] {7, 8}, bytes((ByteBuffer) replayed.get("UNKNOWN_FUTURE_KEY")));
        assertTrue(metaStream.get(MetaStream.PARTITION_META_KEY).isPresent());
        assertArrayEquals(new byte[] {7, 8}, bytes(metaStream.get("UNKNOWN_FUTURE_KEY").orElseThrow()));
    }

    /**
     * Given V6 ElasticLog metadata in the stream, replay must decode the envelope without a version-side channel.
     */
    @Test
    public void testReplayDecodesV6ElasticLogMeta() throws Exception {
        ElasticLogMeta expected = new ElasticLogMeta();
        expected.setStreamMap(Map.of("log0", 42L));
        MemoryClient.StreamImpl innerStream = new MemoryClient.StreamImpl(1L);
        appendRaw(innerStream, MetaKeyValue.of(MetaStream.LOG_META_KEY,
            ElasticLogMetaCodec.encode(expected, AutoMQVersion.V6)));
        MetaStream metaStream = ordinaryMetaStream(innerStream);

        Map<String, Object> replayed = metaStream.replay();

        ElasticLogMeta actual = (ElasticLogMeta) replayed.get(MetaStream.LOG_META_KEY);
        assertEquals(expected.getStreamMap(), actual.getStreamMap());
        assertEquals(0, actual.getSegmentMetas().size());
    }

    /**
     * Given a handoff whose later known value cannot be applied, when replay fails, then no earlier temporary value is
     * published to the formal MetaStream state.
     */
    @Test
    public void testFailedHandoffReplayPublishesNoPartialState() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        MetaKeyValue unknown = MetaKeyValue.of("UNKNOWN_FUTURE_KEY", ByteBuffer.wrap(new byte[] {7, 8}));
        MetaKeyValue invalidPartition = MetaKeyValue.of(
            MetaStream.PARTITION_META_KEY, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        MetaStreamHandoff handoff = new MetaStreamHandoff(2L, List.of(
            new MetaStreamHandoffRecord(0L, MetaKeyValue.encode(unknown)),
            new MetaStreamHandoffRecord(1L, MetaKeyValue.encode(invalidPartition))));
        PartitionHandoffCache cache = new PartitionHandoffCache();
        FastPartitionReassignmentManager manager = manager(cache);
        manager.receive(List.of(new PartitionHandoff(topicId, 0, handoff)));
        MemoryClient.StreamImpl innerStream = new MemoryClient.StreamImpl(1L);
        appendRaw(innerStream, MetaKeyValue.of("FALLBACK_A", ByteBuffer.wrap(new byte[] {3})));
        appendRaw(innerStream, MetaKeyValue.of("FALLBACK_B", ByteBuffer.wrap(new byte[] {4})));
        MetaStream metaStream = new MetaStream(innerStream, scheduler, "test", topicId, 0, manager);

        Map<String, Object> replayed = metaStream.replay();

        assertArrayEquals(new byte[] {3}, bytes((ByteBuffer) replayed.get("FALLBACK_A")));
        assertArrayEquals(new byte[] {4}, bytes((ByteBuffer) replayed.get("FALLBACK_B")));
        assertTrue(metaStream.get("UNKNOWN_FUTURE_KEY").isEmpty());
        assertTrue(metaStream.get(MetaStream.PARTITION_META_KEY).isEmpty());
    }

    /**
     * Given an admitted append at the freeze boundary, when later mutations race with freeze, then freeze drains only
     * the admitted append and captures its record and end offset together.
     */
    @Test
    public void testFreezeDrainsAdmittedAppendAndRejectsConcurrentMutations() {
        BlockingAppendStream innerStream = new BlockingAppendStream(1L);
        AtomicReference<PartitionHandoff> sent = new AtomicReference<>();
        MetaStream metaStream = fastMetaStream(innerStream, sent, true);
        MetaKeyValue admitted = MetaKeyValue.of("key", ByteBuffer.wrap(new byte[] {7}));
        metaStream.append(admitted);

        CompletableFuture<Void> closeFuture = metaStream.close(true);

        assertFalse(closeFuture.isDone());
        assertFrozenFailure(() -> metaStream.append(MetaKeyValue.of("late", ByteBuffer.wrap(new byte[] {8}))).join());
        assertFrozenFailure(() -> metaStream.trim(1L).join());
        innerStream.completeAppend();

        closeFuture.join();
        MetaStreamHandoff handoff = sent.get().metaStreamHandoff();
        assertEquals(1L, handoff.endOffset());
        assertEquals(1, handoff.records().size());
        assertEquals(0L, handoff.records().get(0).baseOffset());
        assertArrayEquals(bytes(MetaKeyValue.encode(admitted)),
            bytes(handoff.records().get(0).encodedMetaKeyValue()));
    }

    /**
     * Given an admitted mutation fails while close drains it, when close continues, then handoff is skipped and the
     * MetaStream remains terminally non-writable.
     */
    @Test
    public void testFailedCloseDrainFallsBackAndRemainsNonWritable() {
        BlockingAppendStream innerStream = new BlockingAppendStream(1L);
        AtomicReference<PartitionHandoff> sent = new AtomicReference<>();
        MetaStream metaStream = fastMetaStream(innerStream, sent, true);
        CompletableFuture<AppendResult> appendFuture =
            metaStream.append(MetaKeyValue.of("key", ByteBuffer.wrap(new byte[] {1})));
        CompletableFuture<Void> closeFuture = metaStream.close(true);

        innerStream.failAppend();

        assertThrows(CompletionException.class, appendFuture::join);
        closeFuture.join();
        assertTrue(sent.get() == null);
        assertFrozenFailure(() -> metaStream.append(MetaKeyValue.of("late", ByteBuffer.wrap(new byte[] {2}))).join());
        assertFrozenFailure(() -> metaStream.trim(1L).join());
    }

    /** Repeated close calls share one lifecycle attempt and one result. */
    @Test
    public void testReassignmentCloseIsMemoizedAtPublicBoundary() {
        CountingCloseStream innerStream = new CountingCloseStream(1L);
        AtomicInteger sendAttempts = new AtomicInteger();
        FastPartitionReassignmentManager manager = manager(new PartitionHandoffCache(), (target, handoff) -> {
            sendAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        MetaStream metaStream = new MetaStream(
            innerStream, scheduler, "test", Uuid.ZERO_UUID, 0, manager);

        CompletableFuture<Void> first = metaStream.close(true);
        CompletableFuture<Void> second = metaStream.close(true);

        assertSame(first, second);
        first.join();
        assertEquals(1, sendAttempts.get());
        assertEquals(1, innerStream.closeAttempts.get());
        assertTrue(metaStream.isFenced());
    }

    /** Send fallback force-compacts metadata history before closing the inner stream. */
    @Test
    public void testSendFallbackForceCompactsBeforeClose() throws Exception {
        MemoryClient.StreamImpl innerStream = new MemoryClient.StreamImpl(1L);
        MetaKeyValue oldA = MetaKeyValue.of("A", ByteBuffer.wrap(new byte[] {1}));
        MetaKeyValue latestB = MetaKeyValue.of("B", ByteBuffer.wrap(new byte[] {2}));
        MetaKeyValue latestA = MetaKeyValue.of("A", ByteBuffer.wrap(new byte[] {3}));
        appendRaw(innerStream, oldA);
        appendRaw(innerStream, latestB);
        appendRaw(innerStream, latestA);
        MetaStream metaStream = fastMetaStream(innerStream, new AtomicReference<>(), false);
        metaStream.replay();

        metaStream.close(true).join();

        assertEquals(4L, innerStream.nextOffset());
        assertEquals(2, innerStream.fetch(null, 2L, 4L, 1024).join().recordBatchList().size());
    }

    private MetaStream ordinaryMetaStream(Stream innerStream) {
        return new MetaStream(innerStream, scheduler, "test", Uuid.ZERO_UUID, -1,
            FastPartitionReassignmentManager.disabled());
    }

    private MetaStream fastMetaStream(
        Stream innerStream,
        AtomicReference<PartitionHandoff> sent,
        boolean sendSucceeds
    ) {
        FastPartitionReassignmentManager manager = manager(new PartitionHandoffCache(), (target, handoff) -> {
            sent.set(handoff);
            return sendSucceeds
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(new PartitionHandoffSendException(
                    PartitionHandoffSendException.Reason.SEND_FAILURE));
        });
        return new MetaStream(innerStream, scheduler, "test", Uuid.randomUuid(), 0, manager);
    }

    private FastPartitionReassignmentManager manager(PartitionHandoffCache cache) {
        return manager(cache, (target, handoff) -> CompletableFuture.completedFuture(null));
    }

    private FastPartitionReassignmentManager manager(
        PartitionHandoffCache cache,
        BiFunction<Node, PartitionHandoff, CompletableFuture<Void>> sender
    ) {
        return new FastPartitionReassignmentManager() {
            @Override
            public CompletableFuture<Void> send(PartitionHandoff handoff) {
                return sender.apply(new Node(2, "target", 9092), handoff);
            }

            @Override
            public void receive(Collection<PartitionHandoff> handoffs) {
                cache.putAll(handoffs);
            }

            @Override
            public Optional<PartitionHandoff> take(PartitionHandoff.Key key) {
                return cache.take(key);
            }

            @Override
            public void close() {
                cache.clear();
            }
        };
    }

    private static void assertFrozenFailure(Runnable operation) {
        CompletionException exception = assertThrows(CompletionException.class, operation::run);
        assertInstanceOf(IllegalStateException.class, exception.getCause());
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    private static void appendRaw(MemoryClient.StreamImpl stream, MetaKeyValue keyValue) {
        stream.append(new DefaultRecordBatch(
            1, 0L, Collections.emptyMap(), MetaKeyValue.encode(keyValue))).join();
    }

    private static final class BlockingAppendStream extends MemoryClient.StreamImpl {
        private final AtomicLong nextOffset = new AtomicLong();
        private final CompletableFuture<AppendResult> appendFuture = new CompletableFuture<>();

        private BlockingAppendStream(long streamId) {
            super(streamId);
        }

        @Override
        public long nextOffset() {
            return nextOffset.get();
        }

        @Override
        public synchronized CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
            long baseOffset = nextOffset.getAndAdd(recordBatch.count());
            return appendFuture.thenApply(nil -> () -> baseOffset);
        }

        private void completeAppend() {
            appendFuture.complete(() -> 0L);
        }

        private void failAppend() {
            appendFuture.completeExceptionally(new IllegalStateException("append failed"));
        }
    }

    private static final class CountingCloseStream extends MemoryClient.StreamImpl {
        private final AtomicInteger closeAttempts = new AtomicInteger();

        private CountingCloseStream(long streamId) {
            super(streamId);
        }

        @Override
        public CompletableFuture<Void> close() {
            closeAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }
}
