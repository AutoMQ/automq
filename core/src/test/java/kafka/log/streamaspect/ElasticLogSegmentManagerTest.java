/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.log.streamaspect;

import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.api.AppendResult;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class ElasticLogSegmentManagerTest {

    /** Given a pending delete, listeners only observe the updated cache after metadata acknowledgement. */
    @Test
    public void testDeleteNotifiesOnlyAfterPersistence() {
        MetaStream metaStream = mock(MetaStream.class);
        CompletableFuture<AppendResult> appendFuture = new CompletableFuture<>();
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(appendFuture);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticLogSegment oldSegment = segmentWithBaseOffset(0L);
        manager.put(0L, oldSegment);
        manager.put(10L, segmentWithBaseOffset(10L));
        ElasticLogMeta oldMeta = manager.logMeta();
        List<ElasticLogMeta> notifications = new ArrayList<>();
        manager.addLogEventListener(meta -> {
            assertSame(meta, manager.logMeta());
            notifications.add(meta);
        });

        manager.logSegmentEventListener().onEvent(0L, oldSegment, ElasticLogSegmentEvent.SEGMENT_DELETE);

        assertTrue(notifications.isEmpty());
        assertSame(oldMeta, manager.logMeta());
        appendFuture.complete(mock(AppendResult.class));
        assertEquals(1, notifications.size());
        assertEquals(List.of(10L), baseOffsets(notifications.get(0)));
    }

    /** Queued deletes must publish the final segment list even when no later append occurs. */
    @Test
    public void testQueuedDeletesNotifyEachAcknowledgedSnapshot() {
        MetaStream metaStream = mock(MetaStream.class);
        CompletableFuture<AppendResult> firstAppend = new DirectExecutorFuture<>();
        CompletableFuture<AppendResult> secondAppend = new CompletableFuture<>();
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(firstAppend, secondAppend);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticLogSegment first = segmentWithBaseOffset(0L);
        ElasticLogSegment second = segmentWithBaseOffset(10L);
        manager.put(0L, first);
        manager.put(10L, second);
        manager.put(20L, segmentWithBaseOffset(20L));
        manager.logMeta();
        kafka.cluster.LogEventListener listener = mock(kafka.cluster.LogEventListener.class);
        manager.addLogEventListener(listener);

        manager.logSegmentEventListener().onEvent(0L, first, ElasticLogSegmentEvent.SEGMENT_DELETE);
        manager.logSegmentEventListener().onEvent(10L, second, ElasticLogSegmentEvent.SEGMENT_DELETE);
        verifyNoInteractions(listener);
        firstAppend.complete(mock(AppendResult.class));
        verify(metaStream, times(2)).append(any(MetaKeyValue.class));
        secondAppend.complete(mock(AppendResult.class));

        ArgumentCaptor<ElasticLogMeta> snapshots = ArgumentCaptor.forClass(ElasticLogMeta.class);
        verify(listener, times(2)).onChange(snapshots.capture());
        assertEquals(Set.of(10L, 20L), new HashSet<>(baseOffsets(snapshots.getAllValues().get(0))));
        assertEquals(List.of(20L), baseOffsets(snapshots.getAllValues().get(1)));
        assertSame(snapshots.getAllValues().get(1), manager.logMeta());
    }

    /** A failed append must not notify listeners or replace the acknowledged snapshot. */
    @Test
    public void testFailedPersistenceDoesNotNotify() {
        MetaStream metaStream = mock(MetaStream.class);
        CompletableFuture<AppendResult> appendFuture = new CompletableFuture<>();
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(appendFuture);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticLogMeta oldMeta = manager.logMeta();
        kafka.cluster.LogEventListener listener = mock(kafka.cluster.LogEventListener.class);
        manager.addLogEventListener(listener);

        CompletableFuture<ElasticLogMeta> persistence = manager.asyncPersistLogMeta();
        appendFuture.completeExceptionally(new IllegalStateException("injected failure"));

        assertTrue(persistence.isCompletedExceptionally());
        assertSame(oldMeta, manager.logMeta());
        verifyNoInteractions(listener);
    }

    /** Listener failures must not fail acknowledged persistence or suppress other listeners. */
    @Test
    public void testListenerFailureDoesNotFailPersistence() {
        MetaStream metaStream = mock(MetaStream.class);
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        manager.addLogEventListener(meta -> {
            throw new IllegalStateException("injected listener failure");
        });
        kafka.cluster.LogEventListener listener = mock(kafka.cluster.LogEventListener.class);
        manager.addLogEventListener(listener);

        ElasticLogMeta meta = manager.persistLogMeta();

        assertSame(meta, manager.logMeta());
        verify(listener).onChange(meta);
    }

    /**
     * Given a controlled metadata append, the authoritative snapshot changes only after acknowledgement.
     */
    @Test
    public void testPendingPersistenceDoesNotPublishPreparedSnapshot() {
        MetaStream metaStream = mock(MetaStream.class);
        CompletableFuture<AppendResult> appendFuture = new CompletableFuture<>();
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(appendFuture);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticStreamSegmentMeta retainedMeta = new ElasticStreamSegmentMeta();
        retainedMeta.baseOffset(0L);
        retainedMeta.log(SliceRange.of(0L, 100L));
        ElasticLogSegment oldSegment = mock(ElasticLogSegment.class);
        when(oldSegment.meta()).thenReturn(retainedMeta);
        ElasticLogSegment laterSegment = segmentWithBaseOffset(5L);
        ElasticLogSegment newSegment = segmentWithBaseOffset(10L);
        manager.put(0L, oldSegment);
        manager.put(5L, laterSegment);
        ElasticLogMeta oldMeta = manager.logMeta();

        retainedMeta.log(SliceRange.of(0L, 50L));
        manager.remove(5L, laterSegment);
        manager.put(10L, newSegment);
        CompletableFuture<ElasticLogMeta> persistence = manager.asyncPersistLogMeta(false);

        assertSame(oldMeta, manager.logMeta());
        assertEquals(List.of(0L, 5L), baseOffsets(manager.logMeta()));
        assertEquals(100L, manager.logMeta().getSegmentMetas().get(0).log().end());
        appendFuture.complete(mock(AppendResult.class));
        assertEquals(List.of(0L, 10L), baseOffsets(persistence.join()));
        assertEquals(50L, persistence.join().getSegmentMetas().get(0).log().end());
        assertEquals(List.of(0L, 10L), baseOffsets(manager.logMeta()));
        verify(metaStream, times(1)).append(any(MetaKeyValue.class));
    }

    /**
     * Given an exceptional metadata append, the manager must keep the previously acknowledged snapshot authoritative.
     */
    @Test
    public void testFailedPersistenceKeepsAcknowledgedSnapshot() {
        MetaStream metaStream = mock(MetaStream.class);
        CompletableFuture<AppendResult> appendFuture = new CompletableFuture<>();
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(appendFuture);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticLogSegment oldSegment = segmentWithBaseOffset(0L);
        manager.put(0L, oldSegment);
        ElasticLogMeta oldMeta = manager.logMeta();
        manager.remove(0L, oldSegment);
        manager.put(10L, segmentWithBaseOffset(10L));

        CompletableFuture<ElasticLogMeta> persistence = manager.asyncPersistLogMeta(false);
        appendFuture.completeExceptionally(new IllegalStateException("injected failure"));

        assertTrue(persistence.isCompletedExceptionally());
        assertSame(oldMeta, manager.logMeta());
        assertEquals(List.of(0L), baseOffsets(manager.logMeta()));
    }

    /** A synchronous metadata append failure must not publish the prepared snapshot. */
    @Test
    public void testMetadataAppendFailureKeepsAcknowledgedSnapshot() {
        MetaStream metaStream = mock(MetaStream.class);
        when(metaStream.append(any(MetaKeyValue.class))).thenThrow(new IllegalStateException("injected failure"));
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        ElasticLogSegment oldSegment = segmentWithBaseOffset(0L);
        manager.put(0L, oldSegment);
        ElasticLogMeta oldMeta = manager.logMeta();
        manager.remove(0L, oldSegment);
        manager.put(10L, segmentWithBaseOffset(10L));

        assertThrows(IllegalStateException.class, () -> manager.asyncPersistLogMeta(false));
        assertSame(oldMeta, manager.logMeta());
        assertEquals(List.of(0L), baseOffsets(manager.logMeta()));
    }

    /** In-memory mutations must not publish intermediate metadata snapshots. */
    @Test
    public void testMutationDoesNotNotifyBeforePersistence() {
        MetaStream metaStream = mock(MetaStream.class);
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(metaStream, streamManager, "test");
        kafka.cluster.LogEventListener listener = mock(kafka.cluster.LogEventListener.class);
        manager.addLogEventListener(listener);
        ElasticLogSegment segment = segmentWithBaseOffset(0L);

        manager.put(0L, segment);
        manager.remove(0L, segment);

        verifyNoInteractions(listener);
    }

    /** Keeps queue-drain callbacks deterministic instead of relying on the shared common pool. */
    private static class DirectExecutorFuture<T> extends CompletableFuture<T> {
        @Override
        public Executor defaultExecutor() {
            return Runnable::run;
        }

        @Override
        public <U> CompletableFuture<U> newIncompleteFuture() {
            return new DirectExecutorFuture<>();
        }
    }

    private static ElasticLogSegment segmentWithBaseOffset(long baseOffset) {
        ElasticLogSegment segment = mock(ElasticLogSegment.class);
        ElasticStreamSegmentMeta meta = new ElasticStreamSegmentMeta();
        meta.baseOffset(baseOffset);
        when(segment.meta()).thenReturn(meta);
        return segment;
    }

    private static List<Long> baseOffsets(ElasticLogMeta meta) {
        return meta.getSegmentMetas().stream().map(ElasticStreamSegmentMeta::baseOffset).toList();
    }

    /**
     * Given a live version supplier, the next normal persistence must switch from JSON to the V6 envelope.
     */
    @Test
    public void testPersistReadsAutoMQVersionDynamically() {
        MetaStream metaStream = mock(MetaStream.class);
        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));
        ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
        when(streamManager.streams()).thenReturn(Map.of());
        AtomicReference<AutoMQVersion> version = new AtomicReference<>(AutoMQVersion.V5);
        ElasticLogSegmentManager manager = new ElasticLogSegmentManager(
            metaStream, streamManager, "testPersistReadsAutoMQVersionDynamically", version::get);

        manager.persistLogMeta();
        version.set(AutoMQVersion.V6);
        manager.persistLogMeta();

        ArgumentCaptor<MetaKeyValue> values = ArgumentCaptor.forClass(MetaKeyValue.class);
        verify(metaStream, times(2)).append(values.capture());
        assertNotEquals(ElasticLogMetaCodec.MAGIC, values.getAllValues().get(0).getValue().getInt());
        assertEquals(ElasticLogMetaCodec.MAGIC, values.getAllValues().get(1).getValue().getInt());
        assertEquals(MetaStream.LOG_META_KEY, values.getAllValues().get(0).getKey());
        assertEquals(MetaStream.LOG_META_KEY, values.getAllValues().get(1).getKey());
    }

    /**
     * Given a default manager created before ElasticLogManager initialization, its writer policy must become V6
     * after the live manager is published.
     */
    @Test
    public void testDefaultManagerObservesLateElasticLogManagerInitialization() {
        Option<ElasticLogManager> originalManager = ElasticLogManager$.MODULE$.INSTANCE();
        try {
            ElasticLogManager$.MODULE$.INSTANCE_$eq(Option.empty());
            MetaStream metaStream = mock(MetaStream.class);
            when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));
            ElasticLogStreamManager streamManager = mock(ElasticLogStreamManager.class);
            when(streamManager.streams()).thenReturn(Map.of());
            ElasticLogSegmentManager manager = new ElasticLogSegmentManager(
                metaStream, streamManager, "testDefaultManagerObservesLateElasticLogManagerInitialization");

            ElasticLogManager liveManager = new ElasticLogManager(
                null, null, () -> AutoMQVersion.V6);
            ElasticLogManager$.MODULE$.INSTANCE_$eq(Option.apply(liveManager));
            manager.persistLogMeta();

            ArgumentCaptor<MetaKeyValue> value = ArgumentCaptor.forClass(MetaKeyValue.class);
            verify(metaStream).append(value.capture());
            assertEquals(ElasticLogMetaCodec.MAGIC, value.getValue().getValue().getInt());
        } finally {
            ElasticLogManager$.MODULE$.INSTANCE_$eq(originalManager);
        }
    }

    @Test
    public void testSegmentDelete() {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        ElasticLogSegment logSegment = mock(ElasticLogSegment.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));
        manager.put(1, logSegment);

        doReturn(CompletableFuture.completedFuture(logMeta)).when(manager).asyncPersistLogMeta();

        ElasticLogSegmentManager.EventListener listener = manager.new EventListener();

        // mismatch
        listener.onEvent(1, mock(ElasticLogSegment.class), ElasticLogSegmentEvent.SEGMENT_DELETE);
        assertEquals(1, manager.segments.size());

        // match
        listener.onEvent(1, logSegment, ElasticLogSegmentEvent.SEGMENT_DELETE);
        assertEquals(0, manager.segments.size());

        verify(manager, atLeastOnce()).asyncPersistLogMeta();
        verify(manager, atMost(2)).asyncPersistLogMeta();
    }

    @Test
    public void testLargeScaleSegmentDelete() throws InterruptedException {
        ElasticLogMeta logMeta = mock(ElasticLogMeta.class);
        MetaStream metaStream = mock(MetaStream.class);

        when(metaStream.append(any(MetaKeyValue.class))).thenReturn(CompletableFuture.completedFuture(null));

        ElasticLogStreamManager elasticLogStreamManager = mock(ElasticLogStreamManager.class);

        ElasticLogSegmentManager manager = spy(new ElasticLogSegmentManager(metaStream, elasticLogStreamManager, "testLargeScaleSegmentDelete"));
        List<ElasticLogSegment> segments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ElasticLogSegment segment = mock(ElasticLogSegment.class);
            manager.put(i, segment);
            segments.add(segment);
        }

        Set<Long> removedSegmentId = new HashSet<>();

        when(manager.remove(anyLong(), any())).thenAnswer(invocation -> {
            long id = invocation.getArgument(0);
            removedSegmentId.add(id);
            return invocation.callRealMethod();
        });

        CountDownLatch latch = new CountDownLatch(2);

        doAnswer(invocation -> {
            CompletableFuture<Object> cf = new CompletableFuture<>()
                .completeOnTimeout(logMeta, 100, TimeUnit.MILLISECONDS);

            cf.whenComplete((res, e) -> {
                latch.countDown();
            });

            return cf;
        }).when(manager).asyncPersistLogMeta();

        ElasticLogSegmentManager.EventListener listener = spy(manager.new EventListener());

        for (int i = 0; i < 10; i++) {
            listener.onEvent(i, segments.get(i), ElasticLogSegmentEvent.SEGMENT_DELETE);
        }

        latch.await();

        // expect the first and the tail should call the persist method.
        verify(manager, times(2)).asyncPersistLogMeta();

        // check all segmentId removed.
        for (long i = 0; i < 10L; i++) {
            assertTrue(removedSegmentId.contains(i));
        }

        // the request can be finished.
        CompletableFuture<ElasticLogMeta> pendingPersistentMetaCf = listener.getPendingPersistentMetaCf();
        pendingPersistentMetaCf.join();

        // all the queue can be removed.
        assertTrue(listener.getPendingDeleteSegmentQueue().isEmpty());

    }
}
