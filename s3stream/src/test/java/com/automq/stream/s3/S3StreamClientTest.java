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

package com.automq.stream.s3;

import com.automq.stream.Version;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.compact.StreamObjectArchiveTask;
import com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType;
import com.automq.stream.s3.exceptions.CompactedObjectsNotFoundException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR_V1;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class S3StreamClientTest {
    private S3StreamClient client;
    private StreamManager streamManager;
    private Storage storage;
    private ScheduledExecutorService scheduler;
    private ThreadPoolExecutor compactionExecutor;
    private ObjectManager objectManager;
    private Config config;
    private LinkedBlockingQueue<Runnable> compactionTasks;

    @BeforeEach
    void setup() throws Exception {
        streamManager = mock(StreamManager.class);
        storage = mock(Storage.class);
        when(storage.awaitUpload(anyLong())).thenReturn(CompletableFuture.completedFuture(null));
        objectManager = mock(ObjectManager.class);
        when(objectManager.getObjectsCount()).thenReturn(CompletableFuture.completedFuture(0));
        config = new Config().version(() -> Version.V1);
        compactionExecutor = mock(ThreadPoolExecutor.class);
        compactionTasks = new LinkedBlockingQueue<>();
        when(compactionExecutor.getQueue()).thenReturn(compactionTasks);
        doAnswer(args -> {
            compactionTasks.add(args.getArgument(0));
            return null;
        }).when(compactionExecutor).execute(any(Runnable.class));
        when(compactionExecutor.awaitTermination(anyLong(), any())).thenReturn(true);
        client = spy(new S3StreamClient(streamManager, storage, objectManager, mock(ObjectStorage.class),
            config, null, null, compactionExecutor));
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void cleanup() {
        scheduler.shutdown();
        client.shutdown();
    }

    @Test
    public void testShutdown_withOpeningStream() {
        S3Stream stream = mock(S3Stream.class);
        when(stream.close(anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(1L);

        CompletableFuture<StreamMetadata> cf = new CompletableFuture<>();
        when(streamManager.openStream(anyLong(), anyLong(), anyMap())).thenReturn(cf);

        doAnswer(args -> stream).when(client).newStream(any(), any());

        scheduler.schedule(() -> {
            cf.complete(new StreamMetadata(1, 2, 100, 200, StreamState.OPENED));
        }, 100, MILLISECONDS);

        client.openStream(1, OpenStreamOptions.builder().build());
        client.shutdown();

        verify(stream, times(1)).close(anyBoolean());
        assertEquals(0, client.openingStreams.size());
        assertEquals(0, client.openedStreams.size());
        assertEquals(0, client.closingStreams.size());
    }

    /**
     * Given a local historical upload in progress, when the same stream is reopened,
     * then Controller open starts only after the upload barrier completes.
     */
    @Test
    public void testOpenWaitsForLocalUpload() {
        CompletableFuture<Void> upload = new CompletableFuture<>();
        when(storage.awaitUpload(1L)).thenReturn(upload);
        when(streamManager.openStream(1L, 2L, Map.of())).thenReturn(CompletableFuture.completedFuture(
            new StreamMetadata(1L, 2L, 100L, 200L, StreamState.OPENED)));

        CompletableFuture<Stream> open = client.openStream(1L, OpenStreamOptions.builder().epoch(2L).build());

        assertFalse(open.isDone());
        verify(streamManager, never()).openStream(anyLong(), anyLong(), anyMap());

        upload.complete(null);

        assertTrue(open.isDone());
        verify(streamManager).openStream(1L, 2L, Map.of());
    }

    /**
     * Given a failed local historical upload, when the same stream is reopened,
     * then the failure reaches the caller and Controller open is not attempted.
     */
    @Test
    public void testUploadFailurePreventsOpen() {
        RuntimeException uploadFailure = new RuntimeException("upload failed");
        when(storage.awaitUpload(1L)).thenReturn(CompletableFuture.failedFuture(uploadFailure));

        CompletableFuture<Stream> open = client.openStream(1L, OpenStreamOptions.builder().epoch(2L).build());

        ExecutionException exception = assertThrows(ExecutionException.class, open::get);
        assertEquals(uploadFailure, exception.getCause());
        verify(streamManager, never()).openStream(anyLong(), anyLong(), anyMap());
    }

    /**
     * Given the cluster object count approaches the hard major-v1 threshold,
     * when deciding whether to run major-v1 compaction, then the soft threshold triggers early.
     */
    @Test
    public void testMajorV1CompactionTriggeredAtSoftObjectThreshold() {
        int hardThreshold = 100;
        assertFalse(S3StreamClient.shouldRunMajorV1CompactionByObjectCount(89, hardThreshold));
        assertTrue(S3StreamClient.shouldRunMajorV1CompactionByObjectCount(90, hardThreshold));
        assertTrue(S3StreamClient.shouldRunMajorV1CompactionByObjectCount(100, hardThreshold));
    }

    /**
     * Given a scheduled MAJOR under pressure, its attempt also satisfies the pending pressure request.
     */
    @Test
    public void testScheduledMajorSatisfiesObjectCountPressure() {
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        S3StreamClient.CompactionHint pressure = new S3StreamClient.CompactionHint(Integer.MAX_VALUE);
        doNothing().when(stream).compact(MAJOR_V1, pressure);
        doNothing().when(stream).compact(CLEANUP_V1, pressure);

        stream.compact(pressure, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(2));
        compactionTasks.remove().run();
        verify(stream).compact(MAJOR_V1, pressure);

        stream.compact(pressure, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
        compactionTasks.remove().run();
        verify(stream).compact(CLEANUP_V1, pressure);
        verify(stream, times(1)).compact(MAJOR_V1, pressure);
    }

    /**
     * Given a queued or running task, repeated scans must not submit another task for the same stream.
     */
    @Test
    public void testCompactionDeduplicatesQueuedAndRunningTasks() {
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        long now = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(0);
        doAnswer(args -> {
            stream.compact(hint, now);
            assertTrue(compactionTasks.isEmpty());
            return null;
        }).when(stream).compact(CLEANUP_V1, hint);
        stream.compact(hint, now);
        Runnable task = compactionTasks.peek();
        stream.compact(hint, now);
        assertSame(task, compactionTasks.peek());
        assertEquals(1, compactionTasks.size());
        compactionTasks.remove().run();
        stream.compact(hint, now);
        assertEquals(1, compactionTasks.size());
    }

    /**
     * Given pressure, MINOR runs alone and a later scan runs pending MAJOR even after pressure falls.
     */
    @Test
    public void testPendingMajorRunsOnLaterScan() {
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        long now = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(11);
        S3StreamClient.CompactionHint pressure = new S3StreamClient.CompactionHint(Integer.MAX_VALUE);
        S3StreamClient.CompactionHint normal = new S3StreamClient.CompactionHint(0);
        doNothing().when(stream).compact(MINOR_V1, pressure);
        doNothing().when(stream).compact(MAJOR_V1, normal);
        stream.compact(pressure, now);
        compactionTasks.remove().run();
        assertTrue(compactionTasks.isEmpty());
        verify(stream, never()).compact(MAJOR_V1, pressure);

        stream.compact(normal, now);
        compactionTasks.remove().run();
        verify(stream).compact(MAJOR_V1, normal);

        doNothing().when(stream).compact(CLEANUP_V1, normal);
        stream.compact(normal, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
        compactionTasks.remove().run();
        verify(stream).compact(CLEANUP_V1, normal);
    }

    /**
     * Given a real compactor failure, release the task without immediate retry and honor the normal interval.
     */
    @Test
    public void testCompactorFailureWaitsForNormalSchedule() {
        S3Stream stream = mock(S3Stream.class);
        when(stream.streamId()).thenReturn(1L);
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new CompactedObjectsNotFoundException()))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        S3StreamClient.StreamWrapper.Compaction wrapper = spy(client.new StreamWrapper(stream).compaction);
        S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(0);
        long now = System.currentTimeMillis();
        wrapper.compact(hint, now + TimeUnit.HOURS.toMillis(2));
        compactionTasks.remove().run();
        assertTrue(compactionTasks.isEmpty());

        wrapper.compact(hint, now + TimeUnit.MINUTES.toMillis(2));
        compactionTasks.remove().run();
        verify(wrapper).compact(CLEANUP_V1, hint);
        verify(wrapper).compact(MAJOR_V1, hint);

        wrapper.compact(hint, now + TimeUnit.HOURS.toMillis(2));
        compactionTasks.remove().run();
        verify(wrapper, times(2)).compact(MAJOR_V1, hint);
        verify(objectManager, times(3)).getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt());
    }

    /**
     * Given a stream closed while queued, its task must finish without touching object storage.
     */
    @Test
    public void testClosedStreamSkipsQueuedCompaction() {
        S3Stream underlying = mock(S3Stream.class);
        S3StreamClient.StreamWrapper.Compaction stream = spy(client.new StreamWrapper(underlying).compaction);
        stream.compact(new S3StreamClient.CompactionHint(0), System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
        when(underlying.isClosed()).thenReturn(true);
        compactionTasks.remove().run();
        verify(stream, never()).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
    }

    /**
     * Given submission rejection or an unexpected worker exception, a later scan can retry the stream.
     */
    @Test
    public void testSubmissionAndWorkerFailuresReleaseState() {
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        long now = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(0);
        AtomicBoolean reject = new AtomicBoolean(true);
        doAnswer(args -> {
            if (reject.getAndSet(false)) {
                throw new java.util.concurrent.RejectedExecutionException();
            }
            compactionTasks.add(args.getArgument(0));
            return null;
        }).when(compactionExecutor).execute(any(Runnable.class));
        stream.compact(hint, now);
        doAnswer(args -> {
            throw new IllegalStateException("worker failure");
        }).when(stream).compact(CLEANUP_V1, hint);
        stream.compact(hint, now);
        compactionTasks.remove().run();
        doNothing().when(stream).compact(CLEANUP_V1, hint);
        stream.compact(hint, now);
        compactionTasks.remove().run();
    }

    /**
     * Given queued compactions, shutdown discards them before waiting for running work without interrupting workers.
     */
    @Test
    public void testShutdownDiscardsQueuedCompactions() throws Exception {
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        stream.compact(new S3StreamClient.CompactionHint(0), System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
        when(compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)).thenAnswer(args -> {
            verify(compactionExecutor, atLeastOnce()).shutdown();
            assertTrue(compactionTasks.isEmpty());
            return false;
        });
        client.shutdown();
        verify(compactionExecutor, never()).shutdownNow();
        verify(stream, never()).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
    }

    /**
     * Given a running and a queued compaction, shutdown discards queued work and lets running work finish normally.
     */
    @Test
    public void testShutdownWaitsForRunningCompaction() throws Exception {
        CountDownLatch queueCleared = new CountDownLatch(1);
        LinkedBlockingQueue<Runnable> queue = spy(new LinkedBlockingQueue<>());
        doAnswer(args -> {
            args.callRealMethod();
            queueCleared.countDown();
            return null;
        }).when(queue).clear();
        ThreadPoolExecutor workers = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        S3StreamClient concurrentClient = new S3StreamClient(streamManager, mock(Storage.class), objectManager,
            mock(ObjectStorage.class), config, null, null, workers);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        S3StreamClient.StreamWrapper.Compaction running = spy(concurrentClient.new StreamWrapper(mock(S3Stream.class)).compaction);
        S3StreamClient.StreamWrapper.Compaction queued = spy(concurrentClient.new StreamWrapper(mock(S3Stream.class)).compaction);
        doAnswer(args -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(running).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
        try {
            long now = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
            S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(0);
            running.compact(hint, now);
            assertTrue(started.await(5, TimeUnit.SECONDS));
            queued.compact(hint, now);
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(concurrentClient::shutdown);
            assertTrue(queueCleared.await(5, TimeUnit.SECONDS));
            assertTrue(workers.getQueue().isEmpty());
            assertFalse(shutdown.isDone());
            release.countDown();
            shutdown.get(5, TimeUnit.SECONDS);
            assertFalse(interrupted.get());
            verify(queued, never()).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
        } finally {
            release.countDown();
            concurrentClient.shutdown();
        }
    }

    /**
     * Given two streams, blocking one compaction must not prevent another from executing concurrently.
     */
    @Test
    public void testDifferentStreamsCompactConcurrently() throws Exception {
        ThreadPoolExecutor workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        S3StreamClient concurrentClient = new S3StreamClient(streamManager, mock(Storage.class), objectManager,
            mock(ObjectStorage.class), config, null, null, workers);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            for (long id = 1; id <= 2; id++) {
                S3Stream underlying = mock(S3Stream.class);
                when(underlying.streamId()).thenReturn(id);
                S3StreamClient.StreamWrapper.Compaction stream = spy(concurrentClient.new StreamWrapper(underlying).compaction);
                doAnswer(args -> {
                    started.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    return null;
                }).when(stream).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
                stream.compact(new S3StreamClient.CompactionHint(0), System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2));
            }
            assertTrue(started.await(5, TimeUnit.SECONDS));
            release.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            concurrentClient.shutdown();
        }
    }

    /**
     * Given cleanup with no eligible objects, pressure still schedules MAJOR only on a later scan.
     */
    @Test
    public void testNoOpCleanupDefersPendingMajor() {
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(Integer.MAX_VALUE);
        long now = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        stream.compact(hint, now);
        compactionTasks.remove().run();
        assertTrue(compactionTasks.isEmpty());
        verify(stream).compact(CLEANUP_V1, hint);
        verify(stream, never()).compact(MAJOR_V1, hint);
        stream.compact(hint, now);
        compactionTasks.remove().run();
        verify(stream).compact(MAJOR_V1, hint);
    }

    /**
     * Given a legacy stream, asynchronous execution preserves its major/minor/cleanup selection.
     */
    @Test
    public void testLegacyCompactionSelection() {
        config.version(() -> Version.V0);
        S3StreamClient.StreamWrapper.Compaction stream = compactionStream(1);
        S3StreamClient.CompactionHint hint = new S3StreamClient.CompactionHint(Integer.MAX_VALUE);
        List<CompactionType> types = new ArrayList<>();
        doAnswer(args -> {
            types.add(args.getArgument(0));
            return null;
        }).when(stream).compact(any(CompactionType.class), any(S3StreamClient.CompactionHint.class));
        for (long minutes : List.of(2L, 6L, 61L)) {
            stream.compact(hint, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(minutes));
            compactionTasks.remove().run();
        }
        assertEquals(List.of(CompactionType.CLEANUP, CompactionType.MINOR, CompactionType.MAJOR), types);
    }

    /**
     * Given Archive protection, cleanup keeps boundary-touching objects and refreshes the range on the next round.
     */
    @Test
    public void testCompactionRefreshesArchiveProtection() {
        config.version(() -> Version.V2);
        S3Stream underlying = mock(S3Stream.class);
        when(underlying.streamId()).thenReturn(1L);
        when(underlying.startOffset()).thenReturn(200L);
        when(underlying.confirmOffset()).thenReturn(200L);
        List<S3ObjectMetadata> objects = List.of(
            cleanupObject(1, 0, 50), cleanupObject(2, 40, 60), cleanupObject(3, 50, 150),
            cleanupObject(4, 140, 160), cleanupObject(5, 150, 200));
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(objects));
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(streamManager.getStreamArchive(1L, 0L)).thenReturn(CompletableFuture.completedFuture(
            new StreamArchiveState(1, 0, 50, 50, 100, 150, 1, 50, 0)));
        S3StreamClient.StreamWrapper.Compaction compaction = client.new StreamWrapper(underlying).compaction;

        compaction.compact(CompactionType.CLEANUP, new S3StreamClient.CompactionHint(0));
        // An empty range must also allow objects straddling the common cursor.
        when(streamManager.getStreamArchive(1L, 0L)).thenReturn(CompletableFuture.completedFuture(
            new StreamArchiveState(1, 0, 100, 100, 100, 100, 0, 100, 0)));
        compaction.compact(CompactionType.CLEANUP, new S3StreamClient.CompactionHint(0));

        ArgumentCaptor<CompactStreamObjectRequest> request = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(request.capture());
        assertEquals(List.of(1L, 5L), request.getAllValues().get(0).getSourceObjectIds());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), request.getAllValues().get(1).getSourceObjectIds());
    }

    /**
     * Given an Archive read failure, compaction must not proceed with an unprotected fallback.
     */
    @Test
    public void testCompactionStopsWhenArchiveReadFails() {
        config.version(() -> Version.V2);
        when(streamManager.getStreamArchive(anyLong(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Archive unavailable")));
        S3StreamClient.StreamWrapper.Compaction compaction = client.new StreamWrapper(mock(S3Stream.class)).compaction;

        assertThrows(java.util.concurrent.CompletionException.class,
            () -> compaction.compact(CompactionType.CLEANUP, new S3StreamClient.CompactionHint(0)));
        verify(objectManager, never()).getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt());
        verify(objectManager, never()).compactStreamObject(any());
    }

    /**
     * Given Archive is disabled, cleanup does not request Archive state and keeps its existing eligibility rules.
     */
    @Test
    public void testLegacyCleanupDoesNotReadArchiveState() {
        S3Stream underlying = mock(S3Stream.class);
        when(underlying.startOffset()).thenReturn(50L);
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(List.of(cleanupObject(1, 0, 50))));
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));

        client.new StreamWrapper(underlying).compaction.compact(CompactionType.CLEANUP, new S3StreamClient.CompactionHint(0));

        verify(streamManager, never()).getStreamArchive(anyLong(), anyLong());
        verify(objectManager).compactStreamObject(any());
    }

    private S3ObjectMetadata cleanupObject(long id, long start, long end) {
        return new S3ObjectMetadata(id, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, start, end)),
            0, 0, 100, 0);
    }

    private S3StreamClient.StreamWrapper.Compaction compactionStream(long id) {
        S3Stream stream = mock(S3Stream.class);
        when(stream.streamId()).thenReturn(id);
        return spy(client.new StreamWrapper(stream).compaction);
    }

    /**
     * Given post-compaction global object counts, when selecting Archive pressure, then the MAJOR_V1 soft and hard
     * thresholds define Low, Medium, and High.
     */
    @Test
    public void testArchivePressureUsesPostCompactionGlobalThresholds() {
        assertEquals(StreamObjectArchiveTask.Pressure.LOW, S3StreamClient.archivePressure(89, 100));
        assertEquals(StreamObjectArchiveTask.Pressure.MEDIUM, S3StreamClient.archivePressure(90, 100));
        assertEquals(StreamObjectArchiveTask.Pressure.HIGH, S3StreamClient.archivePressure(100, 100));
    }

    /**
     * Given Archive feature finalization and a legacy configured 10 GiB compaction size, when MAJOR_V1 selects its
     * layout target, then finalized Archive uses 512 MiB while the previous feature level keeps the legacy setting.
     */
    @Test
    public void testArchiveFeatureFinalizationSelectsFixedCompositeTarget() {
        Config legacy = new Config().version(() -> Version.V1).streamObjectCompactionMaxSizeBytes(10L << 30);
        Config archive = new Config().version(() -> Version.V2).streamObjectCompactionMaxSizeBytes(10L << 30);

        assertEquals(10L << 30, S3StreamClient.majorV1CompactionSize(legacy));
        assertEquals(512L << 20, S3StreamClient.majorV1CompactionSize(archive));
    }

}
