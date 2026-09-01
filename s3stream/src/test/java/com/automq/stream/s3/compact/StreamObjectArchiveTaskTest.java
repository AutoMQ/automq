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

package com.automq.stream.s3.compact;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.CompositeObjectWriter;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamObjectArchiveTaskTest {
    private static final long STREAM_ID = 7L;
    private static final long STREAM_EPOCH = 3L;
    private static final long COMPOSITE_TARGET_SIZE = 512L * 1024 * 1024;

    /**
     * Given consecutive terminal Composites at the Archive cursor, when ARCHIVE runs, then it durably prepares,
     * copies each manifest to its deterministic key, and publishes the complete logical range without deleting sources.
     */
    @Test
    void testArchivePreparesCopiesAndPublishesTerminalComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata first = composite(objectStorage, 11L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        S3ObjectMetadata second = composite(objectStorage, 12L, 10L, 20L, COMPOSITE_TARGET_SIZE);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 100L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenReturn(0L);
        when(stream.confirmOffset()).thenReturn(100L);

        StreamArchiveState initial = state(0L, 0L, 0L, 0L);
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveState update = invocation.getArgument(0);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });

        StreamObjectArchiveTask.builder()
            .objectManager(objectManager)
            .streamManager(streamManager)
            .objectStorage(objectStorage)
            .stream(stream)
            .build()
            .archive();

        assertEquals(2, updates.size());
        assertEquals(List.of(11L, 12L), updates.get(0).archiveObjectIds());
        assertEquals(20L, updates.get(0).archivePreparedEndOffset());
        assertEquals(List.of(), updates.get(1).archiveObjectIds());
        assertEquals(20L, updates.get(1).archiveEndOffset());
        assertEquals(2 * COMPOSITE_TARGET_SIZE, updates.get(1).archiveSize());
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, 11L)));
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, 12L)));
        assertTrue(objectStorage.contains("archive/7/0000000000000000010-0000000000000000000-11-536870912"));
        assertTrue(objectStorage.contains("archive/7/0000000000000000020-0000000000000000010-12-536870912"));
    }

    /**
     * Given a durable prepared range after failover, when ARCHIVE recovers, then it rewrites the complete range and
     * publishes without issuing another prepare.
     */
    @Test
    void testArchiveRecoversPreparedRangeBeforeSelectingNewWork() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata preparedObject = composite(objectStorage, 21L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        String archiveKey = "archive/7/0000000000000000010-0000000000000000000-21-536870912";
        Writer staleWriter = objectStorage.writer(new WriteOptions(), archiveKey);
        staleWriter.write(Unpooled.wrappedBuffer(new byte[] {1}));
        staleWriter.close().get();
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(preparedObject)));
        Stream stream = stream(0L, 100L);
        StreamArchiveState prepared = state(0L, 10L, 0L, 0L);
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = streamManager(prepared, updates);

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(objectStorage).stream(stream).build().archive();

        assertEquals(1, updates.size());
        assertEquals(10L, updates.get(0).archiveEndOffset());
        assertEquals(COMPOSITE_TARGET_SIZE, updates.get(0).archiveSize());
        assertTrue(objectStorage.contains(archiveKey));
        assertTrue(objectStorage.list(new com.automq.stream.s3.operator.ObjectStorage.ListOptions(archiveKey))
            .get().get(0).size() > 1L);
    }

    /**
     * Given retention has overtaken a durable prepared range, when ARCHIVE runs, then recovery still publishes the
     * prepared range before catch-up cleanup can drain it.
     */
    @Test
    void testArchiveRecoversPreparedRangeAfterRetentionOvertakesPublishedEnd() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata preparedObject = composite(objectStorage, 22L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(preparedObject)));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 10L, 0L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(20L, 100L)).build().archive();

        assertEquals(1, updates.size());
        assertEquals(10L, updates.get(0).archiveEndOffset());
        assertEquals(COMPOSITE_TARGET_SIZE, updates.get(0).archiveSize());
    }

    /**
     * Given retention has overtaken published Archive data, when no prepared range exists, then ARCHIVE leaves new
     * online candidates untouched while retention and metadata cleanup drain the old range.
     */
    @Test
    void testArchiveDoesNotSelectNewWorkWhileOvertakenArchiveIsDraining() throws Exception {
        ObjectManager objectManager = mock(ObjectManager.class);
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenReturn(CompletableFuture.completedFuture(
            archiveState(0L, 0L, 10L, 10L, COMPOSITE_TARGET_SIZE)));

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(20L, 100L)).build().archive();

        verify(objectManager, never()).getStreamObjects(org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt());
        verify(streamManager, never()).updateStreamArchive(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Given retention has drained Archive data and metadata, when the first surviving online object overlaps Stream
     * start, then ARCHIVE aligns every empty cursor to that object's start without publishing the object.
     */
    @Test
    void testArchiveAdvancesEmptyCursorToOverlappingOnlineObject() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata surviving = composite(objectStorage, 23L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(0L, 0L, 0L, 0L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(100L, 200L)).build().archive();

        assertEquals(1, updates.size());
        assertEquals(50L, updates.get(0).archiveStartOffset());
        assertEquals(50L, updates.get(0).archiveMetadataEndOffset());
        assertEquals(50L, updates.get(0).archiveEndOffset());
        assertEquals(0L, updates.get(0).archiveSize());
        assertFalse(objectStorage.contains("archive/7/0000000000000000150-0000000000000000050-23-1"));
    }

    /**
     * Given an empty Archive and no surviving online object, when retention is ahead, then ARCHIVE aligns the empty
     * cursor directly to Stream start.
     */
    @Test
    void testArchiveAdvancesEmptyCursorToStreamStartWithoutOnlineObjects() throws Exception {
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(0L, 0L, 0L, 0L, 0L), updates))
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(100L, 200L)).build().archive();

        assertEquals(100L, updates.get(0).archiveEndOffset());
        assertEquals(0L, updates.get(0).archiveSize());
    }

    /**
     * Given a stale Image produces an empty-cursor proposal that the Controller rejects, when ARCHIVE runs, then the
     * task exits after that single update and leaves a later scheduler cycle to retry from a fresh Image.
     */
    @Test
    void testArchiveEmptyCursorConflictDoesNotRetryInTask() {
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenReturn(CompletableFuture.completedFuture(
            archiveState(0L, 0L, 0L, 0L, 0L)));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("stale Archive state")));

        assertThrows(ExecutionException.class, () -> StreamObjectArchiveTask.builder()
            .objectManager(objectManager).streamManager(streamManager)
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(100L, 200L)).build().archive());

        verify(streamManager).updateStreamArchive(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Given an empty cursor aligned below Stream start, when the overlapping Composite is smaller than the normal
     * terminal target, then ARCHIVE copies and publishes it through the normal prepare/publish transitions.
     */
    @Test
    void testArchivePublishesOverlappingCompositeAfterEmptyCursorAdvance() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata surviving = composite(objectStorage, 24L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(50L, 50L, 50L, 50L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(100L, 200L)).build().archive();

        assertEquals(2, updates.size());
        assertEquals(List.of(24L), updates.get(0).archiveObjectIds());
        assertEquals(150L, updates.get(1).archiveEndOffset());
        assertEquals(1L, updates.get(1).archiveSize());
        assertTrue(objectStorage.contains("archive/7/0000000000000000150-0000000000000000050-24-1"));
    }

    /**
     * Given retention overtakes a prepared Archive, when successive lifecycle rounds recover, drain, align, and
     * resume, then the prepared range publishes first and the overlapping survivor publishes only after empty-cursor
     * alignment.
     */
    @Test
    void testArchiveConvergesAfterRetentionOvertakesPreparedWork() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata prepared = composite(objectStorage, 25L, 10L, 20L, 1L);
        S3ObjectMetadata surviving = composite(objectStorage, 26L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 10L, 20L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(prepared)));
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(
            archiveState(0L, 0L, 10L, 20L, 1L));
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveState update = invocation.getArgument(0);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        StreamObjectArchiveTask task = StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager).objectStorage(objectStorage).stream(stream(100L, 200L)).build();

        task.archive();
        assertEquals(20L, current.get().archiveEndOffset());
        current.set(archiveState(20L, 20L, 20L, 20L, 0L));
        task.archive();
        assertEquals(50L, current.get().archiveEndOffset());
        assertEquals(0L, current.get().archiveSize());
        task.archive();

        assertEquals(150L, current.get().archiveEndOffset());
        assertEquals(1L, current.get().archiveSize());
        assertEquals(List.of(26L), updates.get(updates.size() - 2).archiveObjectIds());
        assertTrue(objectStorage.contains("archive/7/0000000000000000150-0000000000000000050-26-1"));
    }

    /**
     * Given a prepared Archive batch whose manifest copy fails, when ARCHIVE runs, then the prepared state remains
     * unpublished so a later task can recover the whole batch.
     */
    @Test
    void testArchiveCopyFailurePreventsPublication() throws Exception {
        MemoryObjectStorage sourceStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata object = composite(sourceStorage, 31L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        com.automq.stream.s3.operator.ObjectStorage objectStorage = mock(
            com.automq.stream.s3.operator.ObjectStorage.class);
        when(objectStorage.rangeRead(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(object.key()), org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq(com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END)))
            .thenAnswer(ignored -> sourceStorage.rangeRead(new com.automq.stream.s3.operator.ObjectStorage.ReadOptions(),
                object.key(), 0L, com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END));
        when(objectStorage.write(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
                io.netty.buffer.ByteBuf manifest = invocation.getArgument(2);
                manifest.release();
                return CompletableFuture.failedFuture(new IllegalStateException("copy failed"));
            });
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(object)));
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = streamManager(state(0L, 10L, 0L, 0L), updates);

        assertThrows(ExecutionException.class, () -> StreamObjectArchiveTask.builder()
            .objectManager(objectManager).streamManager(streamManager).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build().archive());

        assertEquals(List.of(), updates);
    }

    /**
     * Given more than 100 consecutive terminal Composites, when ARCHIVE selects a fresh batch, then prepare contains
     * exactly the first 100 objects and leaves the remainder for a later round.
     */
    @Test
    void testArchivePrepareIsBoundedToOneHundredComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        List<S3ObjectMetadata> objects = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            objects.add(composite(objectStorage, 100L + i, i * 10L, (i + 1) * 10L, COMPOSITE_TARGET_SIZE));
        }
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 2_000L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(objects));
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = streamManager(state(0L, 0L, 0L, 0L), updates);

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(objectStorage).stream(stream(0L, 2_000L)).build().archive();

        assertEquals(100, updates.get(0).archiveObjectIds().size());
        assertEquals(1_000L, updates.get(0).archivePreparedEndOffset());
        assertEquals(1_000L, updates.get(1).archiveEndOffset());
    }

    /**
     * Given a small trailing Composite with no stable merge constraint, when ARCHIVE selects candidates, then the
     * temporarily unscheduled object remains online and no Archive transition is attempted.
     */
    @Test
    void testArchiveDoesNotSelectTemporarilyUnscheduledComposite() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata small = composite(objectStorage, 41L, 0L, 10L, COMPOSITE_TARGET_SIZE / 2);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 100L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(small)));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 0L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build().archive();

        assertEquals(List.of(), updates);
    }

    /**
     * Given two small Composites whose combined offset delta cannot be represented by MAJOR_V1, when ARCHIVE selects
     * candidates, then the first Composite is terminal even though the retained-size target has not been reached.
     */
    @Test
    void testArchiveSelectsCompositeAtStableOffsetDeltaBoundary() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        long boundary = Integer.MAX_VALUE;
        S3ObjectMetadata first = composite(objectStorage, 42L, 0L, boundary, COMPOSITE_TARGET_SIZE / 4);
        S3ObjectMetadata second = composite(objectStorage, 43L, boundary, boundary + 1,
            COMPOSITE_TARGET_SIZE / 4);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, boundary + 1, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 0L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, boundary + 1)).build().archive();

        assertEquals(List.of(42L), updates.get(0).archiveObjectIds());
        assertEquals(boundary, updates.get(1).archiveEndOffset());
    }

    /**
     * Given adjacent Composite manifests that would exceed an encoded index limit or use different formats, when
     * terminal constraints are evaluated, then the boundary is stable independently of retained size.
     */
    @Test
    void testCompositeFormatLimitsAreStableTerminalBoundaries() {
        long maxIndexes = Integer.MAX_VALUE / DataBlockIndex.BLOCK_INDEX_SIZE;
        StreamObjectArchiveTask.CompositeManifestInfo current =
            new StreamObjectArchiveTask.CompositeManifestInfo(1L, maxIndexes, 1L, 0);
        StreamObjectArchiveTask.CompositeManifestInfo next =
            new StreamObjectArchiveTask.CompositeManifestInfo(1L, 1L, 1L, 0);
        StreamObjectArchiveTask.CompositeManifestInfo nextFormat =
            new StreamObjectArchiveTask.CompositeManifestInfo(1L, 1L, 1L, 1);

        assertTrue(StreamObjectArchiveTask.exceedsCompositeFormatLimits(current, next));
        assertTrue(StreamObjectArchiveTask.exceedsCompositeFormatLimits(next, nextFormat));
    }

    /**
     * Given a prepared batch with independent delayed reads and writes, when ARCHIVE recovers it, then each write
     * waits for its own read while writes for different manifests may remain concurrently in flight.
     */
    @Test
    void testArchiveCopiesConcurrentlyWithPerObjectReadBeforeWrite() throws Exception {
        MemoryObjectStorage sourceStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata first = composite(sourceStorage, 61L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        S3ObjectMetadata second = composite(sourceStorage, 62L, 10L, 20L, COMPOSITE_TARGET_SIZE);
        io.netty.buffer.ByteBuf firstManifest = sourceStorage.rangeRead(
            new com.automq.stream.s3.operator.ObjectStorage.ReadOptions(), first.key(), 0L,
            com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END).get();
        io.netty.buffer.ByteBuf secondManifest = sourceStorage.rangeRead(
            new com.automq.stream.s3.operator.ObjectStorage.ReadOptions(), second.key(), 0L,
            com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END).get();
        CompletableFuture<io.netty.buffer.ByteBuf> firstRead = new CompletableFuture<>();
        CompletableFuture<io.netty.buffer.ByteBuf> secondRead = new CompletableFuture<>();
        CompletableFuture<Void> firstWrite = new CompletableFuture<>();
        CompletableFuture<Void> secondWrite = new CompletableFuture<>();
        com.automq.stream.s3.operator.ObjectStorage objectStorage = mock(
            com.automq.stream.s3.operator.ObjectStorage.class);
        when(objectStorage.rangeRead(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq(com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END)))
            .thenAnswer(invocation -> invocation.<String>getArgument(1).equals(first.key()) ? firstRead : secondRead);
        when(objectStorage.write(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
                String key = invocation.getArgument(1);
                io.netty.buffer.ByteBuf manifest = invocation.getArgument(2);
                CompletableFuture<Void> write = key.contains("-61-") ? firstWrite : secondWrite;
                write.whenComplete((ignored, exception) -> manifest.release());
                return write;
            });
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 20L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamObjectArchiveTask task = StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 20L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build();
        CompletableFuture<Void> archive = CompletableFuture.runAsync(() -> {
            try {
                task.archive();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        });

        verify(objectStorage, timeout(1_000).times(2)).rangeRead(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq(com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END));
        verify(objectStorage, never()).write(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
        firstRead.complete(firstManifest);
        String firstKey = "archive/7/0000000000000000010-0000000000000000000-61-536870912";
        String secondKey = "archive/7/0000000000000000020-0000000000000000010-62-536870912";
        verify(objectStorage, timeout(1_000)).write(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(firstKey), org.mockito.ArgumentMatchers.any());
        verify(objectStorage, never()).write(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(secondKey), org.mockito.ArgumentMatchers.any());
        secondRead.complete(secondManifest);
        verify(objectStorage, timeout(1_000)).write(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(secondKey), org.mockito.ArgumentMatchers.any());
        assertFalse(archive.isDone());

        firstWrite.complete(null);
        secondWrite.complete(null);
        archive.get();
        assertEquals(1, updates.size());
        assertEquals(20L, updates.get(0).archiveEndOffset());
    }

    /**
     * Given a prepared batch whose copy cannot finish inside the hard task lifetime, when the deadline expires, then
     * ARCHIVE exits without publication and leaves recovery state durable.
     */
    @Test
    void testArchiveDeadlinePreventsPublication() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata object = composite(objectStorage, 51L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, Integer.MAX_VALUE))
            .thenReturn(CompletableFuture.completedFuture(List.of(object)));
        List<StreamArchiveState> updates = new ArrayList<>();
        AtomicInteger clockReads = new AtomicInteger();

        assertThrows(java.util.concurrent.TimeoutException.class, () -> StreamObjectArchiveTask.builder()
            .objectManager(objectManager).streamManager(streamManager(state(0L, 10L, 0L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(0L, 100L))
            .nanoTime(() -> clockReads.getAndIncrement() < 3 ? 0L : 10L).taskTimeoutNanos(5L).build().archive());

        assertEquals(List.of(), updates);
    }

    private static S3ObjectMetadata composite(MemoryObjectStorage storage, long objectId, long startOffset,
        long endOffset, long logicalSize) throws Exception {
        S3ObjectMetadata linked = new S3ObjectMetadata(objectId + 1000,
            ObjectAttributes.builder().bucket(storage.bucketId()).build().attributes());
        DataBlockIndex index = new DataBlockIndex(STREAM_ID, startOffset, Math.toIntExact(endOffset - startOffset),
            1, 0L, Math.toIntExact(logicalSize));
        CompositeObjectWriter writer = CompositeObject.writer(storage.writer(new WriteOptions(),
            ObjectUtils.genKey(0, objectId)));
        writer.addComponent(linked, List.of(index));
        writer.close().get();
        return new S3ObjectMetadata(objectId, S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(STREAM_ID, startOffset, endOffset)), 0L, 0L, writer.size(), objectId,
            ObjectAttributes.builder().bucket(storage.bucketId()).type(ObjectAttributes.Type.Composite).build()
                .attributes());
    }

    private static StreamArchiveState state(long archiveEndOffset, long preparedEndOffset, long archiveSize,
        long cleanupSize) {
        return new StreamArchiveState(STREAM_ID, STREAM_EPOCH, 0L, 0L, archiveEndOffset, preparedEndOffset,
            archiveSize, 0L, cleanupSize, List.of());
    }

    private static StreamArchiveState archiveState(long archiveStartOffset, long archiveMetadataEndOffset,
        long archiveEndOffset, long archivePreparedEndOffset, long archiveSize) {
        return new StreamArchiveState(STREAM_ID, STREAM_EPOCH, archiveStartOffset, archiveMetadataEndOffset,
            archiveEndOffset, archivePreparedEndOffset, archiveSize, archiveStartOffset, 0L, List.of());
    }

    private static Stream stream(long startOffset, long confirmOffset) {
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenReturn(startOffset);
        when(stream.confirmOffset()).thenReturn(confirmOffset);
        return stream;
    }

    private static StreamManager streamManager(StreamArchiveState initial, List<StreamArchiveState> updates) {
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        StreamManager manager = mock(StreamManager.class);
        when(manager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(manager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveState update = invocation.getArgument(0);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        return manager;
    }
}
