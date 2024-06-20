/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.compact;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.S3Stream;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamObjectCompactorTest {

    private ObjectManager objectManager;
    private ObjectStorage objectStorage;
    private S3Stream stream;
    private final long streamId = 233L;

    @BeforeEach
    void setUp() {
        objectManager = Mockito.mock(ObjectManager.class);
        objectStorage = new MemoryObjectStorage();
        stream = Mockito.mock(S3Stream.class);
    }

    List<S3ObjectMetadata> prepareData() throws ExecutionException, InterruptedException {
        // prepare object
        List<S3ObjectMetadata> objects = new LinkedList<>();
        {
            // object-1: offset 10~15
            ObjectWriter writer = ObjectWriter.writer(1, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                newRecord(10L, 1, 1024),
                newRecord(11L, 1, 1024),
                newRecord(12L, 1, 1024)
            ));
            writer.write(233L, List.of(
                newRecord(13L, 1, 1024),
                newRecord(14L, 1, 1024),
                newRecord(15L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 10, 16)),
                System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 1));
        }
        {
            // object-2: offset 16~17
            ObjectWriter writer = ObjectWriter.writer(2, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                newRecord(16L, 1, 1024)
            ));
            writer.write(233L, List.of(
                newRecord(17L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 16, 18)),
                System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 2));
        }
        {
            // object-3: offset 30
            ObjectWriter writer = ObjectWriter.writer(3, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                newRecord(30L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 30, 31)),
                System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 3));
        }
        {
            // object-4: offset 31-32
            ObjectWriter writer = ObjectWriter.writer(4, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(233L, List.of(
                newRecord(31L, 1, 1024)
            ));
            writer.write(233L, List.of(
                newRecord(32L, 1, 1024)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 31, 33)),
                System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 4));
        }
        return objects;
    }

    @Test
    public void testCompact() throws ExecutionException, InterruptedException {
        List<S3ObjectMetadata> objects = prepareData();
        when(objectManager.getStreamObjects(eq(streamId), eq(0L), eq(32L), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(objects));
        AtomicLong nextObjectId = new AtomicLong(5);
        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(nextObjectId.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(streamId);
        when(stream.startOffset()).thenReturn(14L);
        when(stream.confirmOffset()).thenReturn(32L);

        StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).s3Operator(objectStorage)
            .maxStreamObjectSize(1024 * 1024 * 1024).stream(stream).dataBlockGroupSizeThreshold(1).build();
        task.compact(MAJOR);

        ArgumentCaptor<CompactStreamObjectRequest> ac = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(ac.capture());

        // verify compact request
        List<CompactStreamObjectRequest> requests = ac.getAllValues();
        CompactStreamObjectRequest req1 = requests.get(0);
        assertEquals(5, req1.getObjectId());
        assertEquals(233L, req1.getStreamId());
        assertEquals(13L, req1.getStartOffset());
        assertEquals(18L, req1.getEndOffset());
        assertEquals(List.of(1L, 2L), req1.getSourceObjectIds());

        CompactStreamObjectRequest req2 = requests.get(1);
        assertEquals(6, req2.getObjectId());
        assertEquals(233L, req2.getStreamId());
        assertEquals(30L, req2.getStartOffset());
        assertEquals(33L, req2.getEndOffset());
        assertEquals(List.of(3L, 4L), req2.getSourceObjectIds());

        // verify compacted object record
        {
            ObjectReader objectReader = ObjectReader.reader(new S3ObjectMetadata(5, req1.getObjectSize(), S3ObjectType.STREAM), objectStorage);
            assertEquals(3, objectReader.basicObjectInfo().get().indexBlock().count());
            ObjectReader.FindIndexResult rst = objectReader.find(streamId, 13L, 18L).get();
            assertEquals(3, rst.streamDataBlocks().size());
            ObjectReader.DataBlockGroup dataBlockGroup1 = objectReader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get();
            try (dataBlockGroup1) {
                assertEquals(3, dataBlockGroup1.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup1.iterator();
                assertEquals(13L, it.next().getBaseOffset());
                assertEquals(14L, it.next().getBaseOffset());
                assertEquals(15L, it.next().getBaseOffset());
                assertFalse(it.hasNext());
            }
            ObjectReader.DataBlockGroup dataBlockGroup2 = objectReader.read(rst.streamDataBlocks().get(1).dataBlockIndex()).get();
            try (dataBlockGroup2) {
                assertEquals(1, dataBlockGroup2.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup2.iterator();
                assertEquals(16L, it.next().getBaseOffset());
            }
            ObjectReader.DataBlockGroup dataBlockGroup3 = objectReader.read(rst.streamDataBlocks().get(2).dataBlockIndex()).get();
            try (dataBlockGroup3) {
                assertEquals(1, dataBlockGroup3.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup3.iterator();
                assertEquals(17L, it.next().getBaseOffset());
            }
            objectReader.close();
        }
        {
            ObjectReader objectReader = ObjectReader.reader(new S3ObjectMetadata(6, req2.getObjectSize(), S3ObjectType.STREAM), objectStorage);
            assertEquals(3, objectReader.basicObjectInfo().get().indexBlock().count());
            ObjectReader.FindIndexResult rst = objectReader.find(streamId, 30L, 33L).get();
            assertEquals(3, rst.streamDataBlocks().size());
            ObjectReader.DataBlockGroup dataBlockGroup1 = objectReader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get();
            try (dataBlockGroup1) {
                assertEquals(1, dataBlockGroup1.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup1.iterator();
                assertEquals(30L, it.next().getBaseOffset());
                assertFalse(it.hasNext());
            }
            ObjectReader.DataBlockGroup dataBlockGroup2 = objectReader.read(rst.streamDataBlocks().get(1).dataBlockIndex()).get();
            try (dataBlockGroup2) {
                assertEquals(1, dataBlockGroup2.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup2.iterator();
                assertEquals(31L, it.next().getBaseOffset());
            }
            ObjectReader.DataBlockGroup dataBlockGroup3 = objectReader.read(rst.streamDataBlocks().get(2).dataBlockIndex()).get();
            try (dataBlockGroup3) {
                assertEquals(1, dataBlockGroup3.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup3.iterator();
                assertEquals(32L, it.next().getBaseOffset());
            }
            objectReader.close();
        }
    }

    @Test
    public void testCleanup() throws ExecutionException, InterruptedException {
        List<S3ObjectMetadata> objects = prepareData();
        when(objectManager.getStreamObjects(eq(streamId), eq(0L), eq(32L), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(objects));
        AtomicLong nextObjectId = new AtomicLong(5);
        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(nextObjectId.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(streamId);
        when(stream.startOffset()).thenReturn(17L);
        when(stream.confirmOffset()).thenReturn(32L);

        StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).s3Operator(objectStorage)
            .maxStreamObjectSize(1024 * 1024 * 1024).stream(stream).dataBlockGroupSizeThreshold(1).build();
        task.compact(MAJOR);

        ArgumentCaptor<CompactStreamObjectRequest> ac = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(ac.capture());
        CompactStreamObjectRequest clean = ac.getAllValues().get(0);
        assertEquals(ObjectUtils.NOOP_OBJECT_ID, clean.getObjectId());
        assertEquals(List.of(1L), clean.getSourceObjectIds());

        CompactStreamObjectRequest compact0 = ac.getAllValues().get(1);
        assertEquals(5, compact0.getObjectId());
    }

    @Test
    public void testCompact_groupBlocks() throws ExecutionException, InterruptedException {
        List<S3ObjectMetadata> objects = prepareData();

        CompactStreamObjectRequest req = new StreamObjectCompactor.CompactByPhysicalMerge(streamId, 0L, 14L,
            objects.subList(0, 2), 5, 5000, objectStorage).compact().get();
        // verify compact request
        assertEquals(5, req.getObjectId());
        assertEquals(233L, req.getStreamId());
        assertEquals(13L, req.getStartOffset());
        assertEquals(18L, req.getEndOffset());
        assertEquals(List.of(1L, 2L), req.getSourceObjectIds());

        // verify compacted object record, expect [13,16) + [16, 17) compact to one data block group.
        {
            ObjectReader objectReader = ObjectReader.reader(new S3ObjectMetadata(5, req.getObjectSize(), S3ObjectType.STREAM), objectStorage);
            assertEquals(2, objectReader.basicObjectInfo().get().indexBlock().count());
            ObjectReader.FindIndexResult rst = objectReader.find(streamId, 13L, 18L).get();
            assertEquals(2, rst.streamDataBlocks().size());
            ObjectReader.DataBlockGroup dataBlockGroup1 = objectReader.read(rst.streamDataBlocks().get(0).dataBlockIndex()).get();
            try (dataBlockGroup1) {
                assertEquals(4, dataBlockGroup1.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup1.iterator();
                assertEquals(13L, it.next().getBaseOffset());
                assertEquals(14L, it.next().getBaseOffset());
                assertEquals(15L, it.next().getBaseOffset());
                assertEquals(16L, it.next().getBaseOffset());
                assertFalse(it.hasNext());
            }
            ObjectReader.DataBlockGroup dataBlockGroup2 = objectReader.read(rst.streamDataBlocks().get(1).dataBlockIndex()).get();
            try (dataBlockGroup2) {
                assertEquals(1, dataBlockGroup2.recordCount());
                Iterator<StreamRecordBatch> it = dataBlockGroup2.iterator();
                StreamRecordBatch record = it.next();
                assertEquals(17L, record.getBaseOffset());
                assertEquals(18L, record.getLastOffset());
            }
            objectReader.close();
        }
    }

    @Test
    public void testGroup() {
        List<S3ObjectMetadata> objects = List.of(
            new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 16, 18)),
                System.currentTimeMillis(), System.currentTimeMillis(), 1024, 2),

            new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 18, 19)),
                System.currentTimeMillis(), System.currentTimeMillis(), 1, 3),
            new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 19, 20)),
                System.currentTimeMillis(), System.currentTimeMillis(), 1, 4),

            new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 30, 31)),
                System.currentTimeMillis(), System.currentTimeMillis(), 1, 5),
            new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 31, 32)),
                System.currentTimeMillis(), System.currentTimeMillis(), 1, 6)
        );
        List<List<S3ObjectMetadata>> groups = StreamObjectCompactor.group0(objects, 512, false);
        assertEquals(3, groups.size());
        assertEquals(List.of(2L), groups.get(0).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(3L, 4L), groups.get(1).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(5L, 6L), groups.get(2).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
    }

    @Test
    public void testCleanup_byStep() throws ExecutionException, InterruptedException {
        // prepare object
        List<S3ObjectMetadata> objects = new LinkedList<>();
        for (int i = 0; i < 1500; i++) {
            ObjectWriter writer = ObjectWriter.writer(1, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            writer.write(streamId, List.of(
                newRecord(i, 1, 1)
            ));
            writer.close().get();
            objects.add(new S3ObjectMetadata(i, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, i, i + 1)),
                System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 1));
        }

        when(objectManager.getStreamObjects(eq(streamId), eq(0L), eq(1500L), eq(Integer.MAX_VALUE)))
            .thenReturn(CompletableFuture.completedFuture(objects));
        AtomicLong nextObjectId = new AtomicLong(1501);
        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(nextObjectId.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.compactStreamObject(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(streamId);
        when(stream.startOffset()).thenReturn(1450L);
        when(stream.confirmOffset()).thenReturn(1500L);

        StreamObjectCompactor task = StreamObjectCompactor.builder().objectManager(objectManager).s3Operator(objectStorage)
            .maxStreamObjectSize(1024 * 1024 * 1024).stream(stream).dataBlockGroupSizeThreshold(1).build();
        task.compact(CLEANUP);

        ArgumentCaptor<CompactStreamObjectRequest> ac = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(ac.capture());
        CompactStreamObjectRequest clean = ac.getAllValues().get(0);
        assertEquals(ObjectUtils.NOOP_OBJECT_ID, clean.getObjectId());
        assertEquals(LongStream.range(0, StreamObjectCompactor.EXPIRED_OBJECTS_CLEAN_UP_STEP).boxed().collect(Collectors.toList()), clean.getSourceObjectIds());

        clean = ac.getAllValues().get(1);
        assertEquals(ObjectUtils.NOOP_OBJECT_ID, clean.getObjectId());
        assertEquals(LongStream.range(StreamObjectCompactor.EXPIRED_OBJECTS_CLEAN_UP_STEP, 1450).boxed().collect(Collectors.toList()), clean.getSourceObjectIds());
    }

    @Test
    public void testCompactByCompositeObject() {
        // TODO: add test when MemoryObjectStorage is ready
    }

    StreamRecordBatch newRecord(long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}