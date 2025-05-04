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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.CompositeObjectReader;
import com.automq.stream.s3.DataBlockIndex;
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
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.automq.stream.s3.compact.CompactOperations.DELETE;
import static com.automq.stream.s3.compact.CompactOperations.KEEP_DATA;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactByCompositeObject;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactByPhysicalMerge;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.CLEANUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MAJOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.CompactionType.MINOR_V1;
import static com.automq.stream.s3.compact.StreamObjectCompactor.EXPIRED_OBJECTS_CLEAN_UP_STEP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.SKIP_COMPACTION_TYPE_WHEN_ONE_OBJECT_IN_GROUP;
import static com.automq.stream.s3.compact.StreamObjectCompactor.builder;
import static com.automq.stream.s3.compact.StreamObjectCompactor.getObjectFilter;
import static com.automq.stream.s3.compact.StreamObjectCompactor.group0;
import static com.automq.stream.s3.objects.ObjectAttributes.Type.Composite;
import static com.automq.stream.s3.objects.ObjectAttributes.Type.Normal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
class StreamObjectCompactorTest {

    private ObjectManager objectManager;
    private MemoryObjectStorage objectStorage;
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
    public void testCheckObjectGroupCouldBeCompact() {
        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(10, S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(1, 0, 512 * 1024 * 1024 * 4L)),
            System.currentTimeMillis());
        s3ObjectMetadata.setObjectSize(512 * 1024 * 1024 * 4L);
        s3ObjectMetadata.setAttributes(ObjectAttributes.builder().type(Composite).build().attributes());

        // check only one objectMetadata in group
        List<S3ObjectMetadata> objectMetadataGroup =
            List.of(s3ObjectMetadata);

        SKIP_COMPACTION_TYPE_WHEN_ONE_OBJECT_IN_GROUP.forEach(type -> {
            boolean doCompact = StreamObjectCompactor.checkObjectGroupCouldBeCompact(objectMetadataGroup,
                0, type);

            // MAJOR_V1 compaction should not compact
            // only one s3ObjectMetadata in group
            // which may cause the object to be linked to itself.
            assertFalse(doCompact);
        });

        boolean doCleanupWhenMajorV1Compaction =
            StreamObjectCompactor.checkObjectGroupCouldBeCompact(objectMetadataGroup,
            512 * 1024 * 1024 * 3, MAJOR_V1);
        // MAJOR_V1 should not trigger cleanup even exceeded threshold for now.
        assertFalse(doCleanupWhenMajorV1Compaction);
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

        StreamObjectCompactor task = builder().objectManager(objectManager).objectStorage(objectStorage)
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

        StreamObjectCompactor task = builder().objectManager(objectManager).objectStorage(objectStorage)
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

        CompactStreamObjectRequest req = new CompactByPhysicalMerge(streamId, 0L, 14L,
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

        Predicate<S3ObjectMetadata> objectFilter = __ -> true;
        List<List<S3ObjectMetadata>> groups = group0(objects, 512, objectFilter);
        assertEquals(3, groups.size());
        assertEquals(List.of(2L), groups.get(0).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(3L, 4L), groups.get(1).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        assertEquals(List.of(5L, 6L), groups.get(2).stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
    }

    private List<S3ObjectMetadata> prepareS3ObjectMetadata(int normalObjectNumber, int compositeObjectNumber, int smallObjectNumber,
                                                           int compositeObjectSize, int normalObjectSize, int smallObjectSize) {
        AtomicLong objectNumber = new AtomicLong();
        AtomicLong startOffset = new AtomicLong();
        AtomicLong orderId = new AtomicLong();

        int compositeObjectAttribute = ObjectAttributes.builder().bucket((short) 0).type(Composite).build().attributes();
        int normalObjectAttribute = ObjectAttributes.builder().bucket((short) 0).type(Normal).build().attributes();

        List<S3ObjectMetadata> metadata = new ArrayList<>();

        for (int i = 0; i < compositeObjectNumber; i++) {
            S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(objectNumber.incrementAndGet(),
                S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(streamId, startOffset.get(), startOffset.addAndGet(1024))),
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                compositeObjectSize, orderId.getAndIncrement());
            s3ObjectMetadata
                .setAttributes(compositeObjectAttribute);

            metadata.add(s3ObjectMetadata);
        }

        for (int i = 0; i < normalObjectNumber; i++) {
            S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(objectNumber.incrementAndGet(),
                S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(streamId, startOffset.get(), startOffset.addAndGet(1024))),
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                normalObjectSize, orderId.getAndIncrement());
            s3ObjectMetadata
                .setAttributes(normalObjectAttribute);

            metadata.add(s3ObjectMetadata);
        }

        for (int i = 0; i < smallObjectNumber; i++) {
            S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(objectNumber.incrementAndGet(),
                S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(streamId, startOffset.get(), startOffset.addAndGet(1024))),
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                smallObjectSize, orderId.getAndIncrement());
            s3ObjectMetadata
                .setAttributes(normalObjectAttribute);

            metadata.add(s3ObjectMetadata);
        }

        return metadata;
    }

    @Test
    public void testGroupAndFilterLogic() {
        int majorCompactionObjectThreshold = 4 * 1024 * 1024;
        List<S3ObjectMetadata> metadataList = prepareS3ObjectMetadata(20, 20, 20,
            majorCompactionObjectThreshold, majorCompactionObjectThreshold, 64);
        Predicate<S3ObjectMetadata> objectFilter = getObjectFilter(MAJOR_V1, majorCompactionObjectThreshold);
        List<List<S3ObjectMetadata>> groups = group0(metadataList, 10 * majorCompactionObjectThreshold, objectFilter);

        // major_v1 compaction small composite object can still be compacted
        assertTrue(groups.stream().flatMap(List::stream)
            .anyMatch(meta -> ObjectAttributes.from(meta.attributes()).type().equals(Composite)));

        // major_v1 compaction no more small normal object
        assertTrue(groups.stream().flatMap(List::stream)
            .filter(meta -> ObjectAttributes.from(meta.attributes()).type().equals(Normal) && meta.objectSize() < majorCompactionObjectThreshold)
            .findAny().isEmpty());


        // major_v1 check disable skip small object logic
        long disableMajorV1CompactionSkipSmallObject = 0;

        objectFilter = getObjectFilter(MAJOR_V1, disableMajorV1CompactionSkipSmallObject);
        groups = group0(metadataList, 10 * majorCompactionObjectThreshold, objectFilter);

        assertTrue(groups.stream().flatMap(List::stream)
            .anyMatch(meta -> ObjectAttributes.from(meta.attributes()).type().equals(Composite)));

        // now the small object should be contained in the major_v1 compaction
        assertTrue(groups.stream().flatMap(List::stream)
            .anyMatch(meta -> ObjectAttributes.from(meta.attributes()).type().equals(Normal) && meta.objectSize() < majorCompactionObjectThreshold));


        // MINOR_V1 should skip composite object
        objectFilter = getObjectFilter(MINOR_V1, majorCompactionObjectThreshold);
        groups = group0(metadataList, 10 * majorCompactionObjectThreshold, objectFilter);

        assertTrue(groups.stream().flatMap(List::stream).filter(meta -> ObjectAttributes.from(meta.attributes()).type().equals(Composite))
            .findAny().isEmpty());
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

        StreamObjectCompactor task = builder().objectManager(objectManager).objectStorage(objectStorage)
            .maxStreamObjectSize(1024 * 1024 * 1024).stream(stream).dataBlockGroupSizeThreshold(1).build();
        task.compact(CLEANUP);

        ArgumentCaptor<CompactStreamObjectRequest> ac = ArgumentCaptor.forClass(CompactStreamObjectRequest.class);
        verify(objectManager, times(2)).compactStreamObject(ac.capture());
        CompactStreamObjectRequest clean = ac.getAllValues().get(0);
        assertEquals(ObjectUtils.NOOP_OBJECT_ID, clean.getObjectId());
        assertEquals(LongStream.range(0, EXPIRED_OBJECTS_CLEAN_UP_STEP).boxed().collect(Collectors.toList()), clean.getSourceObjectIds());

        clean = ac.getAllValues().get(1);
        assertEquals(ObjectUtils.NOOP_OBJECT_ID, clean.getObjectId());
        assertEquals(LongStream.range(EXPIRED_OBJECTS_CLEAN_UP_STEP, 1450).boxed().collect(Collectors.toList()), clean.getSourceObjectIds());
    }

    @Test
    public void testCompactByCompositeObject() throws ExecutionException, InterruptedException {
        // TODO: add test when MemoryObjectStorage is ready
        List<S3ObjectMetadata> metadataList = prepareData();
        // composite compact two normal objects
        CompactByCompositeObject compact = new CompactByCompositeObject(streamId, 0L, 0L, metadataList.subList(0, 2), 5, objectStorage);
        CompactStreamObjectRequest req = compact.compact().get();
        assertEquals(ObjectAttributes.Type.Composite, ObjectAttributes.from(req.getAttributes()).type());
        assertEquals(5, req.getObjectId());
        assertEquals(streamId, req.getStreamId());
        assertEquals(10L, req.getStartOffset());
        assertEquals(18L, req.getEndOffset());
        assertEquals(List.of(1L, 2L), req.getSourceObjectIds());
        assertEquals(List.of(KEEP_DATA, KEEP_DATA), req.getOperations());

        S3ObjectMetadata object5Metadata = new S3ObjectMetadata(5, req.getAttributes());
        CompositeObjectReader reader = CompositeObject.reader(object5Metadata, objectStorage);
        List<DataBlockIndex> indexes = reader.basicObjectInfo().get().indexBlock().indexes();
        Assertions.assertEquals(4, indexes.size());
        Assertions.assertEquals(List.of(10L, 11L, 12L),
            reader.read(indexes.get(0)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
        Assertions.assertEquals(List.of(13L, 14L, 15L),
            reader.read(indexes.get(1)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
        Assertions.assertEquals(List.of(16L),
            reader.read(indexes.get(2)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
        Assertions.assertEquals(List.of(17L),
            reader.read(indexes.get(3)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );

        // object-6: offset 18
        ObjectWriter writer = ObjectWriter.writer(6, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
        writer.write(233L, List.of(
            newRecord(18L, 1, 1024)
        ));
        writer.close().get();
        S3ObjectMetadata object6Metadata = new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamId, 18, 19)),
            System.currentTimeMillis(), System.currentTimeMillis(), writer.size(), 6);

        // compact object5 and object6, expect the composite object contains 16 ~ 18 and delete object1
        compact = new CompactByCompositeObject(streamId, 0L, 17L, List.of(object5Metadata, object6Metadata), 7, objectStorage);
        req = compact.compact().get();
        assertEquals(ObjectAttributes.Type.Composite, ObjectAttributes.from(req.getAttributes()).type());
        assertEquals(7, req.getObjectId());
        assertEquals(16L, req.getStartOffset());
        assertEquals(19L, req.getEndOffset());
        assertEquals(List.of(5L, 6L), req.getSourceObjectIds());
        assertEquals(List.of(DELETE, KEEP_DATA), req.getOperations());
        assertFalse(objectStorage.contains(ObjectUtils.genKey(0, 1L)));
        S3ObjectMetadata object7Metadata = new S3ObjectMetadata(7, req.getAttributes());
        reader = CompositeObject.reader(object7Metadata, objectStorage);
        indexes = reader.basicObjectInfo().get().indexBlock().indexes();
        Assertions.assertEquals(3, indexes.size());
        Assertions.assertEquals(List.of(16L),
            reader.read(indexes.get(0)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
        Assertions.assertEquals(List.of(17L),
            reader.read(indexes.get(1)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
        Assertions.assertEquals(List.of(18L),
            reader.read(indexes.get(2)).get().records().stream().map(StreamRecordBatch::getBaseOffset).collect(Collectors.toList())
        );
    }

    StreamRecordBatch newRecord(long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}
