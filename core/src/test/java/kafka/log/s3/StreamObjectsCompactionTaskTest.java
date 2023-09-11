/*
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

package kafka.log.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitStreamObjectRequest;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.CommitWALObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static kafka.log.s3.TestUtils.random;
import static kafka.log.s3.operator.Writer.MAX_PART_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamObjectsCompactionTaskTest {

    private ObjectManager objectManager;
    private S3Operator s3Operator;
    private S3Stream stream;

    @BeforeEach
    void setUp() {
        objectManager = Mockito.mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
        stream = Mockito.mock(S3Stream.class);
        when(stream.streamId()).thenReturn(1L);
        when(stream.startOffset()).thenReturn(5L);
        when(stream.nextOffset()).thenReturn(100L);
    }

    @Test
    void testTriggerTask() throws ExecutionException, InterruptedException {
        // Prepare 4 stream objects. They should be compacted into 2 new stream objects.
        List<List<Long>> objectsDetails = List.of(
            List.of(40L, 50L, 1000L),
            List.of(50L, 60L, 1000L),
            List.of(65L, 70L, 1000L),
            List.of(70L, 80L, 1000L)
        );
        List<S3ObjectMetadata> metadataList = prepareRawStreamObjects(10, stream.streamId(), objectsDetails);

        // two stream object groups should be handled
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(metadataList);
        StreamObjectsCompactionTask task = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, Long.MAX_VALUE, 0);

        AtomicLong objectIdAlloc = new AtomicLong(100);
        List<CommitStreamObjectRequest> committedRequests = new ArrayList<>();
        when(objectManager.prepareObject(anyInt(), anyLong())).thenAnswer(
            invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement()));
        when(objectManager.commitStreamObject(any(CommitStreamObjectRequest.class))).thenAnswer(invocation -> {
            committedRequests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });

        // trigger a stream object compaction task
        task.prepare();
        task.doCompactions();

        // 40L > stream.startOffset, expecting no changes to startSearchingOffset
        assertEquals(stream.startOffset(), task.getNextStartSearchingOffset());

        verify(objectManager, Mockito.times(2)).commitStreamObject(any(CommitStreamObjectRequest.class));

        assertEquals(100, committedRequests.get(0).getObjectId());
        assertEquals(40, committedRequests.get(0).getStartOffset());
        assertEquals(60, committedRequests.get(0).getEndOffset());
        assertEquals(List.of(11L, 13L), committedRequests.get(0).getSourceObjectIds());
        assertEquals(101, committedRequests.get(1).getObjectId());
        assertEquals(65, committedRequests.get(1).getStartOffset());
        assertEquals(80, committedRequests.get(1).getEndOffset());
        assertEquals(List.of(15L, 17L), committedRequests.get(1).getSourceObjectIds());
    }

    @Test
    void testTriggerTaskFailure() throws InterruptedException {
        // 2 compaction groups will be handled.
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(List.of(
                new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 5, 10)), 0, 0, 150,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 10, 20)), 0, 0, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 40, 50)), 0, 0, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 50, 60)), 0, 0, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 65, 70)), 0, 0, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 70, 80)), 0, 0, 20,
                    S3StreamConstant.INVALID_ORDER_ID)));
        when(objectManager.prepareObject(anyInt(), anyLong())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("halt compaction task")));

        // The first group's compaction failed in prepareObject phase, the second group should not be handled.
        StreamObjectsCompactionTask task = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 0);
        task.prepare();
        try {
            task.doCompactions().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException, "should throw RuntimeException");
        }
        verify(objectManager, Mockito.times(1)).prepareObject(anyInt(), anyLong());
        verify(objectManager, Mockito.times(0)).commitStreamObject(any(CommitStreamObjectRequest.class));

        // The first group's compaction failed due to stream's closure, the second group should not be handled.
        when(stream.isClosed()).thenReturn(true);
        task.prepare();
        try {
            task.doCompactions().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StreamObjectsCompactionTask.HaltException, "should throw HaltException");
        }
        verify(objectManager, Mockito.times(1)).prepareObject(anyInt(), anyLong());
        verify(objectManager, Mockito.times(0)).commitStreamObject(any(CommitStreamObjectRequest.class));

    }

    /**
     * Prepare raw stream objects for compaction test
     *
     * @param startObjectId  start object id
     * @param streamId       stream id
     * @param objectsDetails list of [startOffset, endOffset, recordsSize]. Each item in the list will be used to generate a stream object.
     * @return list of stream object metadata
     * @throws ExecutionException   when prepareObject or commitWALObject failed
     * @throws InterruptedException when prepareObject or commitWALObject failed
     */
    List<S3ObjectMetadata> prepareRawStreamObjects(long startObjectId, long streamId,
        List<List<Long>> objectsDetails) throws ExecutionException, InterruptedException {
        AtomicLong objectIdAlloc = new AtomicLong(startObjectId);
        Stack<CommitWALObjectRequest> commitWALObjectRequests = new Stack<>();
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager)
            .prepareObject(anyInt(), anyLong());
        when(objectManager.commitWALObject(any())).thenAnswer(invocation -> {
            commitWALObjectRequests.push(invocation.getArgument(0));
            return CompletableFuture.completedFuture(new CommitWALObjectResponse());
        });

        List<S3ObjectMetadata> metadataList = new ArrayList<>();

        for (int i = 0; i < objectsDetails.size(); i++) {
            List<Long> objectsDetail = objectsDetails.get(i);
            long startOffset = objectsDetail.get(0);
            long endOffset = objectsDetail.get(1);
            int recordsSize = Math.toIntExact(objectsDetail.get(2));

            Map<Long, List<StreamRecordBatch>> map = Map.of(streamId,
                List.of(new StreamRecordBatch(streamId, 0, startOffset, Math.toIntExact(endOffset - startOffset), random(recordsSize))));
            WALObjectUploadTask walObjectUploadTask = new WALObjectUploadTask(map, objectManager, s3Operator, 16 * 1024 * 1024, 16 * 1024 * 1024,
                recordsSize - 1);

            walObjectUploadTask.prepare().get();
            walObjectUploadTask.upload().get();
            walObjectUploadTask.commit().get();

            CommitWALObjectRequest request = commitWALObjectRequests.pop();
            assertEquals(startObjectId + i * 2L, request.getObjectId());

            assertEquals(1, request.getStreamObjects().size());
            StreamObject streamObject = request.getStreamObjects().get(0);
            assertEquals(streamId, streamObject.getStreamId());
            assertEquals(startObjectId + i * 2L + 1, streamObject.getObjectId());
            assertEquals(startOffset, streamObject.getStartOffset());
            assertEquals(endOffset, streamObject.getEndOffset());

            metadataList.add(
                new S3ObjectMetadata(streamObject.getObjectId(), S3ObjectType.STREAM, List.of(new StreamOffsetRange(streamObject.getStreamId(),
                    streamObject.getStartOffset(), streamObject.getEndOffset())), 0, System.currentTimeMillis(), streamObject.getObjectSize(),
                    S3StreamConstant.INVALID_ORDER_ID));
        }
        return metadataList;
    }

    @Test
    void testPrepareCompactGroups() {
        // check if we can filter groups without limit of timestamp
        StreamObjectsCompactionTask task1 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 0);

        long currentTimestamp = System.currentTimeMillis();
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(List.of(
                new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 5, 10)), 0, currentTimestamp, 150,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 10, 20)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 40, 50)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 50, 60)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 65, 70)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 70, 80)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID)));
        Queue<List<StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper>> compactGroups = task1.prepareCompactGroups(0);
        assertEquals(2, compactGroups.size());

        assertEquals(List.of(
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 40, 50)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 50, 60)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID)),
            Objects.requireNonNull(compactGroups.poll()).stream().map(StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper::s3StreamObjectMetadata).collect(Collectors.toList()));
        assertEquals(List.of(
                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 65, 70)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 70, 80)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID)),
            Objects.requireNonNull(compactGroups.poll()).stream().map(StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper::s3StreamObjectMetadata).collect(Collectors.toList()));
        assertEquals(10, task1.getNextStartSearchingOffset());

        // check if we can filter two groups with limit of timestamp
        StreamObjectsCompactionTask task2 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 10000);

        currentTimestamp = System.currentTimeMillis();
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(List.of(
                new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 5, 10)), 0, currentTimestamp - 20000, 60,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 10, 40)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 40, 50)), 0, currentTimestamp - 20000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(4, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 50, 60)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 60, 70)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 70, 80)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(7, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 80, 90)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(8, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 90, 95)), 0, currentTimestamp - 30000, 80,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(9, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 95, 99)), 0, currentTimestamp, 20,
                    S3StreamConstant.INVALID_ORDER_ID)));
        compactGroups = task2.prepareCompactGroups(0);
        assertEquals(1, compactGroups.size());

        assertEquals(List.of(
                new S3ObjectMetadata(5, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 60, 70)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(6, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 70, 80)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(7, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 80, 90)), 0, currentTimestamp - 30000, 20,
                    S3StreamConstant.INVALID_ORDER_ID)),
            Objects.requireNonNull(compactGroups.poll()).stream().map(StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper::s3StreamObjectMetadata).collect(Collectors.toList()));
        assertEquals(5, task2.getNextStartSearchingOffset());

        // check if we can split big objects.
        StreamObjectsCompactionTask task3 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 10 * MAX_PART_SIZE, 0);

        currentTimestamp = System.currentTimeMillis();
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(List.of(
                new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 5, 10)), 0, currentTimestamp, MAX_PART_SIZE + 10,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 10, 20)), 0, currentTimestamp, 2 * MAX_PART_SIZE,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 20, 30)), 0, currentTimestamp, MAX_PART_SIZE - 1,
                    S3StreamConstant.INVALID_ORDER_ID)
            ));
        compactGroups = task3.prepareCompactGroups(0);
        assertEquals(1, compactGroups.size());

        List<StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper> wrappers = compactGroups.poll();
        assert wrappers != null;
        assertEquals(3, wrappers.size());

        assertEquals(List.of(
                new S3ObjectMetadata(1, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 5, 10)), 0, currentTimestamp, MAX_PART_SIZE + 10,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(2, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 10, 20)), 0, currentTimestamp, 2 * MAX_PART_SIZE,
                    S3StreamConstant.INVALID_ORDER_ID),
                new S3ObjectMetadata(3, S3ObjectType.STREAM, List.of(new StreamOffsetRange(1, 20, 30)), 0, currentTimestamp, MAX_PART_SIZE - 1,
                    S3StreamConstant.INVALID_ORDER_ID)
            ),
            Objects.requireNonNull(wrappers).stream().map(StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper::s3StreamObjectMetadata).collect(Collectors.toList()));
        assertEquals(List.of(2, 2, 1),
            Objects.requireNonNull(wrappers).stream().map(StreamObjectsCompactionTask.S3StreamObjectMetadataSplitWrapper::splitCopyCount).collect(Collectors.toList()));
        assertEquals(5, task3.getNextStartSearchingOffset());
    }
}