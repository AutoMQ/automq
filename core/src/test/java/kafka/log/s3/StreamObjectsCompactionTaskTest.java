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
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitStreamObjectRequest;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.CommitWALObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamObjectMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static kafka.log.s3.TestUtils.random;
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
        List<S3StreamObjectMetadata> metadataList = prepareRawStreamObjects(10, stream.streamId(), objectsDetails);

        // two stream object groups should be handled
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(metadataList);
        StreamObjectsCompactionTask task = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, Long.MAX_VALUE, 0);

        AtomicLong objectIdAlloc = new AtomicLong(100);
        List<CommitStreamObjectRequest> committedRequests = new ArrayList<>();
        when(objectManager.prepareObject(anyInt(), anyLong())).thenAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement()));
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
                new S3StreamObjectMetadata(new S3StreamObject(1, 150, 1, 5, 10), 0),
                new S3StreamObjectMetadata(new S3StreamObject(2, 20, 1, 10, 20), 0),
                new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), 0),
                new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), 0),
                new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 65, 70), 0),
                new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), 0)
            ));
        when(objectManager.prepareObject(anyInt(), anyLong())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("halt compaction task")));

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
     * @throws ExecutionException
     * @throws InterruptedException
     */
    List<S3StreamObjectMetadata> prepareRawStreamObjects(long startObjectId, long streamId,
        List<List<Long>> objectsDetails) throws ExecutionException, InterruptedException {
        AtomicLong objectIdAlloc = new AtomicLong(startObjectId);
        Stack<CommitWALObjectRequest> commitWALObjectRequests = new Stack<>();
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitWALObject(any())).thenAnswer(invocation -> {
            commitWALObjectRequests.push(invocation.getArgument(0));
            return CompletableFuture.completedFuture(new CommitWALObjectResponse());
        });

        List<S3StreamObjectMetadata> metadataList = new ArrayList<>();

        for (int i = 0; i < objectsDetails.size(); i++) {
            List<Long> objectsDetail = objectsDetails.get(i);
            long startOffset = objectsDetail.get(0);
            long endOffset = objectsDetail.get(1);
            int recordsSize = Math.toIntExact(objectsDetail.get(2));

            Map<Long, List<StreamRecordBatch>> map = Map.of(streamId, List.of(new StreamRecordBatch(streamId, 0, startOffset, Math.toIntExact(endOffset - startOffset), random(recordsSize))));
            WALObjectUploadTask walObjectUploadTask = new WALObjectUploadTask(map, objectManager, s3Operator, 16 * 1024 * 1024, 16 * 1024 * 1024, recordsSize - 1);

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

            metadataList.add(new S3StreamObjectMetadata(new S3StreamObject(streamObject.getObjectId(), streamObject.getObjectSize(), streamObject.getStreamId(), streamObject.getStartOffset(), streamObject.getEndOffset()), System.currentTimeMillis()));
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
                new S3StreamObjectMetadata(new S3StreamObject(1, 150, 1, 5, 10), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(2, 20, 1, 10, 20), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 65, 70), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp)
            ));
        Queue<List<S3StreamObjectMetadata>> compactGroups = task1.prepareCompactGroups(0);
        assertEquals(2, compactGroups.size());

        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp),
            new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp)), compactGroups.poll());
        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 65, 70), currentTimestamp),
            new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp)), compactGroups.poll());
        assertEquals(10, task1.getNextStartSearchingOffset());

        // check if we can filter two groups with limit of timestamp
        StreamObjectsCompactionTask task2 = new StreamObjectsCompactionTask(objectManager, s3Operator, stream, 100, 10000);

        currentTimestamp = System.currentTimeMillis();
        when(objectManager.getStreamObjects(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(List.of(
                new S3StreamObjectMetadata(new S3StreamObject(1, 60, 1, 5, 10), currentTimestamp - 20000),
                new S3StreamObjectMetadata(new S3StreamObject(2, 20, 1, 10, 40), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(3, 20, 1, 40, 50), currentTimestamp - 20000),
                new S3StreamObjectMetadata(new S3StreamObject(4, 20, 1, 50, 60), currentTimestamp),
                new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 60, 70), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(7, 20, 1, 80, 90), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(8, 80, 1, 90, 95), currentTimestamp - 30000),
                new S3StreamObjectMetadata(new S3StreamObject(9, 20, 1, 95, 99), currentTimestamp)
            ));
        compactGroups = task2.prepareCompactGroups(0);
        assertEquals(1, compactGroups.size());

        assertEquals(List.of(new S3StreamObjectMetadata(new S3StreamObject(5, 20, 1, 60, 70), currentTimestamp - 30000),
            new S3StreamObjectMetadata(new S3StreamObject(6, 20, 1, 70, 80), currentTimestamp - 30000),
            new S3StreamObjectMetadata(new S3StreamObject(7, 20, 1, 80, 90), currentTimestamp - 30000)), compactGroups.poll());
        assertEquals(5, task2.getNextStartSearchingOffset());
    }
}