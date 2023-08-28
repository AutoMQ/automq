package kafka.log.s3;

import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitCompactObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MinorCompactTaskTest {
    ObjectManager objectManager;
    S3Operator s3Operator;
    MinorCompactTask minorCompactTask;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
        minorCompactTask = new MinorCompactTask(128 * 1024, 1000, 1024, objectManager, s3Operator);
    }

    @Test
    public void testTryCompact() {
        AtomicLong objectIdAlloc = new AtomicLong(10);
        doAnswer(invocation -> CompletableFuture.completedFuture(objectIdAlloc.getAndIncrement())).when(objectManager).prepareObject(anyInt(), anyLong());
        when(objectManager.commitMinorCompactObject(any())).thenReturn(CompletableFuture.completedFuture(null));

        List<StreamRecordBatch> records = List.of(
                new StreamRecordBatch(233, 0, 10, DefaultRecordBatch.of(2, 512)),
                new StreamRecordBatch(233, 0, 12, DefaultRecordBatch.of(2, 128)),
                new StreamRecordBatch(234, 0, 20, DefaultRecordBatch.of(2, 128))
        );
        minorCompactTask.tryCompact(new MinorCompactTask.MinorCompactPart(0, records));

        records = List.of(
                new StreamRecordBatch(233, 0, 14, DefaultRecordBatch.of(2, 512)),
                new StreamRecordBatch(234, 0, 22, DefaultRecordBatch.of(2, 128)),
                new StreamRecordBatch(235, 0, 11, DefaultRecordBatch.of(2, 128))
        );
        minorCompactTask.tryCompact(new MinorCompactTask.MinorCompactPart(1, records));
        records = List.of(
                new StreamRecordBatch(235, 1, 30, DefaultRecordBatch.of(2, 128))
        );
        minorCompactTask.tryCompact(new MinorCompactTask.MinorCompactPart(2, records));

        minorCompactTask.close();
        ArgumentCaptor<CommitCompactObjectRequest> reqArg = ArgumentCaptor.forClass(CommitCompactObjectRequest.class);
        verify(objectManager, times(1)).commitMinorCompactObject(reqArg.capture());
        // expect
        // - stream233 split
        // - stream234 write to one stream range
        // - stream235 with different epoch, write to two stream range.
        CommitCompactObjectRequest request = reqArg.getValue();
        assertEquals(10, request.getObjectId());
        assertEquals(List.of(0L, 1L, 2L), request.getCompactedObjectIds());
        assertEquals(3, request.getStreamRanges().size());
        assertEquals(234, request.getStreamRanges().get(0).getStreamId());
        assertEquals(20, request.getStreamRanges().get(0).getStartOffset());
        assertEquals(24, request.getStreamRanges().get(0).getEndOffset());
        assertEquals(235, request.getStreamRanges().get(1).getStreamId());
        assertEquals(11, request.getStreamRanges().get(1).getStartOffset());
        assertEquals(13, request.getStreamRanges().get(1).getEndOffset());
        assertEquals(235, request.getStreamRanges().get(2).getStreamId());
        assertEquals(30, request.getStreamRanges().get(2).getStartOffset());
        assertEquals(32, request.getStreamRanges().get(2).getEndOffset());

        assertEquals(1, request.getStreamObjects().size());
        StreamObject streamObject = request.getStreamObjects().get(0);
        assertEquals(233, streamObject.getStreamId());
        assertEquals(11, streamObject.getObjectId());
        assertEquals(10, streamObject.getStartOffset());
        assertEquals(16, streamObject.getEndOffset());
    }

}
