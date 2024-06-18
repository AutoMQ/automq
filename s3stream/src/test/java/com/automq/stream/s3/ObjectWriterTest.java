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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("S3Unit")
public class ObjectWriterTest {

    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM_SET);

        S3Operator s3Operator = new MemoryS3Operator();
        ObjectWriter objectWriter = ObjectWriter.writer(1, s3Operator, 1024, 1024);
        StreamRecordBatch r1 = newRecord(233, 10, 5, 512);
        StreamRecordBatch r2 = newRecord(233, 15, 10, 512);
        StreamRecordBatch r3 = newRecord(233, 25, 5, 512);
        objectWriter.write(233, List.of(r1, r2, r3));

        StreamRecordBatch r4 = newRecord(234, 0, 5, 512);
        objectWriter.write(234, List.of(r4));
        objectWriter.close().get();

        List<ObjectStreamRange> streamRanges = objectWriter.getStreamRanges();
        assertEquals(2, streamRanges.size());
        assertEquals(233, streamRanges.get(0).getStreamId());
        assertEquals(10, streamRanges.get(0).getStartOffset());
        assertEquals(30, streamRanges.get(0).getEndOffset());
        assertEquals(234, streamRanges.get(1).getStreamId());
        assertEquals(0, streamRanges.get(1).getStartOffset());
        assertEquals(5, streamRanges.get(1).getEndOffset());

        int objectSize = s3Operator.rangeRead(metadata.key(), 0L, objectWriter.size()).get().readableBytes();
        assertEquals(objectSize, objectWriter.size());

        metadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM_SET);
        ObjectReader objectReader = ObjectReader.reader(metadata, s3Operator);
        List<StreamDataBlock> streamDataBlocks = objectReader.find(233, 10, 30).get().streamDataBlocks();
        assertEquals(2, streamDataBlocks.size());
        {
            Iterator<StreamRecordBatch> it = objectReader.read(streamDataBlocks.get(0).dataBlockIndex()).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(10L, r.getBaseOffset());
            assertEquals(5L, r.getCount());
            assertEquals(r1.getPayload(), r.getPayload());
            r.release();
            r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(15L, r.getBaseOffset());
            assertEquals(10L, r.getCount());
            assertEquals(r2.getPayload(), r.getPayload());
            assertFalse(it.hasNext());
            r.release();
        }

        {
            Iterator<StreamRecordBatch> it = objectReader.read(streamDataBlocks.get(1).dataBlockIndex()).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(25L, r.getBaseOffset());
            assertEquals(5L, r.getCount());
            assertEquals(r3.getPayload(), r.getPayload());
            r.release();
        }

        streamDataBlocks = objectReader.find(234, 1, 2).get().streamDataBlocks();
        assertEquals(1, streamDataBlocks.size());
        assertEquals(0, streamDataBlocks.get(0).getStartOffset());
        {
            Iterator<StreamRecordBatch> it = objectReader.read(streamDataBlocks.get(0).dataBlockIndex()).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(234L, r.getStreamId());
            assertEquals(0L, r.getBaseOffset());
            assertEquals(5L, r.getCount());
            assertEquals(r4.getPayload(), r.getPayload());
            assertFalse(it.hasNext());
            r.release();
        }
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}
