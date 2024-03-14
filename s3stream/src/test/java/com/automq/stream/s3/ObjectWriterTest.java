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

package com.automq.stream.s3;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("S3Unit")
public class ObjectWriterTest {

    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.SST);

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

        metadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.SST);
        ObjectReader objectReader = new ObjectReader(metadata, s3Operator);
        List<ObjectReader.DataBlockIndex> blockIndexes = objectReader.find(233, 10, 30).get();
        assertEquals(2, blockIndexes.size());
        {
            Iterator<StreamRecordBatch> it = objectReader.read(blockIndexes.get(0)).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(10L, r.getBaseOffset());
            assertEquals(5L, r.getRecordBatch().count());
            assertEquals(r1.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            r.release();
            r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(15L, r.getBaseOffset());
            assertEquals(10L, r.getRecordBatch().count());
            assertEquals(r2.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            assertFalse(it.hasNext());
            r.release();
        }

        {
            Iterator<StreamRecordBatch> it = objectReader.read(blockIndexes.get(1)).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(233L, r.getStreamId());
            assertEquals(25L, r.getBaseOffset());
            assertEquals(5L, r.getRecordBatch().count());
            assertEquals(r3.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            r.release();
        }

        blockIndexes = objectReader.find(234, 1, 2).get();
        assertEquals(1, blockIndexes.size());
        assertEquals(2, blockIndexes.get(0).blockId());
        {
            Iterator<StreamRecordBatch> it = objectReader.read(blockIndexes.get(0)).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(234L, r.getStreamId());
            assertEquals(0L, r.getBaseOffset());
            assertEquals(5L, r.getRecordBatch().count());
            assertEquals(r4.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            assertFalse(it.hasNext());
            r.release();
        }
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }
}
