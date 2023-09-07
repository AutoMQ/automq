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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.ObjectUtils;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("S3Unit")
public class StreamObjectCopyerTest {
    @Test
    public void testCompact() throws ExecutionException, InterruptedException {
        long targetObjectId = 10;
        long streamId = 233;

        S3Operator s3Operator = new MemoryS3Operator();

        ObjectWriter objectWriter1 = new ObjectWriter(1, s3Operator, 1024, 1024);
        StreamRecordBatch r1 = newRecord(streamId, 10, 5, 512);
        StreamRecordBatch r2 = newRecord(streamId, 15, 10, 512);
        objectWriter1.write(streamId, List.of(r1, r2));
        objectWriter1.close().get();

        ObjectWriter objectWriter2 = new ObjectWriter(2, s3Operator, 1024, 1024);
        StreamRecordBatch r3 = newRecord(streamId, 25, 8, 512);
        StreamRecordBatch r4 = newRecord(streamId, 33, 6, 512);
        objectWriter2.write(streamId, List.of(r3, r4));
        objectWriter2.close().get();

        S3ObjectMetadata metadata1 = new S3ObjectMetadata(1, objectWriter1.size(), S3ObjectType.STREAM);
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(2, objectWriter2.size(), S3ObjectType.STREAM);

        StreamObjectCopyer streamObjectCopyer = new StreamObjectCopyer(targetObjectId,
                s3Operator,
                // TODO: use a better clusterName
                s3Operator.writer(ObjectUtils.genKey(0, "todocluster", targetObjectId))
        );
        streamObjectCopyer.write(metadata1);
        streamObjectCopyer.write(metadata2);
        streamObjectCopyer.close().get();
        long targetObjectSize = streamObjectCopyer.size();
        S3ObjectMetadata metadata = new S3ObjectMetadata(targetObjectId, targetObjectSize, S3ObjectType.STREAM);


        int objectSize = s3Operator.rangeRead(metadata.key(), 0L, targetObjectSize).get().readableBytes();
        assertEquals(targetObjectSize, objectSize);

        ObjectReader objectReader = new ObjectReader(metadata, s3Operator);
        List<ObjectReader.DataBlockIndex> blockIndexes = objectReader.find(streamId, 10, 40).get();
        assertEquals(2, blockIndexes.size());
        {
            Iterator<StreamRecordBatch> it = objectReader.read(blockIndexes.get(0)).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(streamId, r.getStreamId());
            assertEquals(10L, r.getBaseOffset());
            assertEquals(5L, r.getRecordBatch().count());
            assertEquals(r1.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            r = it.next();
            assertEquals(streamId, r.getStreamId());
            assertEquals(15L, r.getBaseOffset());
            assertEquals(10L, r.getRecordBatch().count());
            assertEquals(r2.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            assertFalse(it.hasNext());
        }

        {
            Iterator<StreamRecordBatch> it = objectReader.read(blockIndexes.get(1)).get().iterator();
            StreamRecordBatch r = it.next();
            assertEquals(streamId, r.getStreamId());
            assertEquals(25L, r.getBaseOffset());
            assertEquals(8L, r.getRecordBatch().count());
            assertEquals(r3.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            r = it.next();
            assertEquals(streamId, r.getStreamId());
            assertEquals(33L, r.getBaseOffset());
            assertEquals(6L, r.getRecordBatch().count());
            assertEquals(r4.getRecordBatch().rawPayload(), r.getRecordBatch().rawPayload());
            assertFalse(it.hasNext());
        }
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return new StreamRecordBatch(streamId, 0, offset, count, TestUtils.random(payloadSize));
    }

}
