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

import com.automq.elasticstream.client.api.RecordBatch;
import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.ReadDataBlock;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class DefaultS3BlockCacheTest {
    ObjectManager objectManager;
    S3Operator s3Operator;
    DefaultS3BlockCache s3BlockCache;

    @BeforeEach
    public void setup() {
        objectManager = mock(ObjectManager.class);
        s3Operator = new MemoryS3Operator();
        s3BlockCache = new DefaultS3BlockCache(objectManager, s3Operator);
    }

    @Test
    public void testRead() throws Exception {
        ObjectWriter objectWriter = new ObjectWriter(0, s3Operator, 1024, 1024);
        objectWriter.write(newRecord(233, 10, 5, 512));
        objectWriter.write(newRecord(233, 15, 10, 512));
        objectWriter.write(newRecord(233, 25, 5, 512));
        objectWriter.write(newRecord(234, 0, 5, 512));
        objectWriter.close();
        S3ObjectMetadata metadata1 = new S3ObjectMetadata(0, objectWriter.size(), S3ObjectType.WAL_LOOSE);

        objectWriter = new ObjectWriter(1, s3Operator, 1024, 1024);
        objectWriter.write(newRecord(233, 30, 10, 512));
        objectWriter.close();
        S3ObjectMetadata metadata2 = new S3ObjectMetadata(1, objectWriter.size(), S3ObjectType.WAL_LOOSE);

        objectWriter = new ObjectWriter(2, s3Operator, 1024, 1024);
        objectWriter.write(newRecord(233, 40, 20, 512));
        objectWriter.close();
        S3ObjectMetadata metadata3 = new S3ObjectMetadata(2, objectWriter.size(), S3ObjectType.WAL_LOOSE);

        when(objectManager.getObjects(eq(233L), eq(11L), eq(60L), eq(2))).thenReturn(List.of(
                metadata1, metadata2
        ));
        when(objectManager.getObjects(eq(233L), eq(40L), eq(60L), eq(2))).thenReturn(List.of(
                metadata3
        ));

        ReadDataBlock rst = s3BlockCache.read(233L, 11L, 60L, 10000).get(3, TimeUnit.SECONDS);
        assertEquals(5, rst.getRecords().size());
        assertEquals(10, rst.getRecords().get(0).getBaseOffset());
        assertEquals(60, rst.getRecords().get(4).getLastOffset());
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        RecordBatch recordBatch = DefaultRecordBatch.of(count, payloadSize);
        return new StreamRecordBatch(streamId, 0, offset, recordBatch);
    }


}
