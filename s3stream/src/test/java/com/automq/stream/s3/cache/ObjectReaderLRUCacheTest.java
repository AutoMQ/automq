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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ObjectReaderLRUCacheTest {

    private void writeStream(int streamCount, ObjectWriter objectWriter) {
        for (int i = 0; i < streamCount; i++) {
            StreamRecordBatch r = new StreamRecordBatch(i, 0, i, 1, TestUtils.random(1));
            objectWriter.write(i, List.of(r));
        }
    }

    @Test
    public void testGetPut() throws ExecutionException, InterruptedException {
        S3Operator s3Operator = new MemoryS3Operator();
        ObjectWriter objectWriter = ObjectWriter.writer(233L, s3Operator, 1024, 1024);
        writeStream(1000, objectWriter);
        objectWriter.close().get();

        ObjectWriter objectWriter2 = ObjectWriter.writer(234L, s3Operator, 1024, 1024);
        writeStream(2000, objectWriter2);
        objectWriter2.close().get();

        ObjectWriter objectWriter3 = ObjectWriter.writer(235L, s3Operator, 1024, 1024);
        writeStream(3000, objectWriter3);
        objectWriter3.close().get();

        ObjectReader objectReader = new ObjectReader(new S3ObjectMetadata(233L, objectWriter.size(), S3ObjectType.STREAM_SET), s3Operator);
        ObjectReader objectReader2 = new ObjectReader(new S3ObjectMetadata(234L, objectWriter2.size(), S3ObjectType.STREAM_SET), s3Operator);
        ObjectReader objectReader3 = new ObjectReader(new S3ObjectMetadata(235L, objectWriter3.size(), S3ObjectType.STREAM_SET), s3Operator);
        Assertions.assertEquals(36000, objectReader.basicObjectInfo().get().size());
        Assertions.assertEquals(72000, objectReader2.basicObjectInfo().get().size());
        Assertions.assertEquals(108000, objectReader3.basicObjectInfo().get().size());

        ObjectReaderLRUCache cache = new ObjectReaderLRUCache(100000);
        cache.put(235L, objectReader3);
        cache.put(234L, objectReader2);
        cache.put(233L, objectReader);
        Map.Entry<Long, ObjectReader> entry = cache.pop();
        Assertions.assertNotNull(entry);
        Assertions.assertEquals(objectReader2, entry.getValue());
    }
}
