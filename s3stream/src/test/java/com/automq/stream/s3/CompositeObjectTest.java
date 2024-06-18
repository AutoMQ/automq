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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompositeObjectTest {

    @Test
    public void testCompositeObject_writeAndRead() throws ExecutionException, InterruptedException {
        // TODO: replace s3operator to objectStorage
        S3Operator s3Operator = new MemoryS3Operator();
        // generate two normal object
        S3ObjectMetadata obj1;
        {
            S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM);
            ObjectWriter objectWriter = ObjectWriter.writer(1, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            StreamRecordBatch r1 = newRecord(233, 10, 5, genBuf((byte) 1, 512));
            StreamRecordBatch r2 = newRecord(233, 15, 10, genBuf((byte) 2, 512));
            objectWriter.write(233, List.of(r1, r2));
            StreamRecordBatch r3 = newRecord(233, 25, 5, genBuf((byte) 3, 512));
            objectWriter.write(233, List.of(r3));
            objectWriter.close().get();
            metadata.setObjectSize(objectWriter.size());
            obj1 = metadata;
        }
        S3ObjectMetadata obj2;
        {
            S3ObjectMetadata metadata = new S3ObjectMetadata(2, 0, S3ObjectType.STREAM);
            ObjectWriter objectWriter = ObjectWriter.writer(2, s3Operator, Integer.MAX_VALUE, Integer.MAX_VALUE);
            StreamRecordBatch r1 = newRecord(233, 30, 10, genBuf((byte) 4, 512));
            objectWriter.write(233, List.of(r1));
            objectWriter.close().get();
            metadata.setObjectSize(objectWriter.size());
            obj2 = metadata;
        }

        // generate composite object from previous two objects
        long objectId = 3;
        CompositeObjectWriter compositeObjectWriter = new CompositeObjectWriter(s3Operator.writer(ObjectUtils.genKey(0, objectId)));
        compositeObjectWriter.addComponent(obj1, ObjectReader.reader(obj1, s3Operator).basicObjectInfo().get().indexBlock().indexes());
        compositeObjectWriter.addComponent(obj2, ObjectReader.reader(obj2, s3Operator).basicObjectInfo().get().indexBlock().indexes());
        compositeObjectWriter.close().get();

        // read composite object and verify
        S3ObjectMetadata metadata = new S3ObjectMetadata(3, objectId, S3ObjectType.COMPOSITE);
        metadata.setObjectSize(compositeObjectWriter.size());
        CompositeObjectReader reader = new CompositeObjectReader(metadata, (metadata1, start, end) -> s3Operator.rangeRead(ObjectUtils.genKey(0, metadata1.objectId()), start, end));
        ObjectReader.BasicObjectInfo info = reader.basicObjectInfo().get();
        List<DataBlockIndex> indexes = info.indexBlock().indexes();
        Assertions.assertEquals(3, indexes.size());
        {
            ObjectReader.DataBlockGroup data = reader.read(indexes.get(0)).get();
            Iterator<StreamRecordBatch> it = data.iterator();
            assertTrue(verify(it.next().getPayload(), (byte) 1));
            assertTrue(verify(it.next().getPayload(), (byte) 2));
            assertFalse(it.hasNext());
        }
        {
            ObjectReader.DataBlockGroup data = reader.read(indexes.get(1)).get();
            Iterator<StreamRecordBatch> it = data.iterator();
            assertTrue(verify(it.next().getPayload(), (byte) 3));
            assertFalse(it.hasNext());
        }
        {
            ObjectReader.DataBlockGroup data = reader.read(indexes.get(2)).get();
            Iterator<StreamRecordBatch> it = data.iterator();
            assertTrue(verify(it.next().getPayload(), (byte) 4));
            assertFalse(it.hasNext());
        }
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, ByteBuf buf) {
        return new StreamRecordBatch(streamId, 0, offset, count, buf);
    }

    ByteBuf genBuf(byte data, int length) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, data);
        return Unpooled.wrappedBuffer(bytes);
    }

    boolean verify(ByteBuf buf, byte data) {
        buf = buf.duplicate();
        while (buf.readableBytes() > 0) {
            if (buf.readByte() != data) {
                return false;
            }
        }
        return true;
    }
}
