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

package com.automq.stream.s3;

import com.automq.stream.s3.CompositeObjectReader.BasicObjectInfoExt;
import com.automq.stream.s3.CompositeObjectReader.ObjectIndex;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.metadata.S3StreamConstant.INVALID_TS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class CompositeObjectTest {

    @Test
    public void testCompositeObject_writeAndRead() throws ExecutionException, InterruptedException {
        ObjectStorage objectStorage = new MemoryObjectStorage((short) 234);
        // generate two normal object
        S3ObjectMetadata obj1;
        {
            S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM);
            ObjectWriter objectWriter = ObjectWriter.writer(1, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
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
            S3ObjectMetadata metadata = new S3ObjectMetadata(2, S3ObjectType.STREAM, null, INVALID_TS,
                INVALID_TS, 0, -1L, ObjectAttributes.builder().bucket((short) 233).build().attributes());
            ObjectWriter objectWriter = ObjectWriter.writer(2, objectStorage, Integer.MAX_VALUE, Integer.MAX_VALUE);
            StreamRecordBatch r1 = newRecord(233, 30, 10, genBuf((byte) 4, 512));
            objectWriter.write(233, List.of(r1));
            objectWriter.close().get();
            metadata.setObjectSize(objectWriter.size());
            obj2 = metadata;
        }

        // generate composite object from previous two objects
        long objectId = 3;
        CompositeObjectWriter compositeObjectWriter = new CompositeObjectWriter(objectStorage.writer(WriteOptions.DEFAULT, ObjectUtils.genKey(0, objectId)));
        compositeObjectWriter.addComponent(obj1, ObjectReader.reader(obj1, objectStorage).basicObjectInfo().get().indexBlock().indexes());
        compositeObjectWriter.addComponent(obj2, ObjectReader.reader(obj2, objectStorage).basicObjectInfo().get().indexBlock().indexes());
        compositeObjectWriter.close().get();

        // read composite object and verify
        S3ObjectMetadata metadata = new S3ObjectMetadata(3, objectId, S3ObjectType.COMPOSITE);
        metadata.setObjectSize(compositeObjectWriter.size());
        CompositeObjectReader reader = new CompositeObjectReader(metadata, (readOptions, metadata1, start, end) -> objectStorage.rangeRead(new ReadOptions().bucket(metadata1.bucket()).throttleStrategy(readOptions.throttleStrategy()), ObjectUtils.genKey(0, metadata1.objectId()), start, end));
        ObjectReader.BasicObjectInfo info = reader.basicObjectInfo().get();
        List<DataBlockIndex> indexes = info.indexBlock().indexes();
        assertEquals(3, indexes.size());
        List<ObjectIndex> objectIndexes = ((BasicObjectInfoExt) info).objectsBlock().indexes();
        assertEquals(2, objectIndexes.size());
        assertEquals(List.of(1L, 2L), objectIndexes.stream().map(ObjectIndex::objectId).collect(Collectors.toList()));
        assertEquals(List.of((short) 0, (short) 233), objectIndexes.stream().map(ObjectIndex::bucketId).collect(Collectors.toList()));
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
        return StreamRecordBatch.of(streamId, 0, offset, count, buf);
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

    private CompositeObjectReader getCompositeObjectReader(LongSupplier longSupplier, int linkedObjectNumber) {
        ArrayList<ObjectIndex> list = new ArrayList<>();
        for (int i = 0; i < linkedObjectNumber; i++) {
            ObjectIndex index = new ObjectIndex(longSupplier.getAsLong(), 0, 1000, (short) 0);
            list.add(index);
        }

        CompositeObjectReader.ObjectsBlock block = mock(CompositeObjectReader.ObjectsBlock.class);
        when(block.indexes()).then(invocationOnMock -> list);

        BasicObjectInfoExt ext = mock(BasicObjectInfoExt.class);
        when(ext.objectsBlock()).thenReturn(block);

        CompositeObjectReader reader = mock(CompositeObjectReader.class);
        when(reader.basicObjectInfo()).thenReturn(CompletableFuture.completedFuture(ext));

        return reader;
    }

    @Test
    public void batchDeleteCompositeObject() {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage();
        AtomicLong objectId = new AtomicLong(0);

        LongSupplier supplier = objectId::getAndIncrement;

        long compositeObjectNumber = 1002;
        int linkedObjectNumberPerObject = 200;

        List<CompletableFuture<Void>> cfs = new ArrayList<>();

        List<CompositeObjectReader> readers = new ArrayList<>();
        List<S3ObjectMetadata> metadataList = new ArrayList<>();
        for (long l = 0; l < compositeObjectNumber; l++) {
            CompositeObjectReader compositeObjectReader = getCompositeObjectReader(supplier, linkedObjectNumberPerObject);
            S3ObjectMetadata metadata = new S3ObjectMetadata(objectId.getAndIncrement(), 1024, S3ObjectType.COMPOSITE);

            readers.add(compositeObjectReader);
            metadataList.add(metadata);
        }

        List<CompletableFuture<ObjectStorage.WriteResult>> writeCf = new ArrayList<>();
        LongStream.range(0, objectId.get()).forEach(id -> {
            writeCf.add(objectStorage.write(WriteOptions.DEFAULT, ObjectUtils.genKey(0, id), ByteBufAlloc.byteBuffer(10)));
        });

        try {
            CompletableFuture.allOf(writeCf.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < readers.size(); i++) {
            CompositeObjectReader compositeObjectReader = readers.get(i);
            S3ObjectMetadata metadata = metadataList.get(i);

            CompletableFuture<Void> deleteCf = CompositeObject.deleteWithCompositeObjectReader(metadata, objectStorage, compositeObjectReader);
            cfs.add(deleteCf);
        }

        try {
            CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        Set<String> deleteObjectKeys = objectStorage.getDeleteObjectKeys();

        long l = objectId.get();

        // check all object deleted.
        LongStream.range(0, l).forEach(id -> {
            String key = ObjectUtils.genKey(0, id);
            assertTrue(deleteObjectKeys.contains(key), "expected object ");
        });

    }
}
