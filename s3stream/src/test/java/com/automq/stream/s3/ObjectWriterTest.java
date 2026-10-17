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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.operator.Writer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class ObjectWriterTest {

    /**
     * Pending encoded blocks totaling more than int can still be drained into separate upload parts.
     * Mocked block sizes exercise the accounting without allocating gigabytes of payload.
     */
    @Test
    public void testPendingBlockBytesUseLongAccounting() {
        ObjectStorage storage = mock(ObjectStorage.class);
        Writer writer = mock(Writer.class);
        when(storage.writer(any(), anyString())).thenReturn(writer);
        when(writer.write(any())).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            buffer.release();
            return CompletableFuture.completedFuture(null);
        });
        StreamRecordBatch record = newRecord(42, 0, 1, 1);
        try (MockedConstruction<ObjectWriter.DataBlock> blocks = mockConstruction(ObjectWriter.DataBlock.class, (block, context) -> {
            when(block.size()).thenReturn(1_000_000_000);
            when(block.buffer()).thenAnswer(invocation -> Unpooled.buffer(1).writeByte(0));
        })) {
            ObjectWriter objectWriter = ObjectWriter.writer(1, storage, 1, 16 * 1024 * 1024);
            objectWriter.write(42, List.of(record, record, record));
            assertEquals(3, blocks.constructed().size());
            verify(writer, times(3)).write(any());
        } finally {
            record.release();
        }
    }

    /**
     * Parts split before encoded block sizes exceed ByteBuf capacity, preserving the tail and stream ranges.
     * Mocked sizes and one-byte buffers avoid allocating large payloads.
     */
    @Test
    public void testPartSplitsBeforeExceedingBufferCapacity() {
        int blockSize = 900 * 1024 * 1024;
        ObjectStorage storage = mock(ObjectStorage.class);
        Writer writer = mock(Writer.class);
        List<byte[]> writes = new ArrayList<>();
        List<ByteBuf> buffers = new ArrayList<>();
        when(storage.writer(any(), anyString())).thenReturn(writer);
        when(writer.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.write(any())).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            writes.add(bytes);
            buffer.release();
            return CompletableFuture.completedFuture(null);
        });
        List<StreamRecordBatch> records = List.of(newRecord(42, 0, 1, 1), newRecord(43, 0, 1, 1), newRecord(44, 0, 1, 1));
        try (MockedConstruction<ObjectWriter.DataBlock> blocks = mockConstruction(ObjectWriter.DataBlock.class, (block, context) -> {
            long streamId = (long) context.arguments().get(0);
            when(block.size()).thenReturn(blockSize);
            when(block.recordCount()).thenReturn(1);
            when(block.getStreamRange()).thenReturn(new ObjectStreamRange(streamId, 0, 0, 1));
            when(block.buffer()).thenAnswer(invocation -> {
                ByteBuf buffer = Unpooled.buffer(1).writeByte(context.getCount());
                buffers.add(buffer);
                return buffer;
            });
        })) {
            ObjectWriter objectWriter = ObjectWriter.writer(1, storage, 1, Integer.MAX_VALUE);
            objectWriter.write(42, List.of(records.get(0)));
            objectWriter.write(43, List.of(records.get(1)));
            assertEquals(0, writes.size());
            objectWriter.write(44, List.of(records.get(2)));
            assertEquals(3, blocks.constructed().size());
            assertEquals(1, writes.size());
            Assertions.assertArrayEquals(new byte[] {1, 2}, writes.get(0));

            objectWriter.close().join();
            assertEquals(2, writes.size());
            assertEquals(3, writes.get(1)[0]);
            assertEquals(1 + 3 * DataBlockIndex.BLOCK_INDEX_SIZE + ObjectWriter.Footer.FOOTER_SIZE, writes.get(1).length);
            assertEquals(3L * blockSize + 3 * DataBlockIndex.BLOCK_INDEX_SIZE + ObjectWriter.Footer.FOOTER_SIZE, objectWriter.size());
            assertEquals(List.of(42L, 43L, 44L), objectWriter.getStreamRanges().stream().map(ObjectStreamRange::getStreamId).toList());
            buffers.forEach(buffer -> assertEquals(0, buffer.refCnt()));
        } finally {
            records.forEach(StreamRecordBatch::release);
        }
    }

    /**
     * Encoded record headers count toward block capacity even when the payload threshold has not been reached.
     * Both record and block sizes are mocked, with no large allocations.
     */
    @Test
    public void testBlockGroupingUsesEncodedCapacity() {
        ObjectStorage storage = mock(ObjectStorage.class);
        Writer writer = mock(Writer.class);
        when(storage.writer(any(), anyString())).thenReturn(writer);
        when(writer.write(any())).thenAnswer(invocation -> {
            ((ByteBuf) invocation.getArgument(0)).release();
            return CompletableFuture.completedFuture(null);
        });
        StreamRecordBatch record = mock(StreamRecordBatch.class);
        ByteBuf encoded = mock(ByteBuf.class);
        when(encoded.readableBytes()).thenReturn(1 << 30);
        when(record.encoded()).thenReturn(encoded);
        when(record.size()).thenReturn(1);
        try (MockedConstruction<ObjectWriter.DataBlock> blocks = mockConstruction(ObjectWriter.DataBlock.class, (block, context) -> {
            assertEquals(1, ((List<?>) context.arguments().get(1)).size());
            when(block.size()).thenReturn((1 << 30) + ObjectWriter.DataBlock.BLOCK_HEADER_SIZE);
            when(block.buffer()).thenAnswer(invocation -> Unpooled.buffer(1).writeByte(0));
        })) {
            ObjectWriter objectWriter = ObjectWriter.writer(1, storage, Integer.MAX_VALUE, 16 * 1024 * 1024);
            objectWriter.write(42, List.of(record, record));
            assertEquals(2, blocks.constructed().size());
            verify(writer, times(2)).write(any());
        }
    }

    /**
     * Separate writes start independent payload budgets while the final record may cross the soft threshold.
     */
    @Test
    public void testBlockGroupingDoesNotCarryPayloadAcrossWrites() {
        ObjectStorage storage = mock(ObjectStorage.class);
        Writer writer = mock(Writer.class);
        when(storage.writer(any(), anyString())).thenReturn(writer);
        when(writer.close()).thenReturn(CompletableFuture.completedFuture(null));
        when(writer.write(any())).thenAnswer(invocation -> {
            ((ByteBuf) invocation.getArgument(0)).release();
            return CompletableFuture.completedFuture(null);
        });
        List<StreamRecordBatch> records = List.of(
            newRecord(42, 0, 1, 80), newRecord(42, 1, 1, 30), newRecord(42, 2, 1, 80), newRecord(42, 3, 1, 1));
        List<List<StreamRecordBatch>> groups = new ArrayList<>();
        try (MockedConstruction<ObjectWriter.DataBlock> blocks = mockConstruction(ObjectWriter.DataBlock.class, (block, context) -> {
            @SuppressWarnings("unchecked")
            List<StreamRecordBatch> group = (List<StreamRecordBatch>) context.arguments().get(1);
            groups.add(List.copyOf(group));
            when(block.size()).thenReturn(1);
            when(block.recordCount()).thenReturn(group.size());
            when(block.getStreamRange()).thenReturn(new ObjectStreamRange(42, 0, group.get(0).getBaseOffset(),
                group.get(group.size() - 1).getLastOffset()));
            when(block.buffer()).thenAnswer(invocation -> Unpooled.buffer(1).writeByte(0));
        })) {
            ObjectWriter objectWriter = ObjectWriter.writer(1, storage, 100, 16 * 1024 * 1024);
            objectWriter.write(42, List.of(records.get(0)));
            objectWriter.write(42, records.subList(1, 4));
            assertEquals(List.of(List.of(records.get(0)), List.of(records.get(1), records.get(2)), List.of(records.get(3))), groups);
            assertEquals(3, blocks.constructed().size());
            objectWriter.close().join();
        } finally {
            records.forEach(StreamRecordBatch::release);
        }
    }

    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM_SET);

        ObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = ObjectWriter.writer(1, objectStorage, 1024, 1024);
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

        int objectSize = objectStorage.rangeRead(new ReadOptions().bucket(metadata.bucket()), metadata.key(), 0L, objectWriter.size()).get().readableBytes();
        assertEquals(objectSize, objectWriter.size());

        metadata = new S3ObjectMetadata(1, objectSize, S3ObjectType.STREAM_SET);
        ObjectReader objectReader = ObjectReader.reader(metadata, objectStorage);
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

    @Test
    public void testWrite_check() {
        S3ObjectMetadata metadata = new S3ObjectMetadata(1, 0, S3ObjectType.STREAM_SET);

        ObjectStorage objectStorage = new MemoryObjectStorage();
        ObjectWriter objectWriter = ObjectWriter.writer(1, objectStorage, 1024, 1024);
        objectWriter.write(233, List.of(
            newRecord(233, 10, 5, 512),
            newRecord(233, 15, 5, 512)
        ));

        // write smaller stream
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> objectWriter.write(231, List.of(newRecord(231, 10, 5, 512))));

        objectWriter.write(233, List.of(newRecord(233, 20, 5, 512)));

        // write smaller offset
        Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> objectWriter.write(233, List.of(newRecord(233, 10, 5, 512))));

        objectWriter.write(234, List.of(newRecord(234, 0, 5, 512)));
    }

    StreamRecordBatch newRecord(long streamId, long offset, int count, int payloadSize) {
        return StreamRecordBatch.of(streamId, 0, offset, count, TestUtils.random(payloadSize), DefaultByteBufSupplier.INSTANCE);
    }
}
