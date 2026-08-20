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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class ProxyWriterTest {

    AbstractObjectStorage operator;
    ProxyWriter writer;

    @BeforeEach
    public void setup() {
        operator = mock(AbstractObjectStorage.class);
        writer = new ProxyWriter(ObjectStorage.WriteOptions.DEFAULT.copy().bucketId((short) 0), operator, "testpath");
    }

    @Test
    public void testWrite_onePart() {
        writer.write(TestUtils.random(15 * 1024 * 1024));
        writer.write(TestUtils.random(1024 * 1024));
        when(operator.write(any(), eq("testpath"), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertTrue(writer.hasBatchingPart());
        assertTrue(writer.close().isDone());
        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator, times(1)).write(any(), eq("testpath"), captor.capture());
        Assertions.assertEquals(16 * 1024 * 1024, captor.getValue().readableBytes());
    }

    @Test
    public void testSmallCopyWritesExceedingMultipartUploadThreshold() {
        // Given small copy operations total exactly 32 MiB, stay on the PutObject path until one additional byte
        // makes the cumulative object size exceed the multipart upload threshold.
        when(operator.createMultipartUpload(any(), eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.rangeRead(any(), any(), anyLong(), anyLong())).thenAnswer(inv ->
            CompletableFuture.completedFuture(TestUtils.random((int) ((long) inv.getArgument(3) - (long) inv.getArgument(2)))));
        when(operator.uploadPart(any(), eq("testpath"), eq("test_upload_id"), anyInt(), any())).thenAnswer(inv ->
            CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(
                inv.getArgument(3), "etag", "checksum")));
        when(operator.completeMultipartUpload(any(), eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));

        for (int i = 0; i < 8; i++) {
            S3ObjectMetadata object = new S3ObjectMetadata(i + 1, 4L * 1024 * 1024, S3ObjectType.STREAM);
            writer.copyWrite(object, 0, 4L * 1024 * 1024);
        }
        verify(operator, times(0)).createMultipartUpload(any(), any());
        verify(operator, times(0)).rangeRead(any(), any(), anyLong(), anyLong());

        S3ObjectMetadata lastObject = new S3ObjectMetadata(9, 1, S3ObjectType.STREAM);
        writer.copyWrite(lastObject, 0, 1);
        assertTrue(writer.close().isDone());

        verify(operator, times(1)).createMultipartUpload(any(), eq("testpath"));
        verify(operator, times(0)).write(any(), eq("testpath"), any());
    }

    @Test
    public void testStandaloneLargeCopyUsesPutObject() {
        // Given a standalone copy remains below the multipart threshold, use rangeRead plus one PutObject even
        // when it is large enough for UploadPartCopy.
        when(operator.rangeRead(any(), any(), eq(0L), eq(15L * 1024 * 1024)))
            .thenReturn(CompletableFuture.completedFuture(TestUtils.random(15 * 1024 * 1024)));
        when(operator.write(any(), eq("testpath"), any())).thenReturn(CompletableFuture.completedFuture(null));

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 15 * 1024 * 1024, S3ObjectType.STREAM);
        writer.copyWrite(s3ObjectMetadata, 0, 15 * 1024 * 1024);
        Assertions.assertTrue(writer.close().isDone());

        verify(operator, times(1)).rangeRead(any(), any(), eq(0L), eq(15L * 1024 * 1024));
        verify(operator, times(1)).write(any(), eq("testpath"), any());
        verify(operator, times(0)).createMultipartUpload(any(), any());
    }

    @Test
    public void testCopyWrite_smallObjectsUseSinglePut() {
        // Below minPartSize (5MiB default): should buffer via rangeRead and issue one PutObject request,
        // never touching the multipart APIs (createMultipartUpload/uploadPart/completeMultipartUpload).
        when(operator.write(any(), eq("testpath"), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(operator.rangeRead(any(), any(), eq(0L), eq(2L * 1024 * 1024)))
            .thenReturn(CompletableFuture.completedFuture(TestUtils.random(2 * 1024 * 1024)));
        when(operator.rangeRead(any(), any(), eq(0L), eq(3L * 1024 * 1024)))
            .thenReturn(CompletableFuture.completedFuture(TestUtils.random(3 * 1024 * 1024)));

        S3ObjectMetadata object1 = new S3ObjectMetadata(1, 2 * 1024 * 1024, S3ObjectType.STREAM);
        S3ObjectMetadata object2 = new S3ObjectMetadata(2, 3 * 1024 * 1024, S3ObjectType.STREAM);
        writer.copyWrite(object1, 0, 2 * 1024 * 1024);
        writer.copyWrite(object2, 0, 3 * 1024 * 1024);
        verify(operator, times(0)).rangeRead(any(), any(), anyLong(), anyLong());
        Assertions.assertTrue(writer.close().isDone());

        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator, times(1)).write(any(), eq("testpath"), captor.capture());
        Assertions.assertEquals(5 * 1024 * 1024, captor.getValue().readableBytes());
        verify(operator, times(0)).createMultipartUpload(any(), any());
    }

    @Test
    public void testCopyWrite_escalationPreservesOrder() {
        // Given small copyWrites buffered before multipart selection, replay must preserve their call order before
        // operations issued after selection, independent of multipart boundaries.
        int mib = 1024 * 1024;
        when(operator.createMultipartUpload(any(), eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        Map<Integer, ByteBuf> uploadedParts = new HashMap<>();
        when(operator.uploadPart(any(), eq("testpath"), eq("test_upload_id"), anyInt(), any()))
            .thenAnswer(inv -> {
                int partNumber = inv.getArgument(3);
                uploadedParts.put(partNumber, inv.getArgument(4));
                return CompletableFuture.completedFuture(
                    new AbstractObjectStorage.ObjectStorageCompletedPart(partNumber, "etag", "checksum"));
            });
        when(operator.completeMultipartUpload(any(), eq("testpath"), eq("test_upload_id"), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        // rangeRead hands back a not-yet-completed future per call so we can drive completion order by hand. The
        // multipart replay chain issues each read only after the previous one completes.
        List<CompletableFuture<ByteBuf>> reads = new ArrayList<>();
        List<Integer> readSizes = new ArrayList<>();
        List<Byte> readMarkers = new ArrayList<>();
        Map<String, Byte> markers = new HashMap<>();
        when(operator.rangeRead(any(), any(), anyLong(), anyLong())).thenAnswer(inv -> {
            long start = inv.getArgument(2);
            long end = inv.getArgument(3);
            CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
            reads.add(cf);
            readSizes.add((int) (end - start));
            readMarkers.add(markers.get(inv.getArgument(1)));
            return cf;
        });

        // The first nine ranges total 33MiB and trigger selection. The tenth range is sent directly to the
        // selected writer.
        int[] copySizes = {4, 2, 3, 4, 4, 4, 4, 4, 4, 2};
        for (int i = 0; i < 9; i++) {
            S3ObjectMetadata object = new S3ObjectMetadata(i + 1, (long) copySizes[i] * mib, S3ObjectType.STREAM);
            markers.put(object.key(), (byte) (i + 1));
            writer.copyWrite(object, 0, (long) copySizes[i] * mib);
        }
        S3ObjectMetadata lastObject = new S3ObjectMetadata(10, (long) copySizes[9] * mib, S3ObjectType.STREAM);
        markers.put(lastObject.key(), (byte) 10);
        writer.copyWrite(lastObject, 0, (long) copySizes[9] * mib);
        // Index block written last, exactly like StreamObjectCompactor does after the per-object copyWrites.
        writer.write(TestUtils.random(mib));
        CompletableFuture<Void> closeCf = writer.close();
        assertFalse(closeCf.isDone(), "close must wait for the buffered range reads to be flushed");

        // Drive the reads to completion in issue order; completing one may lazily trigger the next.
        for (int i = 0; i < reads.size(); i++) {
            ByteBuf read = TestUtils.random(readSizes.get(i));
            read.setByte(0, readMarkers.get(i));
            reads.get(i).complete(read);
        }

        assertTrue(closeCf.isDone());
        assertFalse(closeCf.isCompletedExceptionally());
        // PutObject must not be used; the multipart upload must be completed exactly once.
        verify(operator, times(0)).write(any(), eq("testpath"), any());
        verify(operator, times(1)).completeMultipartUpload(any(), any(), any(), any());

        List<ByteBuf> parts = uploadedParts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();
        assertEquals(36L * mib, parts.stream().mapToLong(ByteBuf::readableBytes).sum());
        long operationOffset = 0;
        int partIndex = 0;
        long partOffset = 0;
        for (int i = 0; i < copySizes.length; i++) {
            while (operationOffset >= partOffset + parts.get(partIndex).readableBytes()) {
                partOffset += parts.get(partIndex).readableBytes();
                partIndex++;
            }
            assertEquals(i + 1, parts.get(partIndex).getByte((int) (operationOffset - partOffset)));
            operationOffset += (long) copySizes[i] * mib;
        }
    }

    @Test
    public void testCopyOnWriteSnapshotsPendingBuffers() {
        // Given an operation is still pending, copyOnWrite must snapshot it immediately so later caller mutation
        // cannot change the uploaded object.
        when(operator.write(any(), eq("testpath"), any())).thenReturn(CompletableFuture.completedFuture(null));
        ByteBuf source = TestUtils.randomPooled(16);
        byte originalFirstByte = source.getByte(0);
        writer.write(source.retainedDuplicate());

        writer.copyOnWrite();
        source.setByte(0, originalFirstByte + 1);
        writer.close();

        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator).write(any(), eq("testpath"), captor.capture());
        assertEquals(originalFirstByte, captor.getValue().getByte(0));
        source.release();
    }

    @Test
    public void testFailedCopyWriteReleasesFollowingPendingWrite() {
        // Given a range read fails, a later write buffer that cannot be appended must still be released.
        when(operator.rangeRead(any(), any(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("read failed")));
        S3ObjectMetadata object = new S3ObjectMetadata(1, 1024, S3ObjectType.STREAM);
        ByteBuf part = TestUtils.randomPooled(1024);
        writer.copyWrite(object, 0, 1024);
        writer.write(part);

        CompletableFuture<Void> closeCf = writer.close();

        assertTrue(closeCf.isCompletedExceptionally());
        assertEquals(0, part.refCnt());
        verify(operator, times(0)).write(any(), eq("testpath"), any());
    }

    @Test
    public void testFailedMultipartReplayReleasesOwnedBuffers() {
        // Given multipart replay has accumulated one range buffer, a later failed range read must release both
        // that accumulated buffer and a following write that can no longer be appended.
        when(operator.createMultipartUpload(any(), eq("testpath")))
            .thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPartCopy(any(), any(), eq("testpath"), anyLong(), anyLong(), eq("test_upload_id"), anyInt()))
            .thenAnswer(inv -> CompletableFuture.completedFuture(
                new AbstractObjectStorage.ObjectStorageCompletedPart(inv.getArgument(6), "etag", "checksum")));
        CompletableFuture<ByteBuf> firstReadCf = new CompletableFuture<>();
        when(operator.rangeRead(any(), any(), anyLong(), anyLong()))
            .thenReturn(firstReadCf)
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("read failed")));

        int mib = 1024 * 1024;
        writer.copyWrite(new S3ObjectMetadata(1, 30L * mib, S3ObjectType.STREAM), 0, 30L * mib);
        writer.copyWrite(new S3ObjectMetadata(2, mib, S3ObjectType.STREAM), 0, mib);
        writer.copyWrite(new S3ObjectMetadata(3, mib, S3ObjectType.STREAM), 0, mib);
        ByteBuf followingWrite = TestUtils.randomPooled(1);
        writer.write(followingWrite);
        CompletableFuture<Void> closeCf = writer.close();

        ByteBuf firstRead = TestUtils.randomPooled(mib);
        firstReadCf.complete(firstRead);

        assertTrue(closeCf.isCompletedExceptionally());
        assertEquals(0, firstRead.refCnt());
        assertEquals(0, followingWrite.refCnt());
        verify(operator, times(0)).uploadPart(any(), any(), any(), anyInt(), any());
    }

    @Test
    public void testReleaseDiscardsPendingOperationsWithoutStartingReads() {
        // Given no upload strategy has been selected, release owns and frees pending buffers without applying
        // copy operations or starting remote reads.
        S3ObjectMetadata object = new S3ObjectMetadata(1, 1024, S3ObjectType.STREAM);
        ByteBuf part = TestUtils.randomPooled(1024);
        writer.copyWrite(object, 0, 1024);
        writer.write(part);

        assertTrue(writer.release().isDone());

        assertEquals(0, part.refCnt());
        verify(operator, times(0)).rangeRead(any(), any(), anyLong(), anyLong());
    }

}
