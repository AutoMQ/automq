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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void testWrite_dataLargerThanMaxUploadSize() {
        when(operator.createMultipartUpload(any(), eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPart(any(), eq("testpath"), eq("test_upload_id"), eq(1), any())).thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(1, "etag1", "checksum1")));
        when(operator.uploadPart(any(), eq("testpath"), eq("test_upload_id"), eq(2), any())).thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(2, "etag2", "checksum2")));
        when(operator.completeMultipartUpload(any(), eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertTrue(writer.hasBatchingPart());
        assertNull(writer.largeObjectWriter);
        writer.write(TestUtils.random(33 * 1024 * 1024));
        assertNotNull(writer.largeObjectWriter);
        assertFalse(writer.hasBatchingPart());
        writer.write(TestUtils.random(33 * 1024 * 1024));
        assertNotNull(writer.largeObjectWriter);
        assertFalse(writer.hasBatchingPart());
        writer.close();
        verify(operator, times(2)).uploadPart(any(), any(), any(), anyInt(), any());
    }

    @Test
    public void testWrite_copyWrite() {
        when(operator.createMultipartUpload(any(), eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPartCopy(any(), eq("test_src_path"), eq("testpath"), eq(0L), eq(15L * 1024 * 1024), eq("test_upload_id"), eq(1)))
            .thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(1, "etag1", "checksum1")));
        when(operator.completeMultipartUpload(any(), eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 15 * 1024 * 1024, S3ObjectType.STREAM);
        writer.copyWrite(s3ObjectMetadata, 0, 15 * 1024 * 1024);
        Assertions.assertTrue(writer.close().isDone());

        verify(operator, times(1)).uploadPartCopy(any(), any(), any(), anyLong(), anyLong(), any(), anyInt());
    }

    @Test
    public void testCopyWrite_smallObjectsUseSinglePut() {
        // Below minPartSize (5MiB default): should buffer via rangeRead and issue a single PutObject,
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
        assertNull(writer.largeObjectWriter);
        Assertions.assertTrue(writer.close().isDone());

        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator, times(1)).write(any(), eq("testpath"), captor.capture());
        Assertions.assertEquals(5 * 1024 * 1024, captor.getValue().readableBytes());
        verify(operator, times(0)).createMultipartUpload(any(), any());
        verify(operator, times(0)).uploadPart(any(), any(), any(), anyInt(), any());
        verify(operator, times(0)).completeMultipartUpload(any(), any(), any(), any());
    }

    @Test
    public void testCopyWrite_escalationPreservesOrder() {
        // Buffer many small copyWrites until the accumulated size crosses MAX_UPLOAD_SIZE (32MiB) and forces an
        // escalation to multipart mid-stream. The range reads only complete AFTER close() is called, so this
        // exercises the deferred-handoff path: the buffered data must be flushed to the MultiPartWriter as the
        // first part, before the copyWrite/write issued after escalation, and none of it may be dropped even
        // though close() has already been invoked. This is the scenario a MINOR/MAJOR group (up to 128MiB) hits.
        int mib = 1024 * 1024;
        when(operator.createMultipartUpload(any(), eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPart(any(), eq("testpath"), eq("test_upload_id"), anyInt(), any()))
            .thenAnswer(inv -> CompletableFuture.completedFuture(
                new AbstractObjectStorage.ObjectStorageCompletedPart(inv.getArgument(3), "etag", "checksum")));
        when(operator.completeMultipartUpload(any(), eq("testpath"), eq("test_upload_id"), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        // rangeRead hands back a not-yet-completed future per call so we can drive completion order by hand. The
        // ObjectWriter chains reads sequentially, so each read is only issued once the previous one completes.
        List<CompletableFuture<ByteBuf>> reads = new ArrayList<>();
        List<Integer> readSizes = new ArrayList<>();
        when(operator.rangeRead(any(), any(), anyLong(), anyLong())).thenAnswer(inv -> {
            long start = inv.getArgument(2);
            long end = inv.getArgument(3);
            CompletableFuture<ByteBuf> cf = new CompletableFuture<>();
            reads.add(cf);
            readSizes.add((int) (end - start));
            return cf;
        });

        // 9 * 4MiB = 36MiB > 32MiB, so the writer escalates after the 9th buffered copyWrite; the 10th copyWrite
        // is then issued directly to the multipart writer.
        int smallObjectCount = 10;
        for (int i = 1; i <= smallObjectCount; i++) {
            S3ObjectMetadata object = new S3ObjectMetadata(i, 4L * mib, S3ObjectType.STREAM);
            writer.copyWrite(object, 0, 4L * mib);
        }
        assertNotNull(writer.largeObjectWriter);
        // Index block written last, exactly like StreamObjectCompactor does after the per-object copyWrites.
        writer.write(TestUtils.random(mib));
        CompletableFuture<Void> closeCf = writer.close();
        assertFalse(closeCf.isDone(), "close must wait for the buffered range reads to be flushed");

        // Drive the reads to completion in issue order; completing one may lazily trigger the next.
        for (int i = 0; i < reads.size(); i++) {
            reads.get(i).complete(TestUtils.random(readSizes.get(i)));
        }

        assertTrue(closeCf.isDone());
        assertFalse(closeCf.isCompletedExceptionally());
        assertEquals(smallObjectCount, reads.size(), "one range read per small copyWrite");

        // Single-PUT path must not be used; the multipart upload must be completed exactly once.
        verify(operator, times(0)).write(any(), eq("testpath"), any());
        verify(operator, times(1)).completeMultipartUpload(any(), any(), any(), any());
        verify(operator, times(2)).uploadPart(any(), eq("testpath"), eq("test_upload_id"), anyInt(), any());

        // Part 1 must be the handoff of the 9 buffered objects (36MiB), in order - NOT the object copyWritten
        // after escalation. Part 2 is the 10th object (4MiB) plus the index block (1MiB). A reorder/drop would
        // make part 1 the 4MiB object and lose the 36MiB, so this pins down both ordering and no-data-loss.
        ArgumentCaptor<ByteBuf> part1 = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator).uploadPart(any(), eq("testpath"), eq("test_upload_id"), eq(1), part1.capture());
        assertEquals(9 * 4 * mib, part1.getValue().readableBytes());
        ArgumentCaptor<ByteBuf> part2 = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator).uploadPart(any(), eq("testpath"), eq("test_upload_id"), eq(2), part2.capture());
        assertEquals(4 * mib + mib, part2.getValue().readableBytes());
    }

}
