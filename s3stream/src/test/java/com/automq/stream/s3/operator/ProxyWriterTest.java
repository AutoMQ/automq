/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
        writer = new ProxyWriter(ObjectStorage.WriteOptions.DEFAULT, operator, "testpath");
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
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertNotNull(writer.largeObjectWriter);
        assertFalse(writer.hasBatchingPart());
        writer.write(TestUtils.random(17 * 1024 * 1024));
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

}
