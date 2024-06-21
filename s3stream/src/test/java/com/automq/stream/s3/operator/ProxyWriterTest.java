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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.network.ThrottleStrategy;
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
        when(operator.write(eq("testpath"), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertTrue(writer.hasBatchingPart());
        assertTrue(writer.close().isDone());
        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        ArgumentCaptor<ThrottleStrategy> captor2 = ArgumentCaptor.forClass(ThrottleStrategy.class);
        verify(operator, times(1)).write(eq("testpath"), captor.capture(), captor2.capture());
        Assertions.assertEquals(16 * 1024 * 1024, captor.getValue().readableBytes());
    }

    @Test
    public void testWrite_dataLargerThanMaxUploadSize() {
        when(operator.createMultipartUpload(eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPart(eq("testpath"), eq("test_upload_id"), eq(1), any(), any())).thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(1, "etag1")));
        when(operator.uploadPart(eq("testpath"), eq("test_upload_id"), eq(2), any(), any())).thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(2, "etag2")));
        when(operator.completeMultipartUpload(eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertTrue(writer.hasBatchingPart());
        assertNull(writer.multiPartWriter);
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertNotNull(writer.multiPartWriter);
        assertFalse(writer.hasBatchingPart());
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertNotNull(writer.multiPartWriter);
        assertFalse(writer.hasBatchingPart());
        writer.close();
        verify(operator, times(2)).uploadPart(any(), any(), anyInt(), any(), any());
    }

    @Test
    public void testWrite_copyWrite() {
        when(operator.createMultipartUpload(eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPartCopy(eq("test_src_path"), eq("testpath"), eq(0L), eq(15L * 1024 * 1024), eq("test_upload_id"), eq(1)))
            .thenReturn(CompletableFuture.completedFuture(new AbstractObjectStorage.ObjectStorageCompletedPart(1, "etag1")));
        when(operator.completeMultipartUpload(eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));

        S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(1, 15 * 1024 * 1024, S3ObjectType.STREAM);
        writer.copyWrite(s3ObjectMetadata, 0, 15 * 1024 * 1024);
        Assertions.assertTrue(writer.close().isDone());

        verify(operator, times(1)).uploadPartCopy(any(), any(), anyLong(), anyLong(), any(), anyInt());
    }

}
