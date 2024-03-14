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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.TestUtils;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.concurrent.CompletableFuture;

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

    S3Operator operator;
    ProxyWriter writer;

    @BeforeEach
    public void setup() {
        operator = mock(S3Operator.class);
        writer = new ProxyWriter(operator, "testpath", null);
    }


    @Test
    public void testWrite_onePart() {
        writer.write(TestUtils.random(15 * 1024 * 1024));
        writer.write(TestUtils.random(1024 * 1024));
        when(operator.write(eq("testpath"), any())).thenReturn(CompletableFuture.completedFuture(null));
        assertTrue(writer.hasBatchingPart());
        assertTrue(writer.close().isDone());
        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(operator, times(1)).write(eq("testpath"), captor.capture());
        Assertions.assertEquals(16 * 1024 * 1024, captor.getValue().readableBytes());
    }

    @Test
    public void testWrite_dataLargerThanMaxUploadSize() {
        when(operator.createMultipartUpload(eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPart(eq("testpath"), eq("test_upload_id"), eq(1), any())).thenReturn(CompletableFuture.completedFuture(CompletedPart.builder().partNumber(1).eTag("etag1").build()));
        when(operator.uploadPart(eq("testpath"), eq("test_upload_id"), eq(2), any())).thenReturn(CompletableFuture.completedFuture(CompletedPart.builder().partNumber(1).eTag("etag2").build()));
        when(operator.completeMultipartUpload(eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertTrue(writer.hasBatchingPart());
        assertNull(writer.multiPartWriter);
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertTrue(writer.hasBatchingPart());
        writer.write(TestUtils.random(17 * 1024 * 1024));
        assertNotNull(writer.multiPartWriter);
        assertFalse(writer.hasBatchingPart());
        writer.close();
        verify(operator, times(2)).uploadPart(any(), any(), anyInt(), any());
    }

    @Test
    public void testWrite_copyWrite() {
        when(operator.createMultipartUpload(eq("testpath"))).thenReturn(CompletableFuture.completedFuture("test_upload_id"));
        when(operator.uploadPartCopy(eq("test_src_path"), eq("testpath"), eq(0L), eq(15L * 1024 * 1024), eq("test_upload_id"), eq(1)))
                .thenReturn(CompletableFuture.completedFuture(CompletedPart.builder().partNumber(1).eTag("etag1").build()));
        when(operator.completeMultipartUpload(eq("testpath"), eq("test_upload_id"), any())).thenReturn(CompletableFuture.completedFuture(null));

        writer.copyWrite("test_src_path", 0, 15 * 1024 * 1024);
        Assertions.assertTrue(writer.close().isDone());

        verify(operator, times(1)).uploadPartCopy(any(), any(), anyLong(), anyLong(), any(), anyInt());
    }

}
