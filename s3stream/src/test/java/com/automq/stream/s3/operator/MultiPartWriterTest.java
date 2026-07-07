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
import com.automq.stream.s3.objects.ObjectAttributes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class MultiPartWriterTest {
    private S3AsyncClient s3;
    private AwsObjectStorage operator;
    private MultiPartWriter writer;
    private Lock lock;

    @BeforeEach
    void setUp() {
        s3 = mock(S3AsyncClient.class);
        operator = new AwsObjectStorage(s3, "unit-test-bucket");
        CreateMultipartUploadResponse.Builder builder = CreateMultipartUploadResponse.builder();
        when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(CompletableFuture.completedFuture(builder.build()));
        lock = new ReentrantLock();
    }

    @Test
    void testWrite() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ExecutionException, InterruptedException {
        writer = new MultiPartWriter(ObjectStorage.WriteOptions.DEFAULT, operator, "test-path", 100, 100);

        List<UploadPartRequest> requests = new ArrayList<>();
        List<Long> contentLengths = new ArrayList<>();

        UploadPartResponse.Builder builder = UploadPartResponse.builder();
        Method method = builder.getClass().getDeclaredMethod("setETag", String.class);
        method.setAccessible(true);
        method.invoke(builder, "unit-test-etag");

        when(s3.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenAnswer(invocation -> {
            UploadPartRequest request = invocation.getArgument(0);
            requests.add(request);
            AsyncRequestBody body = invocation.getArgument(1);
            contentLengths.add(body.contentLength().orElse(0L));
            return CompletableFuture.completedFuture(builder.build());
        });
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(CompletableFuture.completedFuture(null));
        writer.uploadIdCf.get();

        List<ByteBuf> payloads = List.of(
            // case 2
            TestUtils.random(120),
            // case 1
            TestUtils.random(20),
            // case 3
            TestUtils.random(40),
            // case 4
            TestUtils.random(60),
            // case 1
            TestUtils.random(80),
            // case 5
            TestUtils.random(200),
            // last part
            TestUtils.random(10)
        );

        payloads.forEach(writer::write);
        writer.close().get();
        assertEquals(4, requests.size());
        assertEquals("unit-test-bucket", requests.get(0).bucket());
        assertEquals("test-path", requests.get(0).key());
        assertEquals(List.of(1, 2, 3, 4), requests.stream()
            .map(UploadPartRequest::partNumber)
            .collect(Collectors.toList()));
        assertEquals(List.of(120L, 120L, 280L, 10L), contentLengths);
        assertEquals(md5Base64(payloads.get(0)), requests.get(0).contentMD5());
    }

    @Test
    void testWriteWithChecksumAlgorithm() throws Exception {
        operator = new AwsObjectStorage(s3, "unit-test-bucket", ChecksumAlgorithm.SHA256);
        writer = new MultiPartWriter(ObjectStorage.WriteOptions.DEFAULT, operator, "test-path-checksum", 100, 100);

        List<UploadPartRequest> requests = new ArrayList<>();
        List<CompleteMultipartUploadRequest> completeRequests = new ArrayList<>();

        UploadPartResponse response = UploadPartResponse.builder()
            .eTag("unit-test-etag")
            .checksumSHA256("part-checksum")
            .build();
        when(s3.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenAnswer(invocation -> {
            requests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(response);
        });
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenAnswer(invocation -> {
            completeRequests.add(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        writer.uploadIdCf.get();

        ByteBuf payload = TestUtils.random(120);
        writer.write(payload);
        writer.close().get();

        assertEquals(1, requests.size());
        assertNull(requests.get(0).contentMD5());
        assertNull(requests.get(0).checksumAlgorithm());
        assertEquals(sha256Base64(payload), requests.get(0).checksumSHA256());
        assertEquals(1, completeRequests.size());
        assertEquals("part-checksum", completeRequests.get(0).multipartUpload().parts().get(0).checksumSHA256());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCopyWrite() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ExecutionException, InterruptedException {
        writer = new MultiPartWriter(ObjectStorage.WriteOptions.DEFAULT, operator, "test-path-2", 100);
        List<UploadPartRequest> uploadPartRequests = new ArrayList<>();
        List<UploadPartCopyRequest> uploadPartCopyRequests = new ArrayList<>();
        List<Long> writeContentLengths = new ArrayList<>();

        UploadPartResponse.Builder builder = UploadPartResponse.builder();
        Method method = builder.getClass().getDeclaredMethod("setETag", String.class);
        method.setAccessible(true);
        method.invoke(builder, "unit-test-etag");

        CopyPartResult.Builder copyResultBuilder = CopyPartResult.builder();
        method = copyResultBuilder.getClass().getDeclaredMethod("setETag", String.class);
        method.setAccessible(true);
        method.invoke(copyResultBuilder, "unit-test-copy-etag");

        UploadPartCopyResponse.Builder copyBuilder = UploadPartCopyResponse.builder();
        method = copyBuilder.getClass().getDeclaredMethod("setCopyPartResult", copyResultBuilder.getClass());
        method.setAccessible(true);
        method.invoke(copyBuilder, copyResultBuilder);

        when(s3.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenAnswer(invocation -> {
            UploadPartRequest request = invocation.getArgument(0);
            lock.lock();
            try {
                uploadPartRequests.add(request);
                AsyncRequestBody body = invocation.getArgument(1);
                writeContentLengths.add(body.contentLength().orElse(0L));
            } finally {
                lock.unlock();
            }
            return CompletableFuture.completedFuture(builder.build());
        });

        when(s3.uploadPartCopy(any(UploadPartCopyRequest.class))).thenAnswer(invocation -> {
            UploadPartCopyRequest request = invocation.getArgument(0);
            uploadPartCopyRequests.add(request);
            return CompletableFuture.completedFuture(copyBuilder.build());
        });
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(CompletableFuture.completedFuture(null));

        when(s3.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenAnswer(invocation -> {
            GetObjectRequest request = invocation.getArgument(0);
            String[] startEnd = request.range().split("=")[1].split("-");
            long start = Long.parseLong(startEnd[0]);
            long end = Long.parseLong(startEnd[1]);

            GetObjectResponse.Builder responseBuilder = GetObjectResponse.builder();
            software.amazon.awssdk.core.async.ResponsePublisher<software.amazon.awssdk.services.s3.model.GetObjectResponse> responsePublisher
                = new ResponsePublisher<>(responseBuilder.build(), AsyncRequestBody.fromByteBuffer(TestUtils.random((int) (end - start + 1)).nioBuffer()));
            return CompletableFuture.completedFuture(responsePublisher);
        });

        S3ObjectMetadata s3ObjectMetadata1 = new S3ObjectMetadata(1, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata2 = new S3ObjectMetadata(2, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata3 = new S3ObjectMetadata(3, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata4 = new S3ObjectMetadata(4, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata5 = new S3ObjectMetadata(5, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata6 = new S3ObjectMetadata(6, 200, S3ObjectType.STREAM);
        S3ObjectMetadata s3ObjectMetadata7 = new S3ObjectMetadata(7, 200, S3ObjectType.STREAM);
        // case 2
        writer.copyWrite(s3ObjectMetadata1, 0, 120);
        // case 1
        writer.copyWrite(s3ObjectMetadata2, 20, 40);
        // case 3
        writer.copyWrite(s3ObjectMetadata3, 60, 100);
        // case 4
        writer.copyWrite(s3ObjectMetadata4, 140, 200);
        // case 1
        writer.copyWrite(s3ObjectMetadata5, 200, 280);
        // case 5
        writer.copyWrite(s3ObjectMetadata6, 400, 600);
        // last part
        writer.copyWrite(s3ObjectMetadata7, 10, 20);

        writer.close().get();
        assertEquals(3, uploadPartRequests.size());
        assertEquals("unit-test-bucket", uploadPartRequests.get(0).bucket());
        assertEquals("test-path-2", uploadPartRequests.get(0).key());
        for (int i = 0; i < 3; i++) {
            int partNum = uploadPartRequests.get(i).partNumber();
            switch (partNum) {
                case 2:
                    assertEquals(120L, writeContentLengths.get(i));
                    break;
                case 3:
                    assertEquals(280L, writeContentLengths.get(i));
                    break;
                case 4:
                    assertEquals(10L, writeContentLengths.get(i));
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

        assertEquals(1, uploadPartCopyRequests.size());
        assertEquals("unit-test-bucket", uploadPartCopyRequests.get(0).sourceBucket());
        assertEquals("unit-test-bucket", uploadPartCopyRequests.get(0).destinationBucket());
        assertEquals(List.of(s3ObjectMetadata1.key()), uploadPartCopyRequests.stream()
            .map(UploadPartCopyRequest::sourceKey)
            .collect(Collectors.toList()));
        assertEquals("test-path-2", uploadPartCopyRequests.get(0).destinationKey());
        assertEquals(List.of(1), uploadPartCopyRequests.stream()
            .map(UploadPartCopyRequest::partNumber)
            .collect(Collectors.toList()));
        assertEquals(List.of("bytes=0-119"), uploadPartCopyRequests.stream()
            .map(UploadPartCopyRequest::copySourceRange)
            .collect(Collectors.toList()));
    }

    /**
     * Given the source bucket differs from the target bucket, copyWrite reads source ranges through ObjectStorage
     * instead of issuing same-bucket server-side copy requests.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testCrossBucketCopyWriteUsesRangeReadChunks() throws Exception {
        ObjectStorage.WriteOptions options = ObjectStorage.WriteOptions.DEFAULT.copy().bucketId((short) 1);
        ObjectStorage readStorage = mock(ObjectStorage.class);
        when(readStorage.createMultipartUpload(options, "cross-bucket-target"))
            .thenReturn(CompletableFuture.completedFuture("upload-id"));
        when(readStorage.rangeRead(any(), anyString(), anyLong(), anyLong())).thenAnswer(invocation -> {
            long start = invocation.getArgument(2);
            long end = invocation.getArgument(3);
            return CompletableFuture.completedFuture(Unpooled.wrappedBuffer(new byte[(int) (end - start)]));
        });
        when(readStorage.uploadPart(eq(options), eq("cross-bucket-target"), eq("upload-id"), anyInt(), any()))
            .thenAnswer(invocation -> CompletableFuture.completedFuture(
                new ObjectStorage.ObjectStorageCompletedPart(invocation.getArgument(3), "etag", null)));
        when(readStorage.completeMultipartUpload(eq(options), eq("cross-bucket-target"), eq("upload-id"), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        writer = new MultiPartWriter(options, readStorage, "cross-bucket-target", 100);
        long chunkSize = 32L * 1024 * 1024;

        S3ObjectMetadata source = new S3ObjectMetadata(1, ObjectAttributes.builder().bucket((short) 2).build().attributes());
        writer.copyWrite(source, 0, 2 * chunkSize + 1);
        writer.close().get();

        ArgumentCaptor<ObjectStorage.ReadOptions> readOptionsCaptor = ArgumentCaptor.forClass(ObjectStorage.ReadOptions.class);
        ArgumentCaptor<Long> startCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> endCaptor = ArgumentCaptor.forClass(Long.class);
        verify(readStorage, times(3)).rangeRead(readOptionsCaptor.capture(), eq(source.key()), startCaptor.capture(), endCaptor.capture());
        assertEquals(List.of((short) 2, (short) 2, (short) 2), readOptionsCaptor.getAllValues().stream()
            .map(ObjectStorage.ReadOptions::bucket)
            .collect(Collectors.toList()));
        assertEquals(List.of(0L, chunkSize, 2 * chunkSize), startCaptor.getAllValues());
        assertEquals(List.of(chunkSize, 2 * chunkSize, 2 * chunkSize + 1), endCaptor.getAllValues());

        ArgumentCaptor<ByteBuf> partCaptor = ArgumentCaptor.forClass(ByteBuf.class);
        verify(readStorage, times(3)).uploadPart(eq(options), eq("cross-bucket-target"), eq("upload-id"), anyInt(), partCaptor.capture());
        assertEquals(List.of(chunkSize, chunkSize, 1L), partCaptor.getAllValues().stream()
            .map(part -> (long) part.readableBytes())
            .collect(Collectors.toList()));
        verify(readStorage, never()).uploadPartCopy(any(), any(), any(), anyLong(), anyLong(), any(), anyInt());
    }

    private static String md5Base64(ByteBuf data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            for (ByteBuffer buffer : data.nioBuffers()) {
                md5.update(buffer.duplicate());
            }
            return Base64.getEncoder().encodeToString(md5.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String sha256Base64(ByteBuf data) throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        for (ByteBuffer buffer : data.nioBuffers()) {
            sha256.update(buffer.duplicate());
        }
        return Base64.getEncoder().encodeToString(sha256.digest());
    }

}
