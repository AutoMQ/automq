/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@Tag("S3Unit")
class AwsObjectStorageProgressTest {
    /** Given streamed chunks, cumulative progress precedes completion of the physical GET. */
    @Test
    void streamsCumulativePrefixesBeforeCompletion() {
        runRead(response(SdkHttpResponse.builder().statusCode(206).build()), true);
    }

    /** Given integrity metadata, bytes are only exposed after the complete response has succeeded. */
    @Test
    void integrityHeadersDeferProgressUntilCompletion() {
        for (String header : List.of("x-amz-checksum-crc32c", "Content-MD5", "x-amz-trailer", "Trailer")) {
            runRead(response(SdkHttpResponse.builder().statusCode(206).putHeader(header, "value").build()), false);
        }
        runRead(GetObjectResponse.builder().build(), false);
    }

    /** Given a late transport failure, progressive bytes remain reported while deferred responses report nothing. */
    @Test
    void transportFailureDoesNotNotifyDeferredResponses() {
        runRead(response(SdkHttpResponse.builder().statusCode(206).build()), true, true);
        runRead(response(SdkHttpResponse.builder().statusCode(206).putHeader("x-amz-checksum-crc32c", "value").build()), false, true);
    }

    /** Given a throwing progress observer, streaming and deferred downloads still complete successfully. */
    @Test
    void listenerFailureDoesNotFailDownload() {
        runRead(response(SdkHttpResponse.builder().statusCode(206).build()), true, false, true);
        runRead(response(SdkHttpResponse.builder().statusCode(206).putHeader("Trailer", "value").build()), false, false, true);
    }

    private GetObjectResponse response(SdkHttpResponse httpResponse) {
        GetObjectResponse.Builder builder = GetObjectResponse.builder();
        builder.sdkHttpResponse(httpResponse);
        return builder.build();
    }

    private void runRead(GetObjectResponse response, boolean progressive) {
        runRead(response, progressive, false);
    }

    private void runRead(GetObjectResponse response, boolean progressive, boolean failure) {
        runRead(response, progressive, failure, false);
    }

    private void runRead(GetObjectResponse response, boolean progressive, boolean failure, boolean listenerFailure) {
        AtomicReference<Subscriber<? super ByteBuffer>> subscriber = new AtomicReference<>();
        SdkPublisher<ByteBuffer> bytes = SdkPublisher.adapt(s -> {
            subscriber.set(s);
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long count) {
                }

                @Override
                public void cancel() {
                }
            });
        });
        S3AsyncClient client = mock(S3AsyncClient.class);
        doReturn(CompletableFuture.completedFuture(new ResponsePublisher<>(response, bytes)))
            .when(client).getObject(any(GetObjectRequest.class), org.mockito.ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponsePublisher<GetObjectResponse>>>any());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        List<Integer> sizes = new ArrayList<>();
        AtomicReference<List<ByteBuf>> observed = new AtomicReference<>();
        try {
            CompletableFuture<ByteBuf> read = storage.doRangeRead(new ObjectStorage.ReadOptions(), "key", 0, 8,
                (chunks, size) -> {
                    if (observed.get() != null) {
                        assertSame(observed.get(), chunks);
                    }
                    observed.set(chunks);
                    assertEquals(size, chunks.stream().mapToInt(ByteBuf::readableBytes).sum());
                    sizes.add(size);
                    if (listenerFailure) {
                        throw new IllegalStateException("progress observer failed");
                    }
                });
            subscriber.get().onNext(ByteBuffer.wrap(new byte[3]));
            assertFalse(read.isDone());
            assertEquals(progressive ? List.of(3) : List.of(), sizes);
            subscriber.get().onNext(ByteBuffer.wrap(new byte[5]));
            assertFalse(read.isDone());
            assertEquals(progressive ? List.of(3, 8) : List.of(), sizes);
            if (failure) {
                subscriber.get().onError(new IllegalStateException("late transport failure"));
                assertTrue(read.isCompletedExceptionally());
                assertEquals(progressive ? List.of(3, 8) : List.of(), sizes);
                return;
            }
            subscriber.get().onComplete();
            assertTrue(read.isDone());
            assertEquals(progressive ? List.of(3, 8) : List.of(8), sizes);
            assertEquals(8, read.join().readableBytes());
            read.join().release();
            observed.get().forEach(chunk -> assertEquals(0, chunk.refCnt()));
        } finally {
            storage.close();
        }
    }
}
