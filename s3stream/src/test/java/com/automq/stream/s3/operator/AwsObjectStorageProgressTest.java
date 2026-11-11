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

import com.automq.stream.s3.metrics.operations.S3RequestSource;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises the real AWS response subscriber with a manually controlled response body. */
@Tag("S3Unit")
@Timeout(10)
class AwsObjectStorageProgressTest {
    /** Progress precedes EOF without completing the ordinary future or issuing another GET. */
    @Test
    void exposesBorrowedPrefixesBeforeCompletion() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        Body body = new Body();
        stubGet(client, body, SdkHttpResponse.builder().statusCode(206).build());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        Listener progress = new Listener();
        try {
            CompletableFuture<ByteBuf> full = storage.doRangeRead(options(() -> progress), "key", 0, 4);
            body.next(1, 2);
            assertEquals(List.of(List.of(1, 2)), progress.prefixes);
            assertFalse(full.isDone());
            body.next(3, 4);
            assertEquals(List.of(List.of(1, 2), List.of(1, 2, 3, 4)), progress.prefixes);
            assertFalse(full.isDone());
            body.complete();
            ByteBuf result = full.join();
            assertEquals(4, result.readableBytes());
            result.release();
            assertEquals(0, progress.failures);
            verify(client, times(1)).getObject(any(GetObjectRequest.class),
                org.mockito.ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponsePublisher<GetObjectResponse>>>any());
        } finally {
            storage.close();
        }
    }

    /** Response checksum and trailer validation cannot be bypassed by early delivery. */
    @ParameterizedTest
    @ValueSource(strings = {"x-amz-checksum-crc32", "X-Amz-Checksum-Sha256", "content-md5", "trailer", "x-amz-trailer"})
    void waitsForResponseIntegrity(String header) {
        S3AsyncClient client = mock(S3AsyncClient.class);
        Body body = new Body();
        stubGet(client, body, SdkHttpResponse.builder().statusCode(206).putHeader(header, "checksum").build());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        Listener progress = new Listener();
        try {
            CompletableFuture<ByteBuf> full = storage.doRangeRead(options(() -> progress), "key", 0, 4);
            body.next(1, 2, 3, 4);
            assertTrue(progress.prefixes.isEmpty());
            assertFalse(full.isDone());
            IOException error = new IOException("response checksum failed");
            body.fail(error);
            assertSame(error, assertThrows(CompletionException.class, full::join).getCause());
        } finally {
            storage.close();
        }
    }

    /** Other S3 sources retain full-response semantics even if their options carry a listener. */
    @ParameterizedTest
    @EnumSource(value = S3RequestSource.class, names = {"DELTA_WAL", "UNCLASSIFIED", "MIXED"})
    void progressIsLimitedToRouterWal(S3RequestSource source) {
        S3AsyncClient client = mock(S3AsyncClient.class);
        Body body = new Body();
        stubGet(client, body, SdkHttpResponse.builder().statusCode(206).build());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        Listener progress = new Listener();
        try {
            CompletableFuture<ByteBuf> full = storage.doRangeRead(options(() -> progress).source(source), "key", 0, 4);
            body.next(1, 2, 3, 4);
            assertTrue(progress.prefixes.isEmpty());
            assertFalse(full.isDone());
            body.complete();
            full.join().release();
        } finally {
            storage.close();
        }
    }

    /** A broken progress callback terminates its attempt and releases the accumulated response. */
    @Test
    void callbackFailureCleansUpAttempt() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        Body body = new Body();
        stubGet(client, body, SdkHttpResponse.builder().statusCode(206).build());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        ReadProgressListener progress = mock(ReadProgressListener.class);
        List<ByteBuf> borrowed = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("callback failed");
        doAnswer(call -> {
            borrowed.add(call.getArgument(0));
            throw failure;
        }).when(progress).onProgress(any());
        try {
            CompletableFuture<ByteBuf> full = storage.doRangeRead(options(() -> progress), "key", 0, 4);
            body.next(1, 2);
            assertSame(failure, assertThrows(CompletionException.class, full::join).getCause());
            assertEquals(0, borrowed.get(0).refCnt());
            verify(progress).onFailure();
        } finally {
            storage.close();
        }
    }

    /** A failed attempt discards its partial state and a later attempt uses a new listener. */
    @Test
    void createsListenersPerPhysicalAttempt() {
        S3AsyncClient client = mock(S3AsyncClient.class);
        Body firstBody = new Body();
        stubGet(client, firstBody, SdkHttpResponse.builder().statusCode(206).build());
        AwsObjectStorage storage = new AwsObjectStorage(client, "bucket");
        List<Listener> attempts = new ArrayList<>();
        ObjectStorage.ReadOptions options = options(() -> {
            Listener listener = new Listener();
            attempts.add(listener);
            return listener;
        });
        try {
            CompletableFuture<ByteBuf> first = storage.doRangeRead(options, "key", 0, 4);
            firstBody.next(1, 2);
            firstBody.fail(new IOException("connection lost"));
            assertThrows(CompletionException.class, first::join);
            assertEquals(1, attempts.get(0).failures);
            Body retryBody = new Body();
            stubGet(client, retryBody, SdkHttpResponse.builder().statusCode(206).build());
            CompletableFuture<ByteBuf> retry = storage.doRangeRead(options.copy(), "key", 0, 4);
            retryBody.next(1, 2, 3, 4);
            retryBody.complete();
            retry.join().release();
            assertEquals(2, attempts.size());
            assertEquals(List.of(List.of(1, 2, 3, 4)), attempts.get(1).prefixes);
        } finally {
            storage.close();
        }
    }

    /** Merged reads preserve each logical start offset, even when the listener is not on the first task. */
    @Test
    void mergerTranslatesPrefixesForEachReader() {
        Listener first = new Listener();
        Listener second = new Listener();
        ObjectStorage.ReadOptions firstOptions = options(() -> first);
        ObjectStorage.ReadOptions secondOptions = options(() -> second);
        assertSame(firstOptions.progressListener(), firstOptions.copy().progressListener());
        AbstractObjectStorage.MergedReadTask merged = new AbstractObjectStorage.MergedReadTask(
            new AbstractObjectStorage.ReadTask(new ObjectStorage.ReadOptions(), "key", 0, 8, new CompletableFuture<>()), 1);
        assertTrue(merged.tryMerge(new AbstractObjectStorage.ReadTask(firstOptions, "key", 1, 5, new CompletableFuture<>())));
        assertTrue(merged.tryMerge(new AbstractObjectStorage.ReadTask(secondOptions, "key", 4, 8, new CompletableFuture<>())));
        ReadProgressListener progress = merged.options().progressListener().get();
        ByteBuf data = Unpooled.wrappedBuffer(new byte[] {0, 1, 2, 3, 4, 5});
        try {
            progress.onProgress(data.asReadOnly());
            assertEquals(List.of(List.of(1, 2, 3, 4)), first.prefixes);
            assertEquals(List.of(List.of(4, 5)), second.prefixes);
            progress.onFailure();
            assertEquals(1, first.failures);
            assertEquals(1, second.failures);
        } finally {
            data.release();
        }
    }

    private static ObjectStorage.ReadOptions options(Supplier<ReadProgressListener> factory) {
        return new ObjectStorage.ReadOptions().source(S3RequestSource.ROUTER_WAL).progressListener(factory);
    }

    private static void stubGet(S3AsyncClient client, Body body, SdkHttpResponse httpResponse) {
        GetObjectResponse.Builder response = GetObjectResponse.builder().contentLength(4L);
        response.sdkHttpResponse(httpResponse);
        when(client.getObject(any(GetObjectRequest.class),
            org.mockito.ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponsePublisher<GetObjectResponse>>>any()))
            .thenReturn(CompletableFuture.completedFuture(new ResponsePublisher<>(response.build(), SdkPublisher.adapt(body))));
    }

    private static final class Listener implements ReadProgressListener {
        private final List<List<Integer>> prefixes = new ArrayList<>();
        private int failures;

        @Override
        public void onProgress(ByteBuf prefix) {
            assertTrue(prefix.isReadOnly());
            List<Integer> bytes = new ArrayList<>();
            for (int i = prefix.readerIndex(); i < prefix.writerIndex(); i++) {
                bytes.add((int) prefix.getByte(i));
            }
            prefixes.add(bytes);
        }

        @Override
        public void onFailure() {
            failures++;
        }
    }

    private static final class Body implements Publisher<ByteBuffer> {
        private Subscriber<? super ByteBuffer> subscriber;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long count) {
                }

                @Override
                public void cancel() {
                }
            });
        }

        void next(int... values) {
            ByteBuffer bytes = ByteBuffer.allocate(values.length);
            for (int value : values) {
                bytes.put((byte) value);
            }
            bytes.flip();
            subscriber.onNext(bytes);
        }

        void complete() {
            subscriber.onComplete();
        }

        void fail(Throwable error) {
            subscriber.onError(error);
        }
    }
}
