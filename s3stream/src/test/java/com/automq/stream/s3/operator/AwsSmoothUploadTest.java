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

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(30)
class AwsSmoothUploadTest {
    /** The actual SDK sends the first chunk while a later chunk waits for quota, using a single HTTP PUT. */
    @Test
    void testSinglePutStreamsToHttpServer() throws Exception {
        int chunk = ThrottledAsyncRequestBody.CHUNK_SIZE;
        CompletableFuture<Void> firstReceived = new CompletableFuture<>();
        CompletableFuture<Integer> received = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/bucket/key", exchange -> {
            requests.incrementAndGet();
            try {
                assertEquals("PUT", exchange.getRequestMethod());
                int first = exchange.getRequestBody().readNBytes(chunk).length;
                assertEquals(chunk, first);
                firstReceived.complete(null);
                int total = first + exchange.getRequestBody().readAllBytes().length;
                exchange.sendResponseHeaders(200, -1);
                received.complete(total);
            } catch (Throwable ex) {
                firstReceived.completeExceptionally(ex);
                received.completeExceptionally(ex);
            } finally {
                exchange.close();
            }
        });
        server.start();
        NetworkBandwidthLimiter limiter = mock(NetworkBandwidthLimiter.class);
        CompletableFuture<Void> lastGrant = new CompletableFuture<>();
        AtomicInteger grants = new AtomicInteger();
        when(limiter.consume(eq(ThrottleStrategy.WAL_UPLOAD), anyLong())).thenAnswer(invocation -> {
            int index = grants.getAndIncrement();
            assertEquals(index == 0 ? (long) chunk : 7L, invocation.<Long>getArgument(1));
            return index == 0 ? CompletableFuture.completedFuture(null) : lastGrant;
        });
        S3AsyncClient client = S3AsyncClient.builder()
            .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true).checksumValidationEnabled(false))
            .overrideConfiguration(builder -> builder.apiCallTimeout(Duration.ofSeconds(15))
                .retryStrategy(AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(1).build()))
            .build();
        AwsObjectStorage storage = new AwsObjectStorage(client, client, "bucket",
            ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION, limiter);
        ByteBuf data = Unpooled.buffer(chunk + 7).writeZero(chunk + 7);
        try {
            CompletableFuture<Void> upload = storage.doWrite(
                new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.WAL_UPLOAD), "key", data);
            firstReceived.get(10, TimeUnit.SECONDS);
            assertFalse(upload.isDone());
            assertFalse(received.isDone());
            lastGrant.complete(null);
            upload.get(10, TimeUnit.SECONDS);
            assertEquals(chunk + 7, received.get(10, TimeUnit.SECONDS));
            assertEquals(2, grants.get());
            assertEquals(1, requests.get());
            assertEquals(1, data.refCnt());
        } finally {
            lastGrant.complete(null);
            storage.close();
            data.release();
            server.stop(0);
        }
    }
}
