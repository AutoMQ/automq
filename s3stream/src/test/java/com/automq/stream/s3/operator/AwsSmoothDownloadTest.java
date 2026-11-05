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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(30)
class AwsSmoothDownloadTest {
    /** The real SDK delivers variable-size buffers, all charged once before the GET future completes. */
    @Test
    void testSingleGetChargesActualBuffers() throws Exception {
        byte[] bytes = new byte[128 * 1024 + 7];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/bucket/key", exchange -> {
            requests.incrementAndGet();
            try {
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            } finally {
                exchange.close();
            }
        });
        server.start();
        NetworkBandwidthLimiter limiter = mock(NetworkBandwidthLimiter.class);
        BlockingQueue<Grant> grants = new LinkedBlockingQueue<>();
        when(limiter.consume(eq(ThrottleStrategy.TAIL), anyLong())).thenAnswer(invocation -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            grants.add(new Grant(invocation.getArgument(1), future));
            return future;
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
            ChecksumAlgorithm.UNKNOWN_TO_SDK_VERSION, limiter, NetworkBandwidthLimiter.NOOP);
        try {
            CompletableFuture<ByteBuf> read = storage.rangeRead(new ObjectStorage.ReadOptions()
                .bucket((short) 0).throttleStrategy(ThrottleStrategy.TAIL), "key", 0, -1);
            long charged = 0;
            while (charged < bytes.length) {
                Grant grant = grants.poll(5, TimeUnit.SECONDS);
                assertNotNull(grant);
                assertFalse(read.isDone());
                assertTrue(grants.isEmpty());
                charged += grant.bytes();
                grant.future().complete(null);
            }
            ByteBuf result = read.get(10, TimeUnit.SECONDS);
            try {
                assertEquals(bytes.length, charged);
                assertArrayEquals(bytes, ByteBufUtil.getBytes(result));
                assertEquals(1, requests.get());
            } finally {
                result.release();
            }
        } finally {
            storage.close();
            server.stop(0);
        }
    }

    private record Grant(long bytes, CompletableFuture<Void> future) { }
}
