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

package kafka.log.streamaspect;

import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies concurrent existing-stream open and creation failure cleanup.
 */
@Tag("S3Unit")
public class ElasticLogStreamManagerTest {

    /**
     * Given two existing streams, both opens start before creation waits for either result.
     */
    @Test
    public void testExistingStreamsOpenConcurrently() throws Exception {
        StreamClient streamClient = mock(StreamClient.class);
        Stream logStream = completedStream(1L);
        Stream timeStream = completedStream(2L);
        CompletableFuture<Stream> logOpen = new CompletableFuture<>();
        CompletableFuture<Stream> timeOpen = new CompletableFuture<>();
        CountDownLatch openCalls = new CountDownLatch(2);
        when(streamClient.openStream(anyLong(), any(OpenStreamOptions.class))).thenAnswer(invocation -> {
            openCalls.countDown();
            return invocation.<Long>getArgument(0) == 1L ? logOpen : timeOpen;
        });

        CompletableFuture<ElasticLogStreamManager> managerFuture = ElasticLogStreamManager.create(
            Map.of("log", 1L, "tim", 2L), streamClient, 1, 3L, Map.of(), false);
        try {
            assertTrue(openCalls.await(5, TimeUnit.SECONDS));
            assertFalse(managerFuture.isDone());

            logOpen.complete(logStream);
            assertFalse(managerFuture.isDone());
            timeOpen.complete(timeStream);

            ElasticLogStreamManager manager = managerFuture.join();
            assertSame(logStream, manager.streams().get("log"));
            assertSame(timeStream, manager.streams().get("tim"));
        } finally {
            logOpen.complete(logStream);
            timeOpen.complete(timeStream);
        }
    }

    /**
     * Given one failed open and one later successful open, creation waits and closes the late success.
     */
    @Test
    public void testLateSuccessfulOpenIsClosedAfterPeerFailure() throws Exception {
        StreamClient streamClient = mock(StreamClient.class);
        Stream timeStream = completedStream(2L);
        IOException openFailure = new IOException("log open failed");
        CompletableFuture<Stream> timeOpen = new CompletableFuture<>();
        when(streamClient.openStream(eq(1L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(openFailure));
        when(streamClient.openStream(eq(2L), any(OpenStreamOptions.class))).thenReturn(timeOpen);

        CompletableFuture<ElasticLogStreamManager> managerFuture = ElasticLogStreamManager.create(
            Map.of("log", 1L, "tim", 2L), streamClient, 1, 3L, Map.of(), false);
        assertFalse(managerFuture.isDone());

        timeOpen.complete(timeStream);
        CompletionException exception = assertThrows(CompletionException.class, managerFuture::join);
        assertSame(openFailure, exception.getCause());
        verify(timeStream).close();
    }

    /**
     * Given a successful peer open and failed cleanup, the open error remains primary.
     */
    @Test
    public void testCleanupFailureIsSuppressed() {
        StreamClient streamClient = mock(StreamClient.class);
        Stream logStream = mock(Stream.class);
        IOException openFailure = new IOException("time open failed");
        IOException cleanupFailure = new IOException("log close failed");
        when(logStream.close()).thenReturn(CompletableFuture.failedFuture(cleanupFailure));
        when(streamClient.openStream(eq(1L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.completedFuture(logStream));
        when(streamClient.openStream(eq(2L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(openFailure));

        CompletionException exception = assertThrows(CompletionException.class,
            () -> ElasticLogStreamManager.create(Map.of("log", 1L, "tim", 2L), streamClient,
                1, 3L, Map.of(), false).join());

        assertSame(openFailure, exception.getCause());
        assertEquals(1, exception.getCause().getSuppressed().length);
        assertSame(cleanupFailure, exception.getCause().getSuppressed()[0]);
    }

    /**
     * Given an incomplete compensating close, creation waits for cleanup before propagating the open failure.
     */
    @Test
    public void testCreationWaitsForCleanup() throws Exception {
        StreamClient streamClient = mock(StreamClient.class);
        Stream logStream = mock(Stream.class);
        IOException openFailure = new IOException("time open failed");
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        CountDownLatch closeCall = new CountDownLatch(1);
        when(logStream.close()).thenAnswer(invocation -> {
            closeCall.countDown();
            return cleanup;
        });
        when(streamClient.openStream(eq(1L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.completedFuture(logStream));
        when(streamClient.openStream(eq(2L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(openFailure));

        CompletableFuture<ElasticLogStreamManager> managerFuture = ElasticLogStreamManager.create(
            Map.of("log", 1L, "tim", 2L), streamClient, 1, 3L, Map.of(), false);
        assertTrue(closeCall.await(5, TimeUnit.SECONDS));
        assertFalse(managerFuture.isDone());

        cleanup.complete(null);
        CompletionException exception = assertThrows(CompletionException.class, managerFuture::join);
        assertSame(openFailure, exception.getCause());
    }

    /**
     * Given all existing-stream opens fail, creation fails without attempting stream cleanup.
     */
    @Test
    public void testAllOpenFailuresRequireNoCleanup() {
        StreamClient streamClient = mock(StreamClient.class);
        IOException logFailure = new IOException("log open failed");
        IOException timeFailure = new IOException("time open failed");
        when(streamClient.openStream(eq(1L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(logFailure));
        when(streamClient.openStream(eq(2L), any(OpenStreamOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(timeFailure));
        Map<String, Long> streams = new LinkedHashMap<>();
        streams.put("log", 1L);
        streams.put("tim", 2L);

        CompletionException exception = assertThrows(CompletionException.class,
            () -> ElasticLogStreamManager.create(streams, streamClient, 1, 3L, Map.of(), false).join());

        assertTrue(exception.getCause() == logFailure || exception.getCause() == timeFailure);
        assertEquals(0, exception.getCause().getSuppressed().length);
    }

    /**
     * Given only uncreated streams, creation retains lazy streams without issuing open requests.
     */
    @Test
    public void testNoopStreamRemainsLazy() {
        StreamClient streamClient = mock(StreamClient.class);

        ElasticLogStreamManager manager = ElasticLogStreamManager.create(
            Map.of("txn", LazyStream.NOOP_STREAM_ID), streamClient, 1, 3L, Map.of(), false).join();

        assertInstanceOf(LazyStream.class, manager.streams().get("txn"));
        verify(streamClient, never()).openStream(anyLong(), any(OpenStreamOptions.class));
    }

    private static Stream completedStream(long streamId) {
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(streamId);
        when(stream.close()).thenReturn(CompletableFuture.completedFuture(null));
        return stream;
    }
}
