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

package com.automq.stream.s3;

import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class S3StreamClientTest {
    private S3StreamClient client;
    private StreamManager streamManager;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setup() {
        streamManager = mock(StreamManager.class);
        client = spy(new S3StreamClient(streamManager, mock(Storage.class), mock(ObjectManager.class), mock(ObjectStorage.class), new Config()));
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void cleanup() {
        scheduler.shutdown();
    }

    @Test
    public void testShutdown_withOpeningStream() {
        S3Stream stream = mock(S3Stream.class);
        when(stream.close(anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        when(stream.streamId()).thenReturn(1L);

        CompletableFuture<StreamMetadata> cf = new CompletableFuture<>();
        when(streamManager.openStream(anyLong(), anyLong(), anyMap())).thenReturn(cf);

        doAnswer(args -> stream).when(client).newStream(any(), any());

        scheduler.schedule(() -> {
            cf.complete(new StreamMetadata(1, 2, 100, 200, StreamState.OPENED));
        }, 100, MILLISECONDS);

        client.openStream(1, OpenStreamOptions.builder().build());
        client.shutdown();

        verify(stream, times(1)).close(anyBoolean());
        assertEquals(0, client.openingStreams.size());
        assertEquals(0, client.openedStreams.size());
        assertEquals(0, client.closingStreams.size());
    }

}
