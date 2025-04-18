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
