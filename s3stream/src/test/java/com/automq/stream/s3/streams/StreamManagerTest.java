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

package com.automq.stream.s3.streams;

import com.automq.stream.s3.memory.MemoryMetadataManager;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.StreamObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamManagerTest {
    StreamManager streamManager;

    @BeforeEach
    public void setUp() throws Exception {
        streamManager = new MemoryMetadataManager();
    }

    @Test
    public void testCreateAndOpenStream() {
        // Create and open stream with epoch 0.
        Long streamId = streamManager.createStream().join();
        StreamMetadata streamMetadata = streamManager.openStream(streamId, 0).join();
        assertEquals(streamId, streamMetadata.streamId());
        assertEquals(0, streamMetadata.epoch());
        assertEquals(0, streamMetadata.startOffset());
        assertEquals(0, streamMetadata.endOffset());
        assertEquals(StreamState.OPENED, streamMetadata.state());
    }

    @Test
    public void testOpenAndCloseStream() {
        // Create and open stream with epoch 0.
        Long streamId = streamManager.createStream().join();
        StreamMetadata streamMetadata = streamManager.openStream(streamId, 0).join();

        // Close stream with epoch 1.
        CompletableFuture<Void> future = streamManager.closeStream(streamId, 1);
        assertEquals(StreamState.OPENED, streamMetadata.state());
        assertTrue(future.isCompletedExceptionally());

        // Close stream with epoch 0.
        streamManager.closeStream(streamId, 0).join();
        assertEquals(StreamState.CLOSED, streamMetadata.state());

        // Open stream with epoch 0.
        CompletableFuture<StreamMetadata> future1 = streamManager.openStream(streamId, 0);
        assertTrue(future1.isCompletedExceptionally());

        // Open stream with epoch 1.
        streamMetadata = streamManager.openStream(streamId, 1).join();
        assertEquals(streamId, streamMetadata.streamId());
        assertEquals(1, streamMetadata.epoch());
        assertEquals(0, streamMetadata.startOffset());
        assertEquals(0, streamMetadata.endOffset());
        assertEquals(StreamState.OPENED, streamMetadata.state());

        // Close stream with epoch 1.
        streamManager.closeStream(streamId, 1).join();
        assertEquals(StreamState.CLOSED, streamMetadata.state());
        streamManager.deleteStream(streamId, 1).join();
        List<StreamMetadata> streamMetadataList = streamManager.getOpeningStreams().join();
        assertEquals(0, streamMetadataList.size());
    }

    @Test
    public void testTrimStream() {
        // Create and open stream with epoch 0.
        Long streamId = streamManager.createStream().join();
        StreamMetadata streamMetadata = streamManager.openStream(streamId, 0).join();

        // Trim stream with epoch 1.
        CompletableFuture<Void> future = streamManager.trimStream(streamId, 1, 1);
        assertTrue(future.isCompletedExceptionally());

        // Trim stream to invalid offset.
        CompletableFuture<Void> future1 = streamManager.trimStream(streamId, 0, -1);
        assertTrue(future1.isCompletedExceptionally());
        future1 = streamManager.trimStream(streamId, 0, 1);
        assertTrue(future1.isCompletedExceptionally());

        // Advance offset and trim stream.
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        ArrayList<StreamObject> streamObjectList = new ArrayList<>();
        StreamObject streamObject = new StreamObject();
        streamObject.setStreamId(streamId);
        streamObject.setStartOffset(0);
        streamObject.setEndOffset(10);
        streamObjectList.add(streamObject);
        request.setStreamObjects(streamObjectList);
        ((ObjectManager) streamManager).commitStreamSetObject(request).join();
        assertEquals(10, streamMetadata.endOffset());

        streamManager.trimStream(streamId, 0, 5).join();
        assertEquals(5, streamMetadata.startOffset());
    }

    @Test
    public void testGetStreams() {
        ArrayList<Long> streamIds = new ArrayList<>();
        // Create and open stream with epoch 0.
        Long streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();
        streamIds.add(streamId);

        streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();
        streamIds.add(streamId);

        // Get streams.
        List<StreamMetadata> streamMetadataList = streamManager.getStreams(streamIds).join();
        assertEquals(2, streamMetadataList.size());
        assertEquals(streamId, streamMetadataList.get(1).streamId());
        assertEquals(0, streamMetadataList.get(1).epoch());
        assertEquals(0, streamMetadataList.get(1).startOffset());
        assertEquals(0, streamMetadataList.get(1).endOffset());
        assertEquals(StreamState.OPENED, streamMetadataList.get(1).state());

        streamIds.add(Long.MAX_VALUE);
        streamMetadataList = streamManager.getStreams(streamIds).join();
        assertEquals(2, streamMetadataList.size());
    }
}
