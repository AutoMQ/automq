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

package com.automq.stream.s3.objects;

import com.automq.stream.s3.memory.MemoryMetadataManager;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.automq.stream.s3.compact.CompactOperations.DELETE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectManagerTest {
    private StreamManager streamManager;
    private ObjectManager objectManager;

    @BeforeEach
    void setUp() {
        MemoryMetadataManager metadataManager = new MemoryMetadataManager();
        streamManager = metadataManager;
        objectManager = metadataManager;
    }

    @Test
    void prepareObject() {
        assertEquals(0, objectManager.prepareObject(1, 1000).join());
        assertEquals(1, objectManager.prepareObject(1, 1000).join());
        assertEquals(2, objectManager.prepareObject(8, 1000).join());
        assertEquals(10, objectManager.prepareObject(1, 1000).join());
    }

    @Test
    void testCommitAndCompact() {
        Long streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();
        streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();
        streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();

        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        // Commit stream set object with stream 0 offset [0, 3) and stream 1 offset [0, 5).
        ArrayList<ObjectStreamRange> streamRangeList = new ArrayList<>();
        streamRangeList.add(new ObjectStreamRange(0, 0, 0, 3, 300));
        streamRangeList.add(new ObjectStreamRange(1, 0, 0, 5, 500));
        request.setStreamRanges(streamRangeList);
        request.setOrderId(0);

        // Commit stream object with stream 2 offset [0, 10).
        List<StreamObject> streamObjectList = new ArrayList<>();
        StreamObject streamObject = new StreamObject();
        streamObject.setObjectId(1);
        streamObject.setStreamId(2);
        streamObject.setStartOffset(0);
        streamObject.setEndOffset(10);
        streamObjectList.add(streamObject);

        streamObject = new StreamObject();
        streamObject.setObjectId(4);
        streamObject.setStreamId(2);
        streamObject.setStartOffset(10);
        streamObject.setEndOffset(20);
        streamObjectList.add(streamObject);

        request.setStreamObjects(streamObjectList);
        objectManager.commitStreamSetObject(request).join();

        List<StreamMetadata> streamMetadataList = streamManager.getStreams(List.of(0L, 1L, 2L)).join();
        assertEquals(3, streamMetadataList.size());
        assertEquals(3, streamMetadataList.get(0).endOffset());
        assertEquals(5, streamMetadataList.get(1).endOffset());
        assertEquals(20, streamMetadataList.get(2).endOffset());

        List<S3ObjectMetadata> streamSetObjectMetadataList = objectManager.getServerObjects().join();
        assertEquals(1, streamSetObjectMetadataList.size());
        S3ObjectMetadata streamSetMetadata = streamSetObjectMetadataList.get(0);
        List<StreamOffsetRange> ranges = streamSetMetadata.getOffsetRanges();
        assertEquals(2, ranges.size());

        assertEquals(0, ranges.get(0).streamId());
        assertEquals(0, ranges.get(0).startOffset());
        assertEquals(3, ranges.get(0).endOffset());

        assertEquals(1, ranges.get(1).streamId());
        assertEquals(0, ranges.get(1).startOffset());
        assertEquals(5, ranges.get(1).endOffset());

        List<S3ObjectMetadata> streamObjectMetadataList = objectManager.getStreamObjects(2, 0, 10, 100).join();
        assertEquals(1, streamObjectMetadataList.size());
        ranges = streamObjectMetadataList.get(0).getOffsetRanges();
        assertEquals(1, ranges.size());
        assertEquals(2, ranges.get(0).streamId());
        assertEquals(0, ranges.get(0).startOffset());
        assertEquals(10, ranges.get(0).endOffset());

        streamObjectMetadataList = objectManager.getStreamObjects(2, 0, 20, 100).join();
        assertEquals(2, streamObjectMetadataList.size());

        // Compact stream set object and commit stream object.
        request = new CommitStreamSetObjectRequest();
        request.setObjectId(ObjectUtils.NOOP_OBJECT_ID);
        request.setCompactedObjectIds(List.of(0L));

        streamObjectList = new ArrayList<>();
        streamObject = new StreamObject();
        streamObject.setObjectId(2);
        streamObject.setStreamId(0);
        streamObject.setStartOffset(0);
        streamObject.setEndOffset(3);
        streamObjectList.add(streamObject);

        streamObject = new StreamObject();
        streamObject.setObjectId(3);
        streamObject.setStreamId(1);
        streamObject.setStartOffset(0);
        streamObject.setEndOffset(5);
        streamObjectList.add(streamObject);

        request.setStreamObjects(streamObjectList);
        objectManager.commitStreamSetObject(request).join();

        streamSetObjectMetadataList = objectManager.getServerObjects().join();
        assertEquals(0, streamSetObjectMetadataList.size());

        streamObjectMetadataList = objectManager.getStreamObjects(0, 0, 10, 100).join();
        assertEquals(1, streamObjectMetadataList.size());
        streamObjectMetadataList = objectManager.getStreamObjects(1, 0, 10, 100).join();
        assertEquals(1, streamObjectMetadataList.size());

        // Compact stream object.
        objectManager.compactStreamObject(new CompactStreamObjectRequest(5, 2000, 2, 0L, 0, 20,
            List.of(1L, 4L), List.of(DELETE, DELETE), ObjectAttributes.DEFAULT.attributes())).join();
        streamObjectMetadataList = objectManager.getStreamObjects(2, 0, 10, 100).join();
        assertEquals(1, streamObjectMetadataList.size());
        ranges = streamObjectMetadataList.get(0).getOffsetRanges();
        assertEquals(1, ranges.size());
        assertEquals(2, ranges.get(0).streamId());
        assertEquals(0, ranges.get(0).startOffset());
        assertEquals(20, ranges.get(0).endOffset());
    }

    @Test
    void testGetObject() {
        // Create and open stream 0 and 1.
        Long streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();
        streamId = streamManager.createStream().join();
        streamManager.openStream(streamId, 0).join();

        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        // Commit stream set object with stream 0 offset [0, 3) and stream 1 offset [0, 5).
        ArrayList<ObjectStreamRange> streamRangeList = new ArrayList<>();
        streamRangeList.add(new ObjectStreamRange(0, 0, 0, 3, 300));
        streamRangeList.add(new ObjectStreamRange(1, 0, 0, 5, 500));
        request.setStreamRanges(streamRangeList);
        request.setOrderId(0);

        List<StreamObject> streamObjectList = new ArrayList<>();
        StreamObject streamObject = new StreamObject();
        streamObject.setObjectId(1);
        streamObject.setStreamId(0);
        streamObject.setStartOffset(3);
        streamObject.setEndOffset(5);
        streamObjectList.add(streamObject);

        streamObject = new StreamObject();
        streamObject.setObjectId(2);
        streamObject.setStreamId(0);
        streamObject.setStartOffset(5);
        streamObject.setEndOffset(10);
        streamObjectList.add(streamObject);

        request.setStreamObjects(streamObjectList);
        objectManager.commitStreamSetObject(request).join();

        // Get object with stream 0 offset [0, 10).
        List<S3ObjectMetadata> streamObjectMetadataList = objectManager.getObjects(0, 0, 10, 100).join();
        assertEquals(3, streamObjectMetadataList.size());
        // Get object with stream 0 offset [1, 9).
        streamObjectMetadataList = objectManager.getObjects(0, 1, 9, 100).join();
        assertEquals(3, streamObjectMetadataList.size());
        // Get object with stream 0 offset [3, 4).
        streamObjectMetadataList = objectManager.getObjects(0, 0, 1, 100).join();
        assertEquals(1, streamObjectMetadataList.size());
        assertEquals(0, streamObjectMetadataList.get(0).objectId());
        // Get object with limit 1.
        streamObjectMetadataList = objectManager.getObjects(0, 0, 10, 1).join();
        assertEquals(1, streamObjectMetadataList.size());
        assertEquals(0, streamObjectMetadataList.get(0).objectId());

        // Get stream object with stream 0 offset [3, 10).
        streamObjectMetadataList = objectManager.getStreamObjects(0, 3, 10, 100).join();
        assertEquals(2, streamObjectMetadataList.size());
        // Get stream object with stream 0 offset [5, 10).
        streamObjectMetadataList = objectManager.getStreamObjects(0, 5, 10, 100).join();
        assertEquals(1, streamObjectMetadataList.size());
        assertEquals(2, streamObjectMetadataList.get(0).objectId());
        // Get object with limit 1.
        streamObjectMetadataList = objectManager.getStreamObjects(0, 0, 10, 1).join();
        assertEquals(1, streamObjectMetadataList.size());
        assertEquals(1, streamObjectMetadataList.get(0).objectId());

        // Get all stream set objects belonging current node.
        streamObjectMetadataList = objectManager.getServerObjects().join();
        assertEquals(1, streamObjectMetadataList.size());
        // Change node id.
        MemoryMetadataManager.advanceNodeId();
        streamObjectMetadataList = objectManager.getServerObjects().join();
        assertEquals(0, streamObjectMetadataList.size());
    }
}
