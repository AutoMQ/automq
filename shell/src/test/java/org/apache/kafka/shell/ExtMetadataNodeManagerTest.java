/*
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

package org.apache.kafka.shell;

import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecordJsonConverter;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RangeRecordJsonConverter;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamRecordJsonConverter;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.metadata.UpdateNextNodeIdRecord;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * This test is for the extended metadata node manager, only works for AutoMQ.
 */
public class ExtMetadataNodeManagerTest {
    private MetadataNodeManager metadataNodeManager;

    @BeforeEach
    public void setup() throws Exception {
        metadataNodeManager = new MetadataNodeManager();
        metadataNodeManager.setup();
    }

    @AfterEach
    public void cleanup() throws Exception {
        metadataNodeManager.close();
    }

    @Test
    public void testS3StreamRecord() {
        long streamId = 1L;
        long epoch = 3L;
        int rangeIndex = 5;
        long startOffset = 7L;
        S3StreamRecord record1 = new S3StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setRangeIndex(rangeIndex)
            .setStartOffset(startOffset)
            .setStreamState(StreamState.OPENED.toByte());
        metadataNodeManager.handleMessage(record1);
        assertEquals(S3StreamRecordJsonConverter.write(record1, S3StreamRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(), metadataNodeManager.getData().root().file("s3Streams", Long.toString(streamId), "data").contents());

        RemoveS3StreamRecord record2 = new RemoveS3StreamRecord().setStreamId(streamId);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("s3Streams").children().containsKey(Long.toString(streamId)));
    }

    @Test
    public void testRangeRecord() {
        long streamId = 1L;
        int rangeIndex = 3;
        long epoch = 5L;
        long startOffset = 7L;
        long endOffset = 9L;
        int nodeId = 11;
        RangeRecord record1 = new RangeRecord()
            .setStreamId(streamId)
            .setRangeIndex(rangeIndex)
            .setEpoch(epoch)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset)
            .setNodeId(nodeId);
        metadataNodeManager.handleMessage(record1);
        assertEquals(RangeRecordJsonConverter.write(record1, RangeRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(), metadataNodeManager.getData().root().file("s3Streams", Long.toString(streamId), "ranges", Integer.toString(rangeIndex)).contents());

        RemoveRangeRecord record2 = new RemoveRangeRecord().setStreamId(streamId).setRangeIndex(rangeIndex);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("s3Streams", Long.toString(streamId), "ranges").children().containsKey(Integer.toString(rangeIndex)));
    }

    @Test
    public void testS3StreamObjectRecord() {
        long streamId = 1L;
        long startOffset = 3L;
        long endOffset = 5L;
        long objectId = 7L;
        long dataTimeInMs = 9L;
        S3StreamObjectRecord record1 = new S3StreamObjectRecord()
            .setStreamId(streamId)
            .setStartOffset(startOffset)
            .setEndOffset(endOffset)
            .setObjectId(objectId)
            .setDataTimeInMs(dataTimeInMs);
        metadataNodeManager.handleMessage(record1);
        assertEquals(S3StreamObjectRecordJsonConverter.write(record1, S3StreamObjectRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(), metadataNodeManager.getData().root().file("s3Streams", Long.toString(streamId), "s3StreamObjects", Long.toString(objectId)).contents());

        RemoveS3StreamObjectRecord record2 = new RemoveS3StreamObjectRecord().setStreamId(streamId).setObjectId(objectId);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("s3Streams", Long.toString(streamId), "s3StreamObjects").children().containsKey(Long.toString(objectId)));
    }

    @Test
    public void testS3StreamSetObjectRecord() {
        int nodeId = 1;
        long objectId = 3L;
        long orderId = 5;
        long dataTimeInMs = 7L;

        List<StreamOffsetRange> rangeList = List.of(
            new StreamOffsetRange(0, 0L, 100L),
            new StreamOffsetRange(1, 0L, 200L)
        );
        byte[] ranges = S3StreamSetObject.encode(rangeList);
        S3StreamSetObjectRecord record1 = new S3StreamSetObjectRecord()
            .setNodeId(nodeId)
            .setObjectId(objectId)
            .setOrderId(orderId)
            .setDataTimeInMs(dataTimeInMs)
            .setRanges(ranges);
        metadataNodeManager.handleMessage(record1);
        assertEquals(Long.toString(orderId), metadataNodeManager.getData().root().file("nodes", Integer.toString(nodeId), "s3StreamSetObjects", Long.toString(objectId), "orderId").contents());
        assertEquals(Long.toString(dataTimeInMs), metadataNodeManager.getData().root().file("nodes", Integer.toString(nodeId), "s3StreamSetObjects", Long.toString(objectId), "dataTimeInMs").contents());

        RemoveStreamSetObjectRecord record2 = new RemoveStreamSetObjectRecord().setNodeId(nodeId).setObjectId(objectId);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("nodes", Integer.toString(nodeId), "s3StreamSetObjects").children().containsKey(Long.toString(objectId)));
    }

    @Test
    public void testS3ObjectRecord() {
        long objectId = 1L;
        long objectSize = 3L;
        long preparedTimeInMs = 5L;
        long expiredTimeInMs = 7L;
        long committedTimeInMs = 9L;
        long markDestroyedTimeInMs = 11L;
        byte objectState = S3ObjectState.COMMITTED.toByte();

        S3ObjectRecord record1 = new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(objectSize)
            .setPreparedTimeInMs(preparedTimeInMs)
            .setExpiredTimeInMs(expiredTimeInMs)
            .setCommittedTimeInMs(committedTimeInMs)
            .setMarkDestroyedTimeInMs(markDestroyedTimeInMs)
            .setObjectState(objectState);
        metadataNodeManager.handleMessage(record1);
        assertEquals(S3ObjectRecordJsonConverter.write(record1, S3ObjectRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(), metadataNodeManager.getData().root().file("s3Objects", Long.toString(objectId)).contents());

        RemoveS3ObjectRecord record2 = new RemoveS3ObjectRecord().setObjectId(objectId);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("s3Objects").children().containsKey(Long.toString(objectId)));
    }

    @Test
    public void testAssignedStreamIdRecord() {
        long streamId = 1L;
        AssignedStreamIdRecord record1 = new AssignedStreamIdRecord().setAssignedStreamId(streamId);
        metadataNodeManager.handleMessage(record1);
        assertEquals(Long.toString(streamId), metadataNodeManager.getData().root().file("s3Streams", "nextStreamId").contents());
    }

    @Test
    public void testAssignedS3ObjectIdRecord() {
        long objectId1 = 1L;
        AssignedS3ObjectIdRecord record1 = new AssignedS3ObjectIdRecord().setAssignedS3ObjectId(objectId1);
        metadataNodeManager.handleMessage(record1);
        assertEquals(Long.toString(objectId1), metadataNodeManager.getData().root().file("s3Objects", "nextObjectId").contents());
    }

    @Test
    public void testNodeWalMetadataRecord() {
        int nodeId = 1;
        long nodeEpoch = 3L;
        boolean failoverMode = true;
        NodeWALMetadataRecord record1 = new NodeWALMetadataRecord()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setFailoverMode(failoverMode);
        metadataNodeManager.handleMessage(record1);
        assertEquals(NodeWALMetadataRecordJsonConverter.write(record1, RangeRecord.HIGHEST_SUPPORTED_VERSION).toPrettyString(), metadataNodeManager.getData().root().file("nodes", Integer.toString(nodeId), "walMetadata").contents());

        RemoveNodeWALMetadataRecord record2 = new RemoveNodeWALMetadataRecord().setNodeId(nodeId);
        metadataNodeManager.handleMessage(record2);
        assertFalse(metadataNodeManager.getData().root().directory("nodes", Integer.toString(nodeId)).children().containsKey("walMetadata"));
    }

    @Test
    public void testKvRecord() {
        String namespace = "_kafka_FVWxwMClSYObxlUORZANzA";
        String topicId = "4IHvQLhZSJ6szHPcyady3Q";
        String partition = "0";
        String key = namespace + "/" + topicId + "/" + partition;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(1);
        byte[] value = buffer.array();
        KVRecord.KeyValue keyValue = new KVRecord.KeyValue().setKey(key).setValue(value);
        List<KVRecord.KeyValue> keyValues = List.of(keyValue);
        KVRecord kvRecord = new KVRecord().setKeyValues(keyValues);
        metadataNodeManager.handleMessage(kvRecord);
        assertEquals(Arrays.toString(value), metadataNodeManager.getData().root().file("kvRecords", namespace, topicId, partition).contents());

        RemoveKVRecord removeKVRecord = new RemoveKVRecord().setKeys(List.of(key));
        metadataNodeManager.handleMessage(removeKVRecord);
        assertFalse(metadataNodeManager.getData().root().directory("kvRecords", namespace, topicId).children().containsKey(partition));
    }

    @Test
    public void testUpdateNextNodeId() {
        int nodeId = 1;
        UpdateNextNodeIdRecord record = new UpdateNextNodeIdRecord().setNodeId(1);
        metadataNodeManager.handleMessage(record);
        assertEquals(Integer.toString(nodeId), metadataNodeManager.getData().root().file("nodes", "nextNodeId").contents());
    }
}
