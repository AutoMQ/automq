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

package org.apache.kafka.image;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.metadata.AdvanceRangeRecord;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;

public final class S3StreamsMetadataDelta {

    private final S3StreamsMetadataImage image;

    private long currentAssignedStreamId;

    private final Map<Long, S3StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, NodeS3WALMetadataDelta> changedNodes = new HashMap<>();

    private final Set<Long> deletedStreams = new HashSet<>();
    // TODO: when we recycle the node's memory data structure
    // We don't use pair of specify NodeCreateRecord and NodeRemoveRecord to create or remove nodes, and
    // we create NodeStreamMetadataImage when we create the first WALObjectRecord for a node,
    // so we should decide when to recycle the node's memory data structure
    private final Set<Integer> deletedNodes = new HashSet<>();

    public S3StreamsMetadataDelta(S3StreamsMetadataImage image) {
        this.image = image;
        this.currentAssignedStreamId = image.nextAssignedStreamId() - 1;
    }

    public void replay(AssignedStreamIdRecord record) {
        this.currentAssignedStreamId = record.assignedStreamId();
    }

    public void replay(S3StreamRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
        if (deletedStreams.contains(record.streamId())) {
            deletedStreams.remove(record.streamId());
        }
    }

    public void replay(RemoveS3StreamRecord record) {
        // add the streamId to the deletedStreams
        deletedStreams.add(record.streamId());
        changedStreams.remove(record.streamId());
    }

    public void replay(NodeWALMetadataRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
        if (deletedNodes.contains(record.nodeId())) {
            deletedNodes.remove(record.nodeId());
        }
    }

    public void replay(RemoveNodeWALMetadataRecord record) {
        // add the nodeId to the deletedNodes
        deletedNodes.add(record.nodeId());
        changedNodes.remove(record.nodeId());
    }

    public void replay(RangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(RemoveRangeRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(S3StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
        getOrCreateStreamMetadataDelta(record.streamId()).replay(new AdvanceRangeRecord()
            .setStartOffset(record.startOffset())
            .setEndOffset(record.endOffset()));
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(WALObjectRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
        record.streamsIndex().forEach(index -> {
            getOrCreateStreamMetadataDelta(index.streamId()).replay(new AdvanceRangeRecord()
                .setStartOffset(index.startOffset())
                .setEndOffset(index.endOffset()));
        });
    }

    public void replay(RemoveWALObjectRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
    }

    private S3StreamMetadataDelta getOrCreateStreamMetadataDelta(Long streamId) {
        S3StreamMetadataDelta delta = changedStreams.get(streamId);
        if (delta == null) {
            delta = new S3StreamMetadataDelta(image.streamsMetadata().getOrDefault(streamId, S3StreamMetadataImage.EMPTY));
            changedStreams.put(streamId, delta);
        }
        return delta;
    }

    private NodeS3WALMetadataDelta getOrCreateNodeStreamMetadataDelta(Integer nodeId) {
        NodeS3WALMetadataDelta delta = changedNodes.get(nodeId);
        if (delta == null) {
            delta = new NodeS3WALMetadataDelta(
                image.nodeWALMetadata().
                    getOrDefault(nodeId, NodeS3WALMetadataImage.EMPTY));
            changedNodes.put(nodeId, delta);
        }
        return delta;
    }

    S3StreamsMetadataImage apply() {
        Map<Long, S3StreamMetadataImage> newStreams = new HashMap<>(image.streamsMetadata());
        Map<Integer, NodeS3WALMetadataImage> newNodeStreams = new HashMap<>(image.nodeWALMetadata());

        // apply the delta changes of old streams since the last image
        this.changedStreams.forEach((streamId, delta) -> {
            S3StreamMetadataImage newS3StreamMetadataImage = delta.apply();
            newStreams.put(streamId, newS3StreamMetadataImage);
        });
        // remove the deleted streams
        deletedStreams.forEach(newStreams::remove);

        // apply the delta changes of old nodes since the last image
        this.changedNodes.forEach((nodeId, delta) -> {
            NodeS3WALMetadataImage newNodeS3WALMetadataImage = delta.apply();
            newNodeStreams.put(nodeId, newNodeS3WALMetadataImage);
        });
        // remove the deleted nodes
        deletedNodes.forEach(newNodeStreams::remove);

        return new S3StreamsMetadataImage(currentAssignedStreamId, newStreams, newNodeStreams);
    }

    @Override
    public String toString() {
        return "S3StreamsMetadataDelta{" +
            "image=" + image +
            ", currentAssignedStreamId=" + currentAssignedStreamId +
            ", changedStreams=" + changedStreams +
            ", changedNodes=" + changedNodes +
            ", deletedStreams=" + deletedStreams +
            ", deletedNodes=" + deletedNodes +
            '}';
    }
}
