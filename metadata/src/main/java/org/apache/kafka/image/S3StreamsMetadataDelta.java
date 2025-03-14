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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveNodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveRangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.metadata.StreamOffsetRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class S3StreamsMetadataDelta {

    private final S3StreamsMetadataImage image;

    private long currentAssignedStreamId;

    private final Map<Long, S3StreamMetadataDelta> changedStreams = new HashMap<>();

    private final Map<Integer, NodeS3WALMetadataDelta> changedNodes = new HashMap<>();

    private final Set<Long> newStreams = new HashSet<>();
    private final Set<Long> deletedStreams = new HashSet<>();
    // TODO: when we recycle the node's memory data structure
    // We don't use pair of specify NodeCreateRecord and NodeRemoveRecord to create or remove nodes, and
    // we create NodeStreamMetadataImage when we create the first StreamSetObjectRecord for a node,
    // so we should decide when to recycle the node's memory data structure
    private final Set<Integer> deletedNodes = new HashSet<>();

    private final Map<Long, Long> changedStreamEndOffsets = new HashMap<>();

    public S3StreamsMetadataDelta(S3StreamsMetadataImage image) {
        this.image = image;
        this.currentAssignedStreamId = image.nextAssignedStreamId() - 1;
    }

    public void replay(AssignedStreamIdRecord record) {
        this.currentAssignedStreamId = record.assignedStreamId();
    }

    public void replay(S3StreamRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
        deletedStreams.remove(record.streamId());
    }

    public void replay(RemoveS3StreamRecord record) {
        // add the streamId to the deletedStreams
        deletedStreams.add(record.streamId());
        changedStreams.remove(record.streamId());
        newStreams.remove(record.streamId());
    }

    public void replay(NodeWALMetadataRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
        deletedNodes.remove(record.nodeId());
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
        // https://github.com/AutoMQ/automq/issues/2333
        // try to fix the old stream end offset
        updateStreamEndOffset(record.streamId(), record.endOffset());
    }

    public void replay(RemoveS3StreamObjectRecord record) {
        getOrCreateStreamMetadataDelta(record.streamId()).replay(record);
    }

    public void replay(S3StreamSetObjectRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
        if (record.ranges().length != 0) {
            List<StreamOffsetRange> range = S3StreamSetObject.decode(record.ranges());
            range.forEach(r -> updateStreamEndOffset(r.streamId(), r.endOffset()));
        }
    }

    public void replay(RemoveStreamSetObjectRecord record) {
        getOrCreateNodeStreamMetadataDelta(record.nodeId()).replay(record);
    }

    public void replay(S3StreamEndOffsetsRecord record) {
        for (StreamEndOffset streamEndOffset : S3StreamEndOffsetsCodec.decode(record.endOffsets())) {
            updateStreamEndOffset(streamEndOffset.streamId(), streamEndOffset.endOffset());
        }
    }

    public Set<Long> changedStreams() {
        Set<Long> set = new HashSet<>();
        set.addAll(changedStreams.keySet());
        set.addAll(changedStreamEndOffsets.keySet());
        return set;
    }

    private void updateStreamEndOffset(long streamId, long newEndOffset) {
        changedStreamEndOffsets.compute(streamId, (id, offset) -> {
            if (offset == null) {
                return newEndOffset;
            }
            return Math.max(offset, newEndOffset);
        });
    }

    private S3StreamMetadataDelta getOrCreateStreamMetadataDelta(Long streamId) {
        S3StreamMetadataDelta delta = changedStreams.get(streamId);
        if (delta == null) {
            delta = new S3StreamMetadataDelta(image.timelineStreamMetadata().getOrDefault(streamId, S3StreamMetadataImage.EMPTY));
            changedStreams.put(streamId, delta);
            if (!image.timelineStreamMetadata().containsKey(streamId)) {
                newStreams.add(streamId);
            }
        }
        return delta;
    }

    private NodeS3WALMetadataDelta getOrCreateNodeStreamMetadataDelta(Integer nodeId) {
        NodeS3WALMetadataDelta delta = changedNodes.get(nodeId);
        if (delta == null) {
            delta = new NodeS3WALMetadataDelta(
                image.timelineNodeMetadata().
                    getOrDefault(nodeId, NodeS3StreamSetObjectMetadataImage.EMPTY));
            changedNodes.put(nodeId, delta);
        }
        return delta;
    }

    S3StreamsMetadataImage apply() {

        RegistryRef registry = image.registryRef();
        TimelineHashMap<Long, Long> newStreamEndOffsets;
        TimelineHashMap<Long, S3StreamMetadataImage> newStreamMetadataMap;
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> newNodeMetadataMap;
        TimelineHashMap<TopicIdPartition, Set<Long>> partition2streams;
        TimelineHashMap<Long, TopicIdPartition> stream2partition;
        if (registry == RegistryRef.NOOP) {
            registry = new RegistryRef();
            newStreamEndOffsets = new TimelineHashMap<>(registry.registry(), 100000);
            newStreamMetadataMap = new TimelineHashMap<>(registry.registry(), 100000);
            newNodeMetadataMap = new TimelineHashMap<>(registry.registry(), 100);
            partition2streams = new TimelineHashMap<>(registry.registry(), 100000);
            stream2partition = new TimelineHashMap<>(registry.registry(), 100000);
        } else {
            newStreamEndOffsets = image.timelineStreamEndOffsets();
            newStreamMetadataMap = image.timelineStreamMetadata();
            newNodeMetadataMap = image.timelineNodeMetadata();
            partition2streams = image.partition2streams();
            stream2partition = image.stream2partition();
        }
        registry.inLock(() -> {
            // apply the delta changes of old streams since the last image
            changedStreams.forEach((streamId, delta) -> newStreamMetadataMap.put(streamId, delta.apply()));
            deletedStreams.forEach(newStreamMetadataMap::remove);

            changedStreamEndOffsets.forEach((streamId, newEndOffset) -> newStreamEndOffsets.compute(streamId, (key, oldEndOffset) -> {
                if (!newStreamMetadataMap.containsKey(streamId)) {
                    return null;
                }
                if (oldEndOffset == null) {
                    return newEndOffset;
                }
                // S3StreamSetObjectRecord maybe the SSO compaction record, we need ignore the offset.
                return Math.max(oldEndOffset, newEndOffset);
            }));
            deletedStreams.forEach(newStreamEndOffsets::remove);

            // apply the delta changes of old nodes since the last image
            this.changedNodes.forEach((nodeId, delta) -> newNodeMetadataMap.put(nodeId, delta.apply()));
            // remove the deleted nodes
            deletedNodes.forEach(newNodeMetadataMap::remove);

            Map<TopicIdPartition, Set<Long>> newPartition2streams = new HashMap<>();
            Function<TopicIdPartition, Set<Long>> getPartitionStreams = k -> {
                Set<Long> s = partition2streams.get(k);
                s = new HashSet<>(s == null ? Collections.emptySet() : s);
                partition2streams.put(k, s);
                return s;
            };
            for (Long streamId : deletedStreams) {
                TopicIdPartition tp = stream2partition.get(streamId);
                if (tp == null) {
                    continue;
                }
                stream2partition.remove(streamId);
                Set<Long> partitionStreams = newPartition2streams.computeIfAbsent(tp, getPartitionStreams);
                if (partitionStreams != null) {
                    partitionStreams.remove(streamId);
                }
            }
            for (Long streamId : newStreams) {
                S3StreamRecord.Tag topicTag = newStreamMetadataMap.get(streamId).tags().find(StreamTags.Topic.KEY);
                S3StreamRecord.Tag partitionTag = newStreamMetadataMap.get(streamId).tags().find(StreamTags.Partition.KEY);
                if (topicTag == null || partitionTag == null) {
                    continue;
                }
                try {
                    Uuid topicId = StreamTags.Topic.decode(topicTag.value());
                    int partition = StreamTags.Partition.decode(partitionTag.value());
                    TopicIdPartition tp = new TopicIdPartition(topicId, partition);
                    Set<Long> partitionStreams = newPartition2streams.computeIfAbsent(tp, getPartitionStreams);
                    if (partitionStreams != null) {
                        partitionStreams.add(streamId);
                    }
                    stream2partition.put(streamId, tp);
                } catch (Throwable e) {
                    // skip
                }
            }
        });
        registry = registry.next();
        return new S3StreamsMetadataImage(currentAssignedStreamId, registry, newStreamMetadataMap, newNodeMetadataMap,
            partition2streams, stream2partition, newStreamEndOffsets);
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
