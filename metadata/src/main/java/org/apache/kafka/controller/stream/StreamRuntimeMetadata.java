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
package org.apache.kafka.controller.stream;

import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;

import com.automq.stream.s3.metadata.StreamState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class StreamRuntimeMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamRuntimeMetadata.class);

    private final long streamId;
    // current epoch, when created but not open, use -1 represent
    private final TimelineLong currentEpoch;
    // rangeIndex, when created but not open, there is no range, use -1 represent
    private final TimelineInteger currentRangeIndex;
    /**
     * The visible start offset of stream, it may be larger than the start offset of current range.
     */
    private final TimelineLong startOffset;
    private final TimelineLong endOffset;
    private final TimelineObject<StreamState> currentState;
    private Map<String, String> tags;
    private final TimelineHashMap<Integer/*rangeIndex*/, RangeMetadata> ranges;
    private final TimelineHashMap<Long/*objectId*/, S3StreamObject> streamObjects;

    public StreamRuntimeMetadata(long streamId, long currentEpoch, int currentRangeIndex, long startOffset,
        StreamState currentState, Map<String, String> tags, SnapshotRegistry registry) {
        this.streamId = streamId;
        this.currentEpoch = new TimelineLong(registry);
        this.currentEpoch.set(currentEpoch);
        this.currentRangeIndex = new TimelineInteger(registry);
        this.currentRangeIndex.set(currentRangeIndex);
        this.startOffset = new TimelineLong(registry);
        this.startOffset.set(startOffset);
        this.endOffset = new TimelineLong(registry);
        this.currentState = new TimelineObject<StreamState>(registry, currentState);
        this.tags = tags;
        this.ranges = new TimelineHashMap<>(registry, 0);
        this.streamObjects = new TimelineHashMap<>(registry, 0);
    }

    public long streamId() {
        return streamId;
    }

    public long currentEpoch() {
        return currentEpoch.get();
    }

    public void currentEpoch(long epoch) {
        this.currentEpoch.set(epoch);
    }

    public int currentRangeIndex() {
        return currentRangeIndex.get();
    }

    public void currentRangeIndex(int currentRangeIndex) {
        this.currentRangeIndex.set(currentRangeIndex);
    }

    /**
     * Return the owner (node id) of current range.
     */
    public int currentRangeOwner() {
        if (ranges.isEmpty()) {
            // there is no range in a new stream
            return -1;
        }
        return ranges.get(currentRangeIndex.get()).nodeId();
    }

    public long startOffset() {
        return startOffset.get();
    }

    public void startOffset(long offset) {
        this.startOffset.set(offset);
    }

    public long endOffset() {
        return this.endOffset.get();
    }

    public StreamState currentState() {
        return currentState.get();
    }

    public void currentState(StreamState state) {
        this.currentState.set(state);
    }

    public Map<String, String> tags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<Integer, RangeMetadata> ranges() {
        return ranges;
    }

    public RangeMetadata currentRangeMetadata() {
        return ranges.get(currentRangeIndex.get());
    }

    public void endOffset(long newEndOffset) {
        long oldEndOffset = endOffset.get();
        if (newEndOffset > oldEndOffset) {
            endOffset.set(newEndOffset);
        }
    }

    public Map<Long, S3StreamObject> streamObjects() {
        return streamObjects;
    }

    public List<RangeMetadata> checkRemovableRanges() {
        NavigableMap<Long, S3StreamObject> objects = new TreeMap<>();
        streamObjects.forEach((objectId, object) -> objects.put(object.startOffset(), object));
        // Get the first continuous data range which stream objects covered.
        long startOffset = -1L;
        long endOffset = -1L;
        for (S3StreamObject object : objects.values()) {
            if (startOffset == -1L) {
                startOffset = object.startOffset();
                endOffset = object.endOffset();
                continue;
            }
            if (object.startOffset() != endOffset) {
                break;
            }
            endOffset = object.endOffset();
        }
        if (startOffset == -1L) {
            return Collections.emptyList();
        }
        List<RangeMetadata> removableRanges = new ArrayList<>();
        List<RangeMetadata> ranges = this.ranges.values().stream().sorted(RangeMetadata::compareTo).collect(Collectors.toList());
        for (int i = 0; i < ranges.size() - 1; i++) {
            // ranges.size() - 1: skip the last active range
            RangeMetadata range = ranges.get(i);
            if (startOffset <= range.startOffset() && range.endOffset() <= endOffset) {
                removableRanges.add(range);
            }
        }
        return removableRanges;
    }

    @Override
    public String toString() {
        return "S3StreamMetadata{" +
            "currentEpoch=" + currentEpoch.get() +
            ", currentState=" + currentState.get() +
            ", currentRangeIndex=" + currentRangeIndex.get() +
            ", startOffset=" + startOffset.get() +
            ", ranges=" + ranges +
            ", streamObjects=" + streamObjects +
            '}';
    }
}
