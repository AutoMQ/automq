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

import com.automq.stream.s3.metadata.StreamState;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class S3StreamMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamMetadata.class);

    private final long streamId;
    // current epoch, when created but not open, use -1 represent
    private final TimelineLong currentEpoch;
    // rangeIndex, when created but not open, there is no range, use -1 represent
    private final TimelineInteger currentRangeIndex;
    /**
     * The visible start offset of stream, it may be larger than the start offset of current range.
     */
    private final TimelineLong startOffset;
    private final TimelineObject<StreamState> currentState;
    private final Map<String, String> tags;
    private final TimelineHashMap<Integer/*rangeIndex*/, RangeMetadata> ranges;
    private final TimelineHashMap<Long/*objectId*/, S3StreamObject> streamObjects;

    public S3StreamMetadata(long streamId, long currentEpoch, int currentRangeIndex, long startOffset,
                            StreamState currentState, Map<String, String> tags, SnapshotRegistry registry) {
        this.streamId = streamId;
        this.currentEpoch = new TimelineLong(registry);
        this.currentEpoch.set(currentEpoch);
        this.currentRangeIndex = new TimelineInteger(registry);
        this.currentRangeIndex.set(currentRangeIndex);
        this.startOffset = new TimelineLong(registry);
        this.startOffset.set(startOffset);
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
        return ranges.get(currentRangeIndex.get()).nodeId();
    }

    public long startOffset() {
        return startOffset.get();
    }

    public void startOffset(long offset) {
        this.startOffset.set(offset);
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

    public Map<Integer, RangeMetadata> ranges() {
        return ranges;
    }

    public RangeMetadata currentRangeMetadata() {
        return ranges.get(currentRangeIndex.get());
    }

    public void updateEndOffset(long newEndOffset) {
        RangeMetadata rangeMetadata = ranges.get(currentRangeIndex.get());
        if (rangeMetadata == null) {
            LOGGER.error("[UNEXPECTED] cannot find range={}", currentRangeIndex.get());
            return;
        }
        if (rangeMetadata.endOffset() < newEndOffset) {
            ranges.put(rangeMetadata.rangeIndex(), new RangeMetadata(
                    rangeMetadata.streamId(),
                    rangeMetadata.epoch(),
                    rangeMetadata.rangeIndex(),
                    rangeMetadata.startOffset(),
                    newEndOffset,
                    rangeMetadata.nodeId()
            ));
        }
    }

    public Map<Long, S3StreamObject> streamObjects() {
        return streamObjects;
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
