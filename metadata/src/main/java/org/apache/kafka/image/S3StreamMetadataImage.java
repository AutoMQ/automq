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

import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamState;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class S3StreamMetadataImage {
    public static final S3StreamMetadataImage EMPTY =
            new S3StreamMetadataImage(S3StreamConstant.INVALID_STREAM_ID, S3StreamConstant.INIT_EPOCH, StreamState.CLOSED, S3StreamConstant.INIT_START_OFFSET, Collections.emptyList(), Collections.emptyList());

    private final long streamId;

    private final long epoch;

    private final long startOffset;

    private final StreamState state;

    private final List<RangeMetadata> ranges;

    final DeltaMap<Long /* objectId */, S3StreamObject> streamObjectsMap;

    private List<S3StreamObject> sortedStreamObjects;

    private NavigableMap<Long /* stream object start offset */, Integer /* stream object index */> streamObjectOffsets;

    public S3StreamMetadataImage(
            long streamId, long epoch, StreamState state,
            long startOffset,
            List<RangeMetadata> ranges,
            List<S3StreamObject> sortedStreamObjects) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.state = state;
        this.startOffset = startOffset;
        this.ranges = ranges;
        DeltaMap<Long, S3StreamObject> streamObjectsMap = new DeltaMap<>(new int[]{10});
        sortedStreamObjects.forEach(streamObject -> streamObjectsMap.put(streamObject.objectId(), streamObject));
        this.streamObjectsMap = streamObjectsMap;
        this.sortedStreamObjects = sortedStreamObjects;
    }

    public S3StreamMetadataImage(
            long streamId, long epoch, StreamState state,
            long startOffset,
            List<RangeMetadata> ranges,
            DeltaMap<Long, S3StreamObject> streamObjectsMap) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.state = state;
        this.startOffset = startOffset;
        this.ranges = ranges;
        this.streamObjectsMap = streamObjectsMap;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new S3StreamRecord()
                .setStreamId(streamId)
                .setRangeIndex(currentRangeIndex())
                .setStreamState(state.toByte())
                .setEpoch(epoch)
                .setStartOffset(startOffset));
        ranges.forEach(rangeMetadata -> writer.write(rangeMetadata.toRecord()));
        streamObjectsMap.forEach((id, obj) -> writer.write(obj.toRecord()));
    }


    public List<RangeMetadata> getRanges() {
        return ranges;
    }

    public RangeMetadata lastRange() {
        if (ranges.isEmpty()) {
            return null;
        }
        return ranges.get(ranges.size() - 1);
    }

    public int currentRangeIndex() {
        if (ranges.isEmpty()) {
            return S3StreamConstant.INIT_RANGE_INDEX;
        }
        return lastRange().rangeIndex();
    }

    public int getRangeContainsOffset(long offset) {
        int currentRangeIndex = currentRangeIndex();
        return Collections.binarySearch(ranges, offset, (o1, o2) -> {
            long startOffset;
            long endOffset;
            long offset1;
            int revert = -1;
            RangeMetadata range;
            if (o1 instanceof RangeMetadata) {
                range = (RangeMetadata) o1;
                offset1 = (Long) o2;
                revert = 1;
            } else {
                range = (RangeMetadata) o2;
                offset1 = (Long) o1;
            }
            startOffset = range.startOffset();
            endOffset = range.rangeIndex() == currentRangeIndex ? Long.MAX_VALUE : range.endOffset();

            if (endOffset <= offset1) {
                return -1 * revert;
            } else if (startOffset > offset1) {
                return revert;
            } else {
                return 0;
            }
        });
    }

    public List<S3StreamObject> getStreamObjects() {
        if (sortedStreamObjects != null) {
            return sortedStreamObjects;
        }
        List<S3StreamObject> streamObjects = new ArrayList<>();
        streamObjectsMap.forEach((objectId, streamObject) -> streamObjects.add(streamObject));
        streamObjects.sort(Comparator.comparingLong(S3StreamObject::startOffset));
        this.sortedStreamObjects = streamObjects;
        return streamObjects;
    }

    public int floorStreamObjectIndex(long offset) {
        List<S3StreamObject> sortedStreamObjects = getStreamObjects();
        if (streamObjectOffsets == null) {
            // TODO: optimize, get floor index without construct sorted map
            NavigableMap<Long, Integer> streamObjectOffsets = new TreeMap<>();
            for (int i = 0; i < sortedStreamObjects.size(); i++) {
                S3StreamObject streamObject = sortedStreamObjects.get(i);
                streamObjectOffsets.put(streamObject.streamOffsetRange().startOffset(), i);
            }
            this.streamObjectOffsets = streamObjectOffsets;
        }
        Map.Entry<Long, Integer> entry = streamObjectOffsets.floorEntry(offset);
        if (entry == null) {
            return -1;
        }
        return entry.getValue();
    }

    public long getEpoch() {
        return epoch;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getStreamId() {
        return streamId;
    }

    public long startOffset() {
        return startOffset;
    }

    public StreamState state() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3StreamMetadataImage that = (S3StreamMetadataImage) o;
        return this.streamId == that.streamId &&
                this.epoch == that.epoch &&
                this.state == that.state &&
                this.startOffset == that.startOffset &&
                this.ranges.equals(that.ranges) &&
                this.streamObjectsMap.equals(that.streamObjectsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, epoch, state, startOffset, ranges);
    }

    @Override
    public String toString() {
        return "S3StreamMetadataImage{" +
                "streamId=" + streamId +
                ", epoch=" + epoch +
                ", startOffset=" + startOffset +
                ", state=" + state +
                ", ranges=" + ranges +
                ", streamObjects=" + streamObjectsMap +
                '}';
    }
}
