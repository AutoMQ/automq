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

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.controller.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.StreamState;

public class S3StreamMetadataImage {

    public static final S3StreamMetadataImage EMPTY =
        new S3StreamMetadataImage(S3StreamConstant.INVALID_STREAM_ID, S3StreamConstant.INIT_EPOCH, StreamState.CLOSED,
            S3StreamConstant.INIT_RANGE_INDEX, S3StreamConstant.INIT_START_OFFSET, Map.of(), Map.of());

    private final long streamId;

    private final long epoch;

    private final int rangeIndex;

    private final long startOffset;

    private final StreamState state;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> ranges;

    private final Map<Long/*objectId*/, S3StreamObject> streamObjects;

    public S3StreamMetadataImage(
        long streamId, long epoch, StreamState state,
        int rangeIndex,
        long startOffset,
        Map<Integer, RangeMetadata> ranges,
        Map<Long, S3StreamObject> streamObjects) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.state = state;
        this.rangeIndex = rangeIndex;
        this.startOffset = startOffset;
        this.ranges = ranges;
        this.streamObjects = streamObjects;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new S3StreamRecord()
            .setStreamId(streamId)
            .setRangeIndex(rangeIndex)
            .setStreamState(state.toByte())
            .setEpoch(epoch)
            .setStartOffset(startOffset));
        ranges.values().forEach(rangeMetadata -> writer.write(rangeMetadata.toRecord()));
        streamObjects.values().forEach(streamObject -> writer.write(streamObject.toRecord()));
    }


    public Map<Integer, RangeMetadata> getRanges() {
        return ranges;
    }

    public Map<Long, S3StreamObject> getStreamObjects() {
        return streamObjects;
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

    public int rangeIndex() {
        return rangeIndex;
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
            this.rangeIndex == that.rangeIndex &&
            this.startOffset == that.startOffset &&
            this.ranges.equals(that.ranges) &&
            this.streamObjects.equals(that.streamObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, epoch, state, rangeIndex, startOffset, ranges, streamObjects);
    }
}
