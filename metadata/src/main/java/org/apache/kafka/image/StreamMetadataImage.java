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
import java.util.Set;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.StreamRecord;
import org.apache.kafka.controller.stream.RangeMetadata;
import org.apache.kafka.controller.stream.s3.StreamObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

public class StreamMetadataImage {

    private final Long streamId;

    private final Integer epoch;

    private final Long startOffset;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> ranges;

    private final Set<StreamObject> streams;

    public StreamMetadataImage(
        Long streamId,
        Integer epoch,
        Long startOffset,
        Map<Integer, RangeMetadata> ranges,
        Set<StreamObject> streams) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.ranges = ranges;
        this.streams = streams;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new StreamRecord()
            .setStreamId(streamId)
            .setEpoch(epoch)
            .setStartOffset(startOffset));
        ranges.values().forEach(rangeMetadata -> writer.write(rangeMetadata.toRecord()));
        streams.forEach(streamObject -> writer.write(streamObject.toRecord()));
    }

    public Map<Integer, RangeMetadata> getRanges() {
        return ranges;
    }

    public Set<StreamObject> getStreams() {
        return streams;
    }

    public Integer getEpoch() {
        return epoch;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Long getStreamId() {
        return streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamMetadataImage that = (StreamMetadataImage) o;
        return Objects.equals(streamId, that.streamId) && Objects.equals(epoch, that.epoch) && Objects.equals(startOffset,
            that.startOffset) && Objects.equals(ranges, that.ranges) && Objects.equals(streams, that.streams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, epoch, startOffset, ranges, streams);
    }
}
