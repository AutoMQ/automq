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
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

public class S3StreamMetadataImage {

    public static final S3StreamMetadataImage EMPTY =
        new S3StreamMetadataImage(-1L, -1L, -1L, Map.of(), Map.of());

    private final long streamId;

    private final long epoch;

    private final long startOffset;

    private final Map<Integer/*rangeIndex*/, RangeMetadata> ranges;

    private final Map<Long/*objectId*/, S3StreamObject> streamObjects;

    public S3StreamMetadataImage(
        long streamId,
        long epoch,
        long startOffset,
        Map<Integer, RangeMetadata> ranges,
        Map<Long, S3StreamObject> streamObjects) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.ranges = ranges;
        this.streamObjects = streamObjects;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new S3StreamRecord()
            .setStreamId(streamId)
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
            this.startOffset == that.startOffset &&
            this.ranges.equals(that.ranges) &&
            this.streamObjects.equals(that.streamObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, epoch, startOffset, ranges, streamObjects);
    }
}
