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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.utils.biniarysearch.AbstractOrderedCollection;
import com.automq.stream.utils.biniarysearch.ComparableItem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public final class S3StreamsMetadataImage {

    public static final S3StreamsMetadataImage EMPTY =
        new S3StreamsMetadataImage(-1, new DeltaMap<>(new int[] {1000, 10000}), new DeltaMap<>(new int[] {1000, 10000}));

    private final long nextAssignedStreamId;

    private final DeltaMap<Long/*streamId*/, S3StreamMetadataImage> streamsMetadata;

    private final DeltaMap<Integer/*nodeId*/, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata;

    public S3StreamsMetadataImage(
        long assignedStreamId,
        DeltaMap<Long, S3StreamMetadataImage> streamsMetadata,
        DeltaMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata) {
        this.nextAssignedStreamId = assignedStreamId + 1;
        this.streamsMetadata = streamsMetadata;
        this.nodeStreamSetObjectMetadata = nodeStreamSetObjectMetadata;
    }

    boolean isEmpty() {
        return this.nodeStreamSetObjectMetadata.isEmpty() && this.streamsMetadata.isEmpty();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(
            new ApiMessageAndVersion(
                new AssignedStreamIdRecord().setAssignedStreamId(nextAssignedStreamId - 1), (short) 0));
        streamsMetadata.forEach((k, v) -> v.write(writer, options));
        nodeStreamSetObjectMetadata.forEach((k, v) -> v.write(writer, options));
    }

    /**
     * Get objects in range [startOffset, endOffset) with limit.
     *
     * @param streamId stream id
     * @param startOffset inclusive start offset of the stream
     * @param endOffset exclusive end offset of the stream.
     *                  NOTE: NOOP_OFFSET means to retrieve as many objects as it's within the limit.
     * @param limit max number of s3 objects to return
     * @return s3 objects within the range
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public InRangeObjects getObjects(long streamId, long startOffset, long endOffset, int limit) {
        if (streamId < 0 || limit < 0 || (endOffset != ObjectUtils.NOOP_OFFSET && startOffset > endOffset)) {
            return InRangeObjects.INVALID;
        }
        S3StreamMetadataImage stream = streamsMetadata.get(streamId);
        if (stream == null || startOffset < stream.startOffset()) {
            return InRangeObjects.INVALID;
        }
        List<S3ObjectMetadata> objects = new LinkedList<>();
        long nextStartOffset = startOffset;

        // floor value < 0 means that all stream objects' ranges are greater than startOffset
        int streamObjectIndex = Math.max(0, stream.floorStreamObjectIndex(startOffset));
        List<S3StreamObject> streamObjects = stream.getStreamObjects();

        int lastRangeIndex = -1;
        List<S3StreamSetObject> streamSetObjects = null;
        int streamSetObjectIndex = 0;
        for (; ; ) {
            int roundStartObjectSize = objects.size();

            // try to find consistent stream objects
            for (; streamObjectIndex < streamObjects.size(); streamObjectIndex++) {
                S3StreamObject streamObject = streamObjects.get(streamObjectIndex);
                if (streamObject.startOffset() != nextStartOffset) {
                    //noinspection StatementWithEmptyBody
                    if (objects.isEmpty() && streamObject.startOffset() <= nextStartOffset && streamObject.endOffset() > nextStartOffset) {
                        // it's the first object, we only need the stream object contains the nextStartOffset
                    } else if (streamObject.endOffset() <= nextStartOffset) {
                        // the stream object not match the requirement, move to the next stream object
                        continue;
                    } else {
                        // the streamObject.startOffset() > nextStartOffset
                        break;
                    }
                }
                objects.add(streamObject.toMetadata());
                nextStartOffset = streamObject.endOffset();
                if (objects.size() >= limit || (endOffset != ObjectUtils.NOOP_OFFSET && nextStartOffset >= endOffset)) {
                    return new InRangeObjects(streamId, objects);
                }
            }

            if (streamSetObjects == null) {
                int rangeIndex = stream.getRangeContainsOffset(nextStartOffset);
                // 1. can not find the range containing nextStartOffset, or
                // 2. the range is the same as the last one, which means the nextStartOffset does not move on.
                if (rangeIndex < 0 || lastRangeIndex == rangeIndex) {
                    break;
                }
                lastRangeIndex = rangeIndex;
                RangeMetadata range = stream.getRanges().get(rangeIndex);
                NodeS3StreamSetObjectMetadataImage node = nodeStreamSetObjectMetadata.get(range.nodeId());
                streamSetObjects = node == null ? Collections.emptyList() : node.orderList();
                streamSetObjectIndex = 0;
            }

            for (; streamSetObjectIndex < streamSetObjects.size(); streamSetObjectIndex++) {
                S3StreamSetObject streamSetObject = streamSetObjects.get(streamSetObjectIndex);
                StreamOffsetRange streamOffsetRange = search(streamSetObject.offsetRangeList(), streamId);
                // skip the stream set object not containing the stream or the range is before the nextStartOffset
                if (streamOffsetRange == null || streamOffsetRange.endOffset() <= nextStartOffset) {
                    continue;
                }
                if ((streamOffsetRange.startOffset() == nextStartOffset)
                    || (objects.isEmpty() && streamOffsetRange.startOffset() < nextStartOffset)) {
                    objects.add(new S3ObjectMetadata(streamSetObject.objectId(), S3ObjectType.STREAM_SET, List.of(streamOffsetRange),
                        streamSetObject.dataTimeInMs()));
                    nextStartOffset = streamOffsetRange.endOffset();
                    if (objects.size() >= limit || (endOffset != ObjectUtils.NOOP_OFFSET && nextStartOffset >= endOffset)) {
                        return new InRangeObjects(streamId, objects);
                    }
                } else {
                    // We keep the corresponding object ( with a range startOffset > nextStartOffset) by not changing
                    // the streamSetObjectIndex. This object may be picked up in the next round.
                    break;
                }
            }
            // case 1. streamSetObjectIndex >= streamSetObjects.size(), which means we have reached the end of the stream set objects.
            // case 2. objects.size() == roundStartObjectSize, which means we have not found any new object in this round.
            if (streamSetObjectIndex >= streamSetObjects.size() || objects.size() == roundStartObjectSize) {
                // move to the next range
                // This can ensure that we can break the loop.
                streamSetObjects = null;
            }
        }
        return new InRangeObjects(streamId, objects);
    }

    /**
     * Get stream objects in range [startOffset, endOffset) with limit. It will throw IllegalArgumentException if limit or streamId is invalid.
     *
     * @param streamId    stream id
     * @param startOffset inclusive start offset of the stream
     * @param endOffset   exclusive end offset of the stream
     * @param limit       max number of stream objects to return
     * @return stream objects
     */
    public List<S3StreamObject> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        S3StreamMetadataImage stream = streamsMetadata.get(streamId);
        if (stream == null) {
            throw new IllegalArgumentException("stream not found");
        }
        List<S3StreamObject> streamObjectsMetadata = stream.getStreamObjects();
        if (streamObjectsMetadata == null || streamObjectsMetadata.isEmpty()) {
            return Collections.emptyList();
        }
        return streamObjectsMetadata.stream().filter(obj -> {
            long objectStartOffset = obj.streamOffsetRange().startOffset();
            long objectEndOffset = obj.streamOffsetRange().endOffset();
            return objectStartOffset < endOffset && objectEndOffset > startOffset;
        }).sorted(Comparator.comparing(S3StreamObject::streamOffsetRange)).limit(limit).collect(Collectors.toCollection(ArrayList::new));
    }

    public List<S3StreamSetObject> getStreamSetObjects(int nodeId) {
        NodeS3StreamSetObjectMetadataImage wal = nodeStreamSetObjectMetadata.get(nodeId);
        if (wal == null) {
            return Collections.emptyList();
        }
        return wal.orderList();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        S3StreamsMetadataImage other = (S3StreamsMetadataImage) obj;
        return this.nextAssignedStreamId == other.nextAssignedStreamId
            && this.streamsMetadata.equals(other.streamsMetadata)
            && this.nodeStreamSetObjectMetadata.equals(other.nodeStreamSetObjectMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedStreamId, streamsMetadata, nodeStreamSetObjectMetadata);
    }

    public DeltaMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeWALMetadata() {
        return nodeStreamSetObjectMetadata;
    }

    public DeltaMap<Long, S3StreamMetadataImage> streamsMetadata() {
        return streamsMetadata;
    }

    public long nextAssignedStreamId() {
        return nextAssignedStreamId;
    }

    @Override
    public String toString() {
        return "S3StreamsMetadataImage{nextAssignedStreamId=" + nextAssignedStreamId + '}';
    }

    public static StreamOffsetRange search(List<StreamOffsetRange> ranges, long streamId) {
        int index = new StreamOffsetRanges(ranges).search(streamId);
        if (index < 0) {
            return null;
        }
        return ranges.get(index);
    }

    static class StreamOffsetRanges extends AbstractOrderedCollection<Long> {
        private final List<StreamOffsetRange> ranges;

        public StreamOffsetRanges(List<StreamOffsetRange> ranges) {
            this.ranges = ranges;
        }

        @Override
        protected int size() {
            return ranges.size();
        }

        @Override
        protected ComparableItem<Long> get(int index) {
            StreamOffsetRange range = ranges.get(index);
            return new ComparableItem<>() {
                @Override
                public boolean isLessThan(Long o) {
                    return range.streamId() < o;
                }

                @Override
                public boolean isGreaterThan(Long o) {
                    return range.streamId() > o;
                }
            };
        }

    }
}
