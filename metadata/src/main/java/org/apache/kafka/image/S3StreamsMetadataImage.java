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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.utils.biniarysearch.AbstractOrderedCollection;
import com.automq.stream.utils.biniarysearch.ComparableItem;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

public final class S3StreamsMetadataImage {

    public static final S3StreamsMetadataImage EMPTY =
            new S3StreamsMetadataImage(-1, Collections.emptyMap(), Collections.emptyMap());

    private long nextAssignedStreamId;

    private final Map<Long/*streamId*/, S3StreamMetadataImage> streamsMetadata;

    private final Map<Integer/*nodeId*/, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata;

    public S3StreamsMetadataImage(
            long assignedStreamId,
            Map<Long, S3StreamMetadataImage> streamsMetadata,
            Map<Integer, NodeS3StreamSetObjectMetadataImage> nodeStreamSetObjectMetadata) {
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
        streamsMetadata.values().forEach(image -> image.write(writer, options));
        nodeStreamSetObjectMetadata.values().forEach(image -> image.write(writer, options));
    }

    public InRangeObjects getObjects(long streamId, long startOffset, long endOffset, int limit) {
        S3StreamMetadataImage streamMetadata = streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            return InRangeObjects.INVALID;
        }
        if (startOffset < streamMetadata.startOffset()) {
            // start offset mismatch
            return InRangeObjects.INVALID;
        }
        List<S3ObjectMetadata> objects = new ArrayList<>();
        long realEndOffset = startOffset;
        List<RangeSearcher> rangeSearchers = rangeSearchers(streamId, startOffset, endOffset);
        // TODO: if one stream object in multiple ranges, we may get duplicate objects
        for (RangeSearcher rangeSearcher : rangeSearchers) {
            InRangeObjects inRangeObjects = rangeSearcher.getObjects(limit);
            if (inRangeObjects == InRangeObjects.INVALID) {
                break;
            }
            if (inRangeObjects.objects().isEmpty()) {
                throw new IllegalStateException("[BUG] expect getObjects return objects from " + rangeSearcher);
            }
            realEndOffset = inRangeObjects.endOffset();
            objects.addAll(inRangeObjects.objects());
            limit -= inRangeObjects.objects().size();
            if (limit <= 0 || realEndOffset >= endOffset) {
                break;
            }
        }
        return new InRangeObjects(streamId, startOffset, realEndOffset, objects);
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
        Map<Long, S3StreamObject> streamObjectsMetadata = stream.getStreamObjects();
        if (streamObjectsMetadata == null || streamObjectsMetadata.isEmpty()) {
            return Collections.emptyList();
        }
        return streamObjectsMetadata.values().stream().filter(obj -> {
            long objectStartOffset = obj.streamOffsetRange().getStartOffset();
            long objectEndOffset = obj.streamOffsetRange().getEndOffset();
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

    private List<RangeSearcher> rangeSearchers(long streamId, long startOffset, long endOffset) {
        S3StreamMetadataImage streamMetadata = streamsMetadata.get(streamId);
        List<RangeSearcher> rangeSearchers = new ArrayList<>();
        // TODO: refactor to make ranges in order
        List<RangeMetadata> ranges = streamMetadata.getRanges().values().stream().sorted(Comparator.comparingInt(RangeMetadata::rangeIndex)).collect(Collectors.toList());
        for (RangeMetadata range : ranges) {
            if (range.endOffset() <= startOffset) {
                continue;
            }
            if (range.startOffset() >= endOffset) {
                break;
            }
            long searchEndOffset = Math.min(range.endOffset(), endOffset);
            long searchStartOffset = Math.max(range.startOffset(), startOffset);
            if (searchStartOffset == searchEndOffset) {
                continue;
            }
            rangeSearchers.add(new RangeSearcher(searchStartOffset, searchEndOffset, streamId, range.nodeId()));
        }
        return rangeSearchers;
    }

    class RangeSearcher {

        private final long startOffset;
        private final long endOffset;
        private final long streamId;
        private final int nodeId;

        public RangeSearcher(long startOffset, long endOffset, long streamId, int nodeId) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.streamId = streamId;
            this.nodeId = nodeId;
        }

        private Queue<S3ObjectMetadataWrapper> rangeOfStreamSetObjects() {
            NodeS3StreamSetObjectMetadataImage streamSetObjectImage = nodeStreamSetObjectMetadata.get(nodeId);
            List<S3StreamSetObject> streamSetObjects = streamSetObjectImage.orderList();
            Queue<S3ObjectMetadataWrapper> s3ObjectMetadataList = new LinkedList<>();
            for (S3StreamSetObject obj : streamSetObjects) {
                // TODO: cache the stream offset ranges to accelerate the search
                // TODO: cache the last search index, to accelerate the search
                List<StreamOffsetRange> ranges = obj.offsetRangeList();
                int index = new StreamOffsetRanges(ranges).search(streamId);
                if (index < 0) {
                    continue;
                }
                StreamOffsetRange range = ranges.get(index);
                if (range.getStartOffset() >= endOffset || range.getEndOffset() < startOffset) {
                    continue;
                }
                S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(
                        obj.objectId(), obj.objectType(), ranges, obj.dataTimeInMs(),
                        obj.orderId());
                s3ObjectMetadataList.add(new S3ObjectMetadataWrapper(s3ObjectMetadata, range.getStartOffset(), range.getEndOffset()));
                if (range.getEndOffset() >= endOffset) {
                    break;
                }
            }
            return s3ObjectMetadataList;
        }

        private Queue<S3ObjectMetadataWrapper> rangeOfStreamObjects() {
            S3StreamMetadataImage stream = streamsMetadata.get(streamId);
            Map<Long, S3StreamObject> streamObjectsMetadata = stream.getStreamObjects();
            // TODO: refactor to make stream objects in order
            if (streamObjectsMetadata != null && !streamObjectsMetadata.isEmpty()) {
                return streamObjectsMetadata.values().stream().filter(obj -> {
                    long objectStartOffset = obj.streamOffsetRange().getStartOffset();
                    long objectEndOffset = obj.streamOffsetRange().getEndOffset();
                    return objectStartOffset < endOffset && objectEndOffset > startOffset;
                }).sorted(Comparator.comparing(S3StreamObject::streamOffsetRange)).map(obj -> {
                    long startOffset = obj.streamOffsetRange().getStartOffset();
                    long endOffset = obj.streamOffsetRange().getEndOffset();
                    S3ObjectMetadata s3ObjectMetadata = new S3ObjectMetadata(
                            obj.objectId(), obj.objectType(), List.of(obj.streamOffsetRange()), obj.dataTimeInMs());
                    return new S3ObjectMetadataWrapper(s3ObjectMetadata, startOffset, endOffset);
                }).collect(Collectors.toCollection(LinkedList::new));
            }
            return new LinkedList<>();
        }

        public InRangeObjects getObjects(int limit) {
            if (limit <= 0) {
                return InRangeObjects.INVALID;
            }
            if (!nodeStreamSetObjectMetadata.containsKey(nodeId) || !streamsMetadata.containsKey(streamId)) {
                return InRangeObjects.INVALID;
            }

            Queue<S3ObjectMetadataWrapper> streamObjects = rangeOfStreamObjects();
            Queue<S3ObjectMetadataWrapper> streamSetObjects = rangeOfStreamSetObjects();
            List<S3ObjectMetadata> inRangeObjects = new ArrayList<>();
            long nextStartOffset = startOffset;

            while (limit > 0
                    && nextStartOffset < endOffset
                    && (!streamObjects.isEmpty() || !streamSetObjects.isEmpty())) {
                S3ObjectMetadataWrapper streamRange = null;
                if (streamSetObjects.isEmpty() || (!streamObjects.isEmpty() && streamObjects.peek().startOffset() < streamSetObjects.peek().startOffset())) {
                    streamRange = streamObjects.poll();
                } else {
                    streamRange = streamSetObjects.poll();
                }
                long objectStartOffset = streamRange.startOffset();
                long objectEndOffset = streamRange.endOffset();
                if (objectStartOffset > nextStartOffset) {
                    break;
                }
                if (objectEndOffset <= nextStartOffset) {
                    continue;
                }
                inRangeObjects.add(streamRange.metadata);
                limit--;
                nextStartOffset = objectEndOffset;
            }
            return new InRangeObjects(streamId, startOffset, nextStartOffset, inRangeObjects);
        }

        @Override
        public String toString() {
            return "RangeSearcher{" +
                    "startOffset=" + startOffset +
                    ", endOffset=" + endOffset +
                    ", streamId=" + streamId +
                    ", nodeId=" + nodeId +
                    '}';
        }
    }

    static class S3ObjectMetadataWrapper {

        private final S3ObjectMetadata metadata;
        private final long startOffset;
        private final long endOffset;

        public S3ObjectMetadataWrapper(S3ObjectMetadata metadata, long startOffset, long endOffset) {
            this.metadata = metadata;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public S3ObjectMetadata metadata() {
            return metadata;
        }

        public long startOffset() {
            return startOffset;
        }

        public long endOffset() {
            return endOffset;
        }
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

    public Map<Integer, NodeS3StreamSetObjectMetadataImage> nodeWALMetadata() {
        return nodeStreamSetObjectMetadata;
    }

    public Map<Long, S3StreamMetadataImage> streamsMetadata() {
        return streamsMetadata;
    }

    public StreamOffsetRange offsetRange(long streamId) {
        S3StreamMetadataImage streamMetadata = streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            return StreamOffsetRange.INVALID;
        }
        return streamMetadata.offsetRange();
    }


    public long nextAssignedStreamId() {
        return nextAssignedStreamId;
    }

    @Override
    public String toString() {
        return "S3StreamsMetadataImage{" +
                "nextAssignedStreamId=" + nextAssignedStreamId +
                ", streamsMetadata=" + streamsMetadata.entrySet().stream().
                map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
                ", nodeWALMetadata=" + nodeStreamSetObjectMetadata.entrySet().stream().
                map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
                '}';
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
                    return range.getStreamId() < o;
                }

                @Override
                public boolean isGreaterThan(Long o) {
                    return range.getStreamId() > o;
                }
            };
        }

    }
}
