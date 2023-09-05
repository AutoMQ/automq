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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3ObjectType;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public final class S3StreamsMetadataImage {

    public static final S3StreamsMetadataImage EMPTY =
        new S3StreamsMetadataImage(-1, Collections.emptyMap(), Collections.emptyMap());

    private long nextAssignedStreamId;

    private final Map<Long/*streamId*/, S3StreamMetadataImage> streamsMetadata;

    private final Map<Integer/*brokerId*/, BrokerS3WALMetadataImage> brokerWALMetadata;

    public S3StreamsMetadataImage(
        long assignedStreamId,
        Map<Long, S3StreamMetadataImage> streamsMetadata,
        Map<Integer, BrokerS3WALMetadataImage> brokerWALMetadata) {
        this.nextAssignedStreamId = assignedStreamId + 1;
        this.streamsMetadata = streamsMetadata;
        this.brokerWALMetadata = brokerWALMetadata;
    }


    boolean isEmpty() {
        return this.brokerWALMetadata.isEmpty() && this.streamsMetadata.isEmpty();
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(
            new ApiMessageAndVersion(
                new AssignedStreamIdRecord().setAssignedStreamId(nextAssignedStreamId - 1), (short) 0));
        streamsMetadata.values().forEach(image -> image.write(writer, options));
        brokerWALMetadata.values().forEach(image -> image.write(writer, options));
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
        for (RangeSearcher rangeSearcher : rangeSearchers) {
            InRangeObjects inRangeObjects = rangeSearcher.getObjects(limit);
            if (inRangeObjects == InRangeObjects.INVALID) {
                break;
            }
            realEndOffset = inRangeObjects.endOffset();
            objects.addAll(inRangeObjects.objects());
            limit -= inRangeObjects.objects().size();
            if (limit <= 0) {
                break;
            }
        }
        return new InRangeObjects(streamId, startOffset, realEndOffset, objects);
    }

    private List<RangeSearcher> rangeSearchers(long streamId, long startOffset, long endOffset) {
        S3StreamMetadataImage streamMetadata = streamsMetadata.get(streamId);
        List<RangeSearcher> rangeSearchers = new ArrayList<>();
        // TODO: refactor to make ranges in order
        List<RangeMetadata> ranges = streamMetadata.getRanges().values().stream().sorted(new Comparator<RangeMetadata>() {
            @Override
            public int compare(RangeMetadata o1, RangeMetadata o2) {
                return o1.rangeIndex() - o2.rangeIndex();
            }
        }).collect(Collectors.toList());
        for (RangeMetadata range : ranges) {
            if (range.endOffset() <= startOffset) {
                continue;
            }
            if (range.startOffset() >= endOffset) {
                break;
            }
            long searchEndOffset = Math.min(range.endOffset(), endOffset);
            long searchStartOffset = Math.max(range.startOffset(), startOffset);
            rangeSearchers.add(new RangeSearcher(searchStartOffset, searchEndOffset, streamId, range.brokerId()));
        }
        return rangeSearchers;
    }

    class RangeSearcher {

        private final long startOffset;
        private final long endOffset;
        private final long streamId;
        private final int brokerId;

        public RangeSearcher(long startOffset, long endOffset, long streamId, int brokerId) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.streamId = streamId;
            this.brokerId = brokerId;
        }

        @SuppressWarnings("all")
        public InRangeObjects getObjects(int limit) {
            if (limit <= 0) {
                return InRangeObjects.INVALID;
            }
            BrokerS3WALMetadataImage wal = brokerWALMetadata.get(brokerId);
            if (wal == null) {
                return InRangeObjects.INVALID;
            }
            List<ObjectStreamRange> walObjects = wal.getWalObjects().list().stream()
                .filter(obj -> obj.streamsIndex().containsKey(streamId) && obj.streamsIndex().get(streamId).size() != 0)
                .flatMap(obj -> {
                    List<S3ObjectStreamIndex> indexes = obj.streamsIndex().get(streamId);
                    // TODO: pre filter useless objects
                    return indexes.stream().filter(index -> {
                        long objectStartOffset = index.getStartOffset();
                        long objectEndOffset = index.getEndOffset();
                        return objectStartOffset < endOffset && objectEndOffset > startOffset;
                    }).map(index -> {
                        long startOffset = index.getStartOffset();
                        long endOffset = index.getEndOffset();
                        return new ObjectStreamRange(obj.objectId(), obj.objectType(), startOffset, endOffset);
                    });
                }).collect(Collectors.toList());
            S3StreamMetadataImage stream = streamsMetadata.get(streamId);
            if (stream == null) {
                return InRangeObjects.INVALID;
            }
            List<ObjectStreamRange> streamObjects = new ArrayList<>();
            Map<Long, S3StreamObject> streamObjectsMetadata = stream.getStreamObjects();
            // TODO: refactor to make stream objects in order
            if (streamObjectsMetadata != null && !streamObjectsMetadata.isEmpty()) {
                List<S3StreamObject> streamObjs = streamObjectsMetadata.values().stream().filter(obj -> {
                    long objectStartOffset = obj.streamIndex().getStartOffset();
                    long objectEndOffset = obj.streamIndex().getEndOffset();
                    return objectStartOffset < endOffset && objectEndOffset > startOffset;
                }).sorted(new Comparator<S3StreamObject>() {
                    @Override
                    public int compare(S3StreamObject o1, S3StreamObject o2) {
                        return o1.objectId() > o2.objectId() ? 1 : -1;
                    }
                }).collect(Collectors.toList());
                streamObjects.addAll(
                    streamObjs.stream().map(
                        obj -> {
                            long startOffset = obj.streamIndex().getStartOffset();
                            long endOffset = obj.streamIndex().getEndOffset();
                            return new ObjectStreamRange(obj.objectId(), obj.objectType(), startOffset, endOffset);
                        }).collect(Collectors.toList()));
            }
            List<S3ObjectMetadata> inRangeObjects = new ArrayList<>();
            int walIndex = 0;
            int streamIndex = 0;
            long nextStartOffset = startOffset;
            while (limit > 0
                && nextStartOffset < endOffset
                && (walIndex < walObjects.size() || streamIndex < streamObjects.size())) {

                if (walIndex >= walObjects.size()) {
                    // only stream objects left
                    ObjectStreamRange streamRange = streamObjects.get(streamIndex++);
                    long objectStartOffset = streamRange.startOffset();
                    long objectEndOffset = streamRange.endOffset();
                    if (objectStartOffset > nextStartOffset) {
                        break;
                    }
                    if (objectEndOffset <= nextStartOffset) {
                        continue;
                    }
                    inRangeObjects.add(streamRange.toS3ObjectMetadata());
                    limit--;
                    nextStartOffset = objectEndOffset;
                    continue;
                }

                if (streamIndex >= streamObjects.size()) {
                    // only wal objects left
                    ObjectStreamRange walRange = walObjects.get(walIndex++);
                    long objectStartOffset = walRange.startOffset();
                    long objectEndOffset = walRange.endOffset();
                    if (objectStartOffset > nextStartOffset) {
                        break;
                    }
                    if (objectEndOffset <= nextStartOffset) {
                        continue;
                    }
                    inRangeObjects.add(walRange.toS3ObjectMetadata());
                    limit--;
                    nextStartOffset = objectEndOffset;
                    continue;
                }

                ObjectStreamRange walRange = walObjects.get(walIndex);
                ObjectStreamRange streamRange = streamObjects.get(streamIndex);
                long walObjectStartOffset = walRange.startOffset();
                long walObjectEndOffset = walRange.endOffset();
                long streamObjectStartOffset = streamRange.startOffset();
                long streamObjectEndOffset = streamRange.endOffset();
                if (walObjectStartOffset > nextStartOffset && streamObjectStartOffset > nextStartOffset) {
                    // both start offset are greater than nextStartOffset
                    break;
                }
                if (walObjectStartOffset <= nextStartOffset && walObjectEndOffset > nextStartOffset) {
                    // wal object contains nextStartOffset
                    inRangeObjects.add(walRange.toS3ObjectMetadata());
                    limit--;
                    nextStartOffset = walObjectEndOffset;
                    walIndex++;
                    continue;
                }
                if (streamObjectStartOffset <= nextStartOffset && streamObjectEndOffset > nextStartOffset) {
                    // stream object contains nextStartOffset
                    inRangeObjects.add(streamRange.toS3ObjectMetadata());
                    limit--;
                    nextStartOffset = streamObjectEndOffset;
                    streamIndex++;
                }
            }
            return new InRangeObjects(streamId, startOffset, nextStartOffset, inRangeObjects);
        }

    }

    static class ObjectStreamRange {

        private final long objectId;
        private final S3ObjectType objectType;
        private final long startOffset;
        private final long endOffset;

        public ObjectStreamRange(long objectId, S3ObjectType objectType, long startOffset, long endOffset) {
            this.objectId = objectId;
            this.objectType = objectType;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public long objectId() {
            return objectId;
        }

        public long startOffset() {
            return startOffset;
        }

        public long endOffset() {
            return endOffset;
        }

        public S3ObjectMetadata toS3ObjectMetadata() {
            // TODO: fill object size later
            return new S3ObjectMetadata(objectId, -1, objectType);
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
            && this.brokerWALMetadata.equals(other.brokerWALMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedStreamId, streamsMetadata, brokerWALMetadata);
    }

    public Map<Integer, BrokerS3WALMetadataImage> brokerWALMetadata() {
        return brokerWALMetadata;
    }

    public Map<Long, S3StreamMetadataImage> streamsMetadata() {
        return streamsMetadata;
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
            ", brokerWALMetadata=" + brokerWALMetadata.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            '}';
    }
}
