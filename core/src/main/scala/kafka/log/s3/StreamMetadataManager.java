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

package kafka.log.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.image.BrokerS3WALMetadataImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamMetadataManager {

    // TODO: optimize by more suitable concurrent protection
    // TODO: use order id instead of object id
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    private final KafkaConfig config;
    private final BrokerServer broker;
    private final InflightWalObjects inflightWalObjects;
    private final MetadataCache metadataCache;
    private final CatchUpMetadataListener catchUpMetadataListener;

    public StreamMetadataManager(BrokerServer broker, KafkaConfig config) {
        this.config = config;
        this.broker = broker;
        this.inflightWalObjects = new InflightWalObjects();
        this.metadataCache = broker.metadataCache();
        this.catchUpMetadataListener = new CatchUpMetadataListener();
        this.broker.metadataListener().registerStreamMetadataListener(this.catchUpMetadataListener);
    }

    public synchronized void append(InflightWalObject object) {
        this.inflightWalObjects.append(object);
    }

    public synchronized void catchupTo(long objectId) {
        // delete all wal objects which are <= objectId
        this.inflightWalObjects.trim(objectId);
    }

    @SuppressWarnings("all")
    public synchronized List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        List<S3ObjectMetadata> objects = new ArrayList<>();
        if (startOffset >= endOffset) {
            LOGGER.warn("[GetObjects]: invalid offset range, stream: {}, startOffset: {}, endOffset: {}", streamId, startOffset, endOffset);
            return objects;
        }
        OffsetRange walRange = this.inflightWalObjects.getWalRange(streamId);
        if (walRange == null || endOffset <= walRange.startOffset()) {
            // only search in cache
            InRangeObjects cachedInRangeObjects = this.metadataCache.getObjects(streamId, startOffset, endOffset, limit);
            if (cachedInRangeObjects == null) {
                LOGGER.warn(
                    "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                    streamId, startOffset, endOffset, limit);
                return objects;
            }
            objects.addAll(cachedInRangeObjects.objects());
            objects.forEach(obj -> {
                S3Object metadata = metadataCache.getObjectMetadata(obj.getObjectId());
                if (metadata == null) {
                    LOGGER.error("object: {} metadata not exist", obj.getObjectId());
                    throw new RuntimeException("object: " + obj.getObjectId() + " metadata not exist");
                }
                obj.setObjectSize(metadata.getObjectSize());
            });
            LOGGER.info(
                "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and return all from metadataCache: startOffset: {}, endOffset: {}, object count: {}",
                streamId, startOffset, endOffset, limit,
                cachedInRangeObjects.startOffset(), cachedInRangeObjects.endOffset(), objects.size());
            return objects;
        }
        if (startOffset >= walRange.startOffset()) {
            // only search in inflight wal
            InRangeObjects inflightInRangeObjects = this.inflightWalObjects.getObjects(streamId, startOffset, endOffset, limit);
            if (inflightInRangeObjects == null) {
                LOGGER.warn(
                    "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in inflightWalObjects failed with empty result",
                    streamId, startOffset, endOffset, limit);
                return objects;
            }
            objects.addAll(inflightInRangeObjects.objects());
            LOGGER.info(
                "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and return all from inflight: startOffset: {}, endOffset: {}, object count: {}",
                streamId, startOffset, endOffset, limit,
                inflightInRangeObjects.startOffset(), inflightInRangeObjects.endOffset(), objects.size());
            return objects;
        }
        long cachedEndOffset = walRange.startOffset();
        InRangeObjects cachedInRangeObjects = this.metadataCache.getObjects(streamId, startOffset, cachedEndOffset, limit);
        if (cachedInRangeObjects == null || cachedInRangeObjects == InRangeObjects.INVALID) {
            LOGGER.warn("[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                streamId, startOffset, endOffset, limit);
            return objects;
        }
        objects.addAll(cachedInRangeObjects.objects());
        objects.forEach(obj -> {
            S3Object metadata = metadataCache.getObjectMetadata(obj.getObjectId());
            if (metadata == null) {
                LOGGER.error("object: {} metadata not exist", obj.getObjectId());
                throw new RuntimeException("object: " + obj.getObjectId() + " metadata not exist");
            }
            obj.setObjectSize(metadata.getObjectSize());
        });
        if (objects.size() >= limit) {
            LOGGER.info(
                "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and return all from metadataCache: startOffset: {}, endOffset: {}, object count: {}",
                streamId, startOffset, endOffset, limit,
                cachedInRangeObjects.startOffset(), cachedInRangeObjects.endOffset(), objects.size());
            return objects;
        }
        InRangeObjects inflightinRangeObjects = this.inflightWalObjects.getObjects(streamId, cachedEndOffset, endOffset, limit - objects.size());
        if (inflightinRangeObjects == null || inflightinRangeObjects == InRangeObjects.INVALID) {
            LOGGER.warn(
                "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in inflightWalObjects failed with empty result",
                streamId, startOffset, endOffset, limit);
            return objects;
        }
        objects.addAll(inflightinRangeObjects.objects());
        LOGGER.info(
            "[GetObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and return all from metadataCache and inflight: startOffset: {}, endOffset: {}, object count: {}",
            streamId, startOffset, endOffset, limit,
            cachedInRangeObjects.startOffset(), inflightinRangeObjects.endOffset(), objects.size());
        return objects;
    }

    private static class OffsetRange {

        private long startOffset;
        private long endOffset;

        public OffsetRange(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public long startOffset() {
            return startOffset;
        }

        public long endOffset() {
            return endOffset;
        }

        public void setEndOffset(long endOffset) {
            this.endOffset = endOffset;
        }

        public void setStartOffset(long startOffset) {
            this.startOffset = startOffset;
        }
    }

    public static class InflightWalObject extends S3WALObject {

        private final long objectSize;

        public InflightWalObject(long objectId, int brokerId, Map<Long, List<S3ObjectStreamIndex>> streamsIndex, long orderId, long objectSize) {
            super(objectId, brokerId, streamsIndex, orderId);
            this.objectSize = objectSize;
        }

        public long startOffset(long streamId) {
            List<S3ObjectStreamIndex> indexes = streamsIndex().get(streamId);
            if (indexes == null || indexes.isEmpty()) {
                return S3StreamConstant.INVALID_OFFSET;
            }
            return indexes.get(0).getStartOffset();
        }

        public long endOffset(long streamId) {
            List<S3ObjectStreamIndex> indexes = streamsIndex().get(streamId);
            if (indexes == null || indexes.isEmpty()) {
                return S3StreamConstant.INVALID_OFFSET;
            }
            return indexes.get(indexes.size() - 1).getEndOffset();
        }

        public long objectSize() {
            return objectSize;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    static class InflightWalObjects {

        private final Logger log = LoggerFactory.getLogger(InflightWalObjects.class);

        private final List<InflightWalObject> objects;
        private final Map<Long/*streamId*/, OffsetRange/*inflight offset range*/> streamOffsets;
        private volatile long firstObjectId = S3StreamConstant.MAX_OBJECT_ID;

        public InflightWalObjects() {
            this.objects = new LinkedList<>();
            this.streamOffsets = new HashMap<>();
        }

        public void append(InflightWalObject object) {
            objects.add(object);
            if (objects.size() == 1) {
                firstObjectId = object.objectId();
            }
            log.info("[AppendInflight]: append wal object: {}", object.objectId());
            object.streamsIndex().forEach((stream, indexes) -> {
                // wal object only contains one index for each stream
                streamOffsets.putIfAbsent(stream, new OffsetRange(indexes.get(0).getStartOffset(), indexes.get(indexes.size() - 1).getEndOffset()));
                streamOffsets.get(stream).setEndOffset(indexes.get(indexes.size() - 1).getEndOffset());
            });
        }

        public void trim(long objectId) {
            log.info("[TrimInflight]: trim wal object <= {}", objectId);
            // TODO: speed up by binary search
            int clearEndIndex = objects.size();
            for (int i = 0; i < objects.size(); i++) {
                S3WALObject wal = objects.get(i);
                if (wal.objectId() > objectId) {
                    clearEndIndex = i;
                    firstObjectId = wal.objectId();
                    break;
                }
                wal.streamsIndex().forEach((stream, indexes) -> {
                    streamOffsets.get(stream).setStartOffset(indexes.get(indexes.size() - 1).getEndOffset());
                });
            }
            if (clearEndIndex == objects.size()) {
                firstObjectId = S3StreamConstant.MAX_OBJECT_ID;
            }
            objects.subList(0, clearEndIndex).clear();
        }

        public OffsetRange getWalRange(long streamId) {
            return streamOffsets.get(streamId);
        }

        @SuppressWarnings("all")
        public InRangeObjects getObjects(long streamId, long startOffset, long endOffset, int limit) {
            OffsetRange walRange = getWalRange(streamId);
            if (walRange == null) {
                return InRangeObjects.INVALID;
            }
            if (startOffset < walRange.startOffset()) {
                return InRangeObjects.INVALID;
            }
            if (endOffset > walRange.endOffset()) {
                endOffset = walRange.endOffset();
            }
            if (startOffset >= endOffset) {
                return InRangeObjects.INVALID;
            }
            List<S3ObjectMetadata> inRangeObjects = new LinkedList<>();
            long nextStartOffset = startOffset;
            for (InflightWalObject object : objects) {
                if (limit <= 0) {
                    break;
                }
                if (nextStartOffset >= endOffset) {
                    break;
                }
                long objStartOffset = object.startOffset(streamId);
                long objEndOffset = object.endOffset(streamId);
                if (objStartOffset == S3StreamConstant.INVALID_OFFSET || objEndOffset == S3StreamConstant.INVALID_OFFSET) {
                    continue;
                }
                if (objStartOffset > startOffset) {
                    break;
                }
                if (objEndOffset <= startOffset) {
                    continue;
                }
                limit--;
                inRangeObjects.add(new S3ObjectMetadata(object.objectId(), object.objectSize(), object.objectType()));
                nextStartOffset = objEndOffset;
            }
            return new InRangeObjects(streamId, startOffset, nextStartOffset, inRangeObjects);
        }
    }

    public interface StreamMetadataListener {

        void onChange(MetadataDelta delta, MetadataImage image);
    }

    class CatchUpMetadataListener implements StreamMetadataListener {

        @Override
        public void onChange(MetadataDelta delta, MetadataImage newImage) {
            BrokerS3WALMetadataImage walMetadataImage = newImage.streamsMetadata().brokerWALMetadata().get(config.brokerId());
            if (walMetadataImage == null) {
                LOGGER.warn("[CatchUpMetadataListener]: wal metadata image not exist");
                return;
            }
            S3WALObject wal = walMetadataImage.getWalObjects().get(walMetadataImage.getWalObjects().size() - 1);
            if (wal == null) {
                LOGGER.warn("[CatchUpMetadataListener]: wal object not exist");
                return;
            }
            if (wal.objectId() < inflightWalObjects.firstObjectId) {
                return;
            }
            catchupTo(wal.objectId());
        }
    }

}
