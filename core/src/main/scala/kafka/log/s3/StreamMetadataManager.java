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
        // register listener
        this.broker.metadataListener().registerStreamMetadataListener(this.catchUpMetadataListener);
    }

    public synchronized void catchupTo(long objectId) {
        // delete all wal objects which are <= objectId
        this.inflightWalObjects.trim(objectId);
    }

    @SuppressWarnings("all")
    public synchronized List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        List<S3ObjectMetadata> objects = new ArrayList<>();
        if (startOffset >= endOffset) {
            return objects;
        }
        OffsetRange walRange = this.inflightWalObjects.getWalRange(streamId);
        if (walRange == null || endOffset <= walRange.startOffset()) {
            // only search in cache
            InRangeObjects cachedInRangeObjects = this.metadataCache.getObjects(streamId, startOffset, endOffset, limit);
            if (cachedInRangeObjects != null) {
                objects.addAll(cachedInRangeObjects.objects());
            }
            return objects;
        }
        if (startOffset >= walRange.startOffset()) {
            // only search in inflight wal
            InRangeObjects inflightInRangeObjects = this.inflightWalObjects.getObjects(streamId, startOffset, endOffset, limit);
            if (inflightInRangeObjects != null) {
                objects.addAll(inflightInRangeObjects.objects());
            }
            return objects;
        }
        long cachedEndOffset = walRange.startOffset();
        InRangeObjects cachedInRangeObjects = this.metadataCache.getObjects(streamId, startOffset, cachedEndOffset, limit);
        if (cachedInRangeObjects == null || cachedInRangeObjects == InRangeObjects.INVALID) {
            return objects;
        }
        objects.addAll(cachedInRangeObjects.objects());
        if (objects.size() >= limit) {
            return objects;
        }
        InRangeObjects inflightinRangeObjects = this.inflightWalObjects.getObjects(streamId, cachedEndOffset, endOffset, limit - objects.size());
        if (inflightinRangeObjects != null) {
            objects.addAll(inflightinRangeObjects.objects());
        }
        objects.forEach(obj -> {
            S3Object metadata = metadataCache.getObjectMetadata(obj.getObjectId());
            if (metadata == null) {
                LOGGER.error("object: {} metadata not exist", obj.getObjectId());
                throw new RuntimeException("object: " + obj.getObjectId() + " metadata not exist");
            }
            obj.setObjectSize(metadata.getObjectSize());
        });
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

    static class InflightWalObjects {

        private final List<S3WALObject> objects;
        private final Map<Long/*streamId*/, OffsetRange/*inflight offset range*/> streamOffsets;

        public InflightWalObjects() {
            this.objects = new LinkedList<>();
            this.streamOffsets = new HashMap<>();
        }

        public void append(S3WALObject object) {
            objects.add(object);
            object.streamsIndex().forEach((stream, indexes) -> {
                // wal object only contains one index for each stream
                streamOffsets.putIfAbsent(stream, new OffsetRange(indexes.get(0).getStartOffset(), indexes.get(indexes.size() - 1).getEndOffset()));
                streamOffsets.get(stream).setEndOffset(indexes.get(indexes.size() - 1).getEndOffset());
            });
        }

        public void trim(long objectId) {
            // TODO: speed up by binary search
            int clearEndIndex = objects.size();
            for (int i = 0; i < objects.size(); i++) {
                S3WALObject wal = objects.get(i);
                if (wal.objectId() > objectId) {
                    clearEndIndex = i;
                    break;
                }
                wal.streamsIndex().forEach((stream, indexes) -> {
                    streamOffsets.get(stream).setStartOffset(indexes.get(indexes.size() - 1).getEndOffset());
                });
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
            for (S3WALObject object : objects) {
                if (limit <= 0) {
                    break;
                }
                if (nextStartOffset >= endOffset) {
                    break;
                }
                List<S3ObjectStreamIndex> indexes = object.streamsIndex().get(streamId);
                if (indexes == null || indexes.size() != 1) {
                    LOGGER.error("invalid wal object: {}", object);
                    continue;
                }
                long objStartOffset = indexes.get(0).getStartOffset();
                long objEndOffset = indexes.get(0).getEndOffset();
                if (objStartOffset > startOffset) {
                    break;
                }
                if (objEndOffset <= startOffset) {
                    continue;
                }
                limit--;
                inRangeObjects.add(new S3ObjectMetadata(object.objectId(), object.objectType()));
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
                return;
            }
            S3WALObject wal = walMetadataImage.getWalObjects().get(walMetadataImage.getWalObjects().size() - 1);
            if (wal == null) {
                return;
            }
            catchupTo(wal.objectId());
        }
    }

}
