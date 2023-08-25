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

package kafka.log.s3.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.log.s3.model.RangeMetadata;
import kafka.log.s3.model.StreamMetadata;
import kafka.log.s3.objects.CommitCompactObjectRequest;
import kafka.log.s3.objects.CommitStreamObjectRequest;
import kafka.log.s3.objects.CommitWalObjectRequest;
import kafka.log.s3.objects.CommitWalObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.OpenStreamMetadata;
import kafka.log.s3.objects.S3ObjectMetadata;
import kafka.log.s3.streams.StreamManager;
import kafka.log.s3.utils.ObjectUtils;
import org.apache.kafka.common.errors.s3.StreamFencedException;
import org.apache.kafka.common.errors.s3.StreamNotExistException;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMetadataManager implements StreamManager, ObjectManager {

    private static final int MOCK_BROKER_ID = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryMetadataManager.class);
    private final EventDriver eventDriver;
    private final Map<Long/*objectId*/, S3Object> objectsMetadata;

    private volatile long nextAssignedObjectId = 0;

    private final Map<Long, StreamMetadata> streamsMetadata;

    private final Map<Integer, MemoryBrokerWALMetadata> brokerWALMetadata;

    private static class MemoryBrokerWALMetadata {

        private final int brokerId;
        private final List<S3WALObject> walObjects;

        public MemoryBrokerWALMetadata(int brokerId) {
            this.brokerId = brokerId;
            this.walObjects = new ArrayList<>();
        }
    }

    private volatile long nextAssignedStreamId = 0;

    public MemoryMetadataManager() {
        this.eventDriver = new EventDriver();
        this.objectsMetadata = new HashMap<>();
        this.streamsMetadata = new HashMap<>();
        this.brokerWALMetadata = new HashMap<>();
    }

    public <T> CompletableFuture<T> submitEvent(Supplier<T> eventHandler) {
        CompletableFuture<T> cb = new CompletableFuture<>();
        MemoryMetadataEvent event = new MemoryMetadataEvent(cb, eventHandler);
        if (!eventDriver.submit(event)) {
            throw new RuntimeException("Offer event failed");
        }
        return cb;
    }

    public void shutdown() {
        this.eventDriver.stop();
    }

    public void start() {
        this.eventDriver.start();
    }

    @Override
    public CompletableFuture<Long> prepareObject(int count, long ttl) {
        return this.submitEvent(() -> {
            long objectRangeStart = this.nextAssignedObjectId;
            for (int i = 0; i < count; i++) {
                long objectId = this.nextAssignedObjectId++;
                S3Object object = prepareObject(objectId, ttl);
                this.objectsMetadata.put(objectId, object);
            }
            return objectRangeStart;
        });
    }

    @Override
    public CompletableFuture<CommitWalObjectResponse> commitWalObject(CommitWalObjectRequest request) {
        return this.submitEvent(() -> {
            CommitWalObjectResponse resp = new CommitWalObjectResponse();
            List<Long> failedStreamIds = new ArrayList<>();
            resp.setFailedStreamIds(failedStreamIds);
            long objectId = request.getObjectId();
            long objectSize = request.getObjectSize();
            List<ObjectStreamRange> streamRanges = request.getStreamRanges();
            S3Object object = this.objectsMetadata.get(objectId);
            if (object == null) {
                throw new RuntimeException("Object " + objectId + " does not exist");
            }
            if (object.getS3ObjectState() != S3ObjectState.PREPARED) {
                throw new RuntimeException("Object " + objectId + " is not in prepared state");
            }
            // verify the stream
            streamRanges.stream().filter(range -> !verifyWalStreamRanges(range)).mapToLong(ObjectStreamRange::getStreamId)
                .forEach(failedStreamIds::add);
            if (!failedStreamIds.isEmpty()) {
                return resp;
            }
            // commit object
            this.objectsMetadata.put(objectId, new S3Object(
                objectId, objectSize, object.getObjectKey(),
                object.getPreparedTimeInMs(), object.getExpiredTimeInMs(), System.currentTimeMillis(), -1,
                S3ObjectState.COMMITTED)
            );
            // build metadata
            MemoryBrokerWALMetadata walMetadata = this.brokerWALMetadata.computeIfAbsent(MOCK_BROKER_ID,
                k -> new MemoryBrokerWALMetadata(k));
            Map<Long, S3ObjectStreamIndex> index = new HashMap<>();
            streamRanges.stream().forEach(range -> {
                long streamId = range.getStreamId();
                long startOffset = range.getStartOffset();
                long endOffset = range.getEndOffset();
                index.put(streamId, new S3ObjectStreamIndex(streamId, startOffset, endOffset));
                // update range endOffset
                StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
                streamMetadata.getRanges().get(streamMetadata.getRanges().size() - 1).setEndOffset(endOffset);
            });
            S3WALObject walObject = new S3WALObject(objectId, MOCK_BROKER_ID, index);
            walMetadata.walObjects.add(walObject);
            return resp;
        });
    }

    private boolean verifyWalStreamRanges(ObjectStreamRange range) {
        long streamId = range.getStreamId();
        long epoch = range.getEpoch();
        // verify
        StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            return false;
        }
        // compare epoch
        if (streamMetadata.getEpoch() > epoch) {
            return false;
        }
        if (streamMetadata.getEpoch() < epoch) {
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> commitMinorCompactObject(CommitCompactObjectRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitMajorCompactObject(CommitCompactObjectRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        return null;
    }

    @Override
    public List<S3ObjectMetadata> getServerObjects() {
        CompletableFuture<List<S3ObjectMetadata>> future = this.submitEvent(() -> {
            return this.brokerWALMetadata.get(MOCK_BROKER_ID).walObjects.stream().map(obj -> {
                S3Object s3Object = this.objectsMetadata.get(obj.objectId());
                return new S3ObjectMetadata(obj.objectId(), s3Object.getObjectSize(), obj.objectType());
            }).collect(Collectors.toList());
        });
        try {
            return future.get();
        } catch (Exception e) {
            LOGGER.error("Error in getServerObjects", e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        // TODO: support search not only in wal objects
        CompletableFuture<List<S3ObjectMetadata>> future = this.submitEvent(() -> {
            int need = limit;
            List<S3ObjectMetadata> objs = new ArrayList<>();
            StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            if (endOffset <= streamMetadata.getStartOffset()) {
                return objs;
            }
            List<RangeMetadata> ranges = streamMetadata.getRanges();
            for (RangeMetadata range : ranges) {
                if (endOffset < range.getStartOffset() || need <= 0) {
                    break;
                }
                if (startOffset >= range.getEndOffset()) {
                    continue;
                }
                // find range, get wal objects
                int brokerId = range.getBrokerId();
                MemoryBrokerWALMetadata walMetadata = this.brokerWALMetadata.get(brokerId);
                for (S3WALObject walObject : walMetadata.walObjects) {
                    if (need <= 0) {
                        break;
                    }
                    // TODO: speed up query
                    if (!walObject.intersect(streamId, startOffset, endOffset)) {
                        continue;
                    }
                    // find stream index, get object
                    S3Object object = this.objectsMetadata.get(walObject.objectId());
                    S3ObjectMetadata obj = new S3ObjectMetadata(walObject.objectId(), object.getObjectSize(), walObject.objectType());
                    objs.add(obj);
                    need--;
                }
            }
            return objs;
        });
        try {
            return future.get();
        } catch (Exception e) {
            LOGGER.error("Error in getObjects", e);
            return Collections.emptyList();
        }
    }

    @Override
    public CompletableFuture<Long> createStream() {
        return this.submitEvent(() -> {
            long streamId = this.nextAssignedStreamId++;
            this.streamsMetadata.put(streamId,
                new StreamMetadata(streamId, 0, -1, 0, new ArrayList<>()));
            return streamId;
        });
    }

    @Override
    public CompletableFuture<OpenStreamMetadata> openStream(long streamId, long epoch) {
        return this.submitEvent(() -> {
            // verify stream exist
            if (!this.streamsMetadata.containsKey(streamId)) {
                throw new StreamNotExistException("Stream " + streamId + " does not exist");
            }
            // verify epoch match
            StreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            if (streamMetadata.getEpoch() > epoch) {
                throw new StreamFencedException("Stream " + streamId + " is fenced");
            }
            if (streamMetadata.getEpoch() == epoch) {
                // get active range
                long endOffset = streamMetadata.getRanges().get(streamMetadata.getRanges().size() - 1).getEndOffset();
                return new OpenStreamMetadata(streamId, epoch, streamMetadata.getStartOffset(), endOffset);
            }
            // create new range
            long newEpoch = epoch;
            int newRangeIndex = streamMetadata.getRangeIndex() + 1;
            long startOffset = 0;
            if (newRangeIndex > 0) {
                startOffset = streamMetadata.getRanges().get(streamMetadata.getRanges().size() - 1).getEndOffset();
            }
            RangeMetadata rangeMetadata = new RangeMetadata(newRangeIndex, startOffset, startOffset, MOCK_BROKER_ID);
            streamMetadata.getRanges().add(rangeMetadata);
            // update epoch and rangeIndex
            streamMetadata.setRangeIndex(newRangeIndex);
            streamMetadata.setEpoch(newEpoch);
            return new OpenStreamMetadata(streamId, newEpoch, startOffset, startOffset);
        });
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        return null;
    }

    private S3Object prepareObject(long objectId, long ttl) {
        long preparedTs = System.currentTimeMillis();
        String objectKey = ObjectUtils.genKey(0, "todocluster", objectId);
        return new S3Object(
            objectId, -1, objectKey,
            preparedTs, preparedTs + ttl, -1, -1,
            S3ObjectState.PREPARED);
    }


    static class EventDriver implements Runnable {

        private final ExecutorService service;

        private final BlockingQueue<MemoryMetadataEvent> eventQueue;

        public EventDriver() {
            this.service = Executors.newSingleThreadExecutor();
            this.eventQueue = new LinkedBlockingQueue<>(1024);
        }

        public void start() {
            this.service.submit(this::run);
        }

        public void stop() {
            this.service.shutdownNow();
        }

        public boolean submit(MemoryMetadataEvent event) {
            return eventQueue.offer(event);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    run0();
                } catch (Exception e) {
                    LOGGER.error("Error in memory manager event driver", e);
                }
            }
        }

        private void run0() throws InterruptedException {
            MemoryMetadataEvent event = eventQueue.poll(5, TimeUnit.SECONDS);
            if (event != null) {
                event.done();
            }
        }

    }

    static class MemoryMetadataEvent<T> {

        CompletableFuture<T> cb;
        Supplier<T> eventHandler;

        public MemoryMetadataEvent(CompletableFuture<T> cb, Supplier<T> eventHandler) {
            this.cb = cb;
            this.eventHandler = eventHandler;
        }

        public void done() {
            cb.complete(eventHandler.get());
        }
    }
}
