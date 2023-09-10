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

import kafka.log.s3.objects.CommitStreamObjectRequest;
import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.CommitWALObjectResponse;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.OpenStreamMetadata;
import kafka.log.s3.streams.StreamManager;
import org.apache.kafka.common.errors.s3.StreamFencedException;
import org.apache.kafka.common.errors.s3.StreamNotClosedException;
import org.apache.kafka.common.errors.s3.StreamNotExistException;
import org.apache.kafka.metadata.stream.ObjectUtils;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3WALObject;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.apache.kafka.metadata.stream.StreamState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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

public class MemoryMetadataManager implements StreamManager, ObjectManager {

    private static final int MOCK_BROKER_ID = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryMetadataManager.class);
    private final EventDriver eventDriver;
    private volatile long nextAssignedObjectId = 0;
    private final Map<Long/*objectId*/, S3Object> objectsMetadata;
    private volatile long nextAssignedStreamId = 0;
    private final Map<Long/*streamId*/, MemoryStreamMetadata> streamsMetadata;
    private final Map<Integer/*brokerId*/, MemoryBrokerWALMetadata> brokerWALMetadata;

    private static class MemoryStreamMetadata {

        private final long streamId;
        private StreamState state = StreamState.CLOSED;
        private long epoch = S3StreamConstant.INIT_EPOCH;
        private long startOffset = S3StreamConstant.INIT_START_OFFSET;
        private long endOffset = S3StreamConstant.INIT_END_OFFSET;
        private List<S3StreamObject> streamObjects;

        public MemoryStreamMetadata(long streamId) {
            this.streamId = streamId;
            this.state = StreamState.CLOSED;
        }

        public void addStreamObject(S3StreamObject object) {
            if (streamObjects == null) {
                streamObjects = new LinkedList<>();
            }
            streamObjects.add(object);
        }

        public StreamState state() {
            return state;
        }

        public void setState(StreamState state) {
            this.state = state;
        }
    }

    private static class MemoryBrokerWALMetadata {

        private final int brokerId;
        private final List<S3WALObject> walObjects;

        public MemoryBrokerWALMetadata(int brokerId) {
            this.brokerId = brokerId;
            this.walObjects = new ArrayList<>();
        }
    }

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
    public CompletableFuture<CommitWALObjectResponse> commitWALObject(CommitWALObjectRequest request) {
        return this.submitEvent(() -> {
            CommitWALObjectResponse resp = new CommitWALObjectResponse();
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
            // commit object
            S3Object s3Object = new S3Object(
                objectId, objectSize, object.getObjectKey(),
                object.getPreparedTimeInMs(), object.getExpiredTimeInMs(), System.currentTimeMillis(), -1,
                S3ObjectState.COMMITTED);
            this.objectsMetadata.put(objectId, s3Object
            );
            // build metadata
            MemoryBrokerWALMetadata walMetadata = this.brokerWALMetadata.computeIfAbsent(MOCK_BROKER_ID,
                    k -> new MemoryBrokerWALMetadata(k));
            Map<Long, List<StreamOffsetRange>> index = new HashMap<>();
            streamRanges.stream().forEach(range -> {
                long streamId = range.getStreamId();
                long startOffset = range.getStartOffset();
                long endOffset = range.getEndOffset();
                index.put(streamId, List.of(new StreamOffsetRange(streamId, startOffset, endOffset)));
                // update range endOffset
                MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
                streamMetadata.endOffset = endOffset;
            });
            request.getStreamObjects().forEach(streamObject -> {
                MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamObject.getStreamId());
                S3StreamObject s3StreamObject = new S3StreamObject(streamObject.getObjectId(), streamObject.getObjectSize(),
                        streamObject.getStreamId(), streamObject.getStartOffset(), streamObject.getEndOffset());
                streamMetadata.addStreamObject(s3StreamObject);
                streamMetadata.endOffset = Math.max(streamMetadata.endOffset, streamObject.getEndOffset());
            });
            S3WALObject walObject = new S3WALObject(objectId, MOCK_BROKER_ID, index, request.getOrderId(), s3Object.getCommittedTimeInMs());
            walMetadata.walObjects.add(walObject);
            return resp;
        });
    }

    private boolean verifyWalStreamRanges(ObjectStreamRange range) {
        long streamId = range.getStreamId();
        long epoch = range.getEpoch();
        // verify
        MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
        if (streamMetadata == null) {
            return false;
        }
        // compare epoch
        if (streamMetadata.epoch > epoch) {
            return false;
        }
        if (streamMetadata.epoch < epoch) {
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        //TODO: SUPPORT
        return CompletableFuture.completedFuture(null);
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
            MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            if (endOffset <= streamMetadata.startOffset) {
                return objs;
            }
            MemoryBrokerWALMetadata metadata = this.brokerWALMetadata.get(MOCK_BROKER_ID);
            if (metadata == null) {
                return objs;
            }
            for (S3WALObject walObject : metadata.walObjects) {
                if (need <= 0) {
                    break;
                }
                if (!walObject.intersect(streamId, startOffset, endOffset)) {
                    continue;
                }
                // find stream index, get object
                S3Object object = this.objectsMetadata.get(walObject.objectId());
                S3ObjectMetadata obj = new S3ObjectMetadata(walObject.objectId(), object.getObjectSize(), walObject.objectType());
                objs.add(obj);
                need--;
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
    public List<S3ObjectMetadata> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        throw new UnsupportedOperationException("Not support");
    }

    @Override
    public CompletableFuture<List<StreamOffsetRange>> getOpeningStreams() {
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public CompletableFuture<Long> createStream() {
        return this.submitEvent(() -> {
            long streamId = this.nextAssignedStreamId++;
            this.streamsMetadata.put(streamId,
                    new MemoryStreamMetadata(streamId));
            return streamId;
        });
    }

    @Override
    public CompletableFuture<OpenStreamMetadata> openStream(long streamId, long epoch) {
        return this.submitEvent(() -> {
            // TODO: all task should wrapped with try catch to avoid future is forgot to complete
            // verify stream exist
            if (!this.streamsMetadata.containsKey(streamId)) {
                throw new StreamNotExistException("Stream " + streamId + " does not exist");
            }
            MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            // verify epoch match
            if (streamMetadata.epoch > epoch) {
                throw new StreamFencedException("Stream " + streamId + " is fenced");
            }
            if (streamMetadata.epoch == epoch) {
                if (streamMetadata.state == StreamState.OPENED) {
                    // duplicate open
                    return new OpenStreamMetadata(streamId, epoch, streamMetadata.startOffset, streamMetadata.endOffset);
                }
                if (streamMetadata.state == StreamState.CLOSED) {
                    // stream is closed, can't open again at same epoch
                    throw new StreamFencedException("Stream " + streamId + " is fenced");
                }
            }
            if (streamMetadata.state == StreamState.OPENED) {
                // stream still opened, can't open again until it's closed
                throw new StreamNotClosedException("Stream " + streamId + " is not closed");
            }
            // update epoch
            streamMetadata.epoch = epoch;
            streamMetadata.state = StreamState.OPENED;
            return new OpenStreamMetadata(streamId, epoch, streamMetadata.startOffset, streamMetadata.endOffset);
        });
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long epoch) {
        return this.submitEvent(() -> {
            // verify stream exist
            if (!this.streamsMetadata.containsKey(streamId)) {
                throw new StreamNotExistException("Stream " + streamId + " does not exist");
            }
            MemoryStreamMetadata streamMetadata = this.streamsMetadata.get(streamId);
            // verify epoch match
            if (streamMetadata.epoch > epoch) {
                LOGGER.warn("Stream {} is fenced, request: {}, current: {}", streamId, epoch, streamMetadata.epoch);
                throw new StreamFencedException("Stream " + streamId + " is fenced");
            }
            if (streamMetadata.epoch < epoch) {
                // this should not happen
                LOGGER.error("Stream {} epoch is not match, request: {}, current: {}", streamId, epoch, streamMetadata.epoch);
                throw new RuntimeException("Stream " + streamId + " epoch is not match");
            }
            if (streamMetadata.state == StreamState.CLOSED) {
                LOGGER.warn("Stream {} is already closed at epoch: {}", streamId, epoch);
                // duplicate close
                return null;
            }
            // update epoch
            streamMetadata.state = StreamState.CLOSED;
            return null;
        });
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
            try {
                T value = eventHandler.get();
                cb.complete(value);
            } catch (Exception e) {
                LOGGER.error("Failed to execute event", e);
                cb.completeExceptionally(e);
            }
        }
    }
}
