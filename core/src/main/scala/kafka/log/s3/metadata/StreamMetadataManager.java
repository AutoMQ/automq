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

package kafka.log.s3.metadata;

import io.netty.util.concurrent.DefaultThreadFactory;
import kafka.log.es.FutureUtil;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3StreamConstant;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.StreamOffsetRange;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.kafka.metadata.stream.ObjectUtils.NOOP_OFFSET;

public class StreamMetadataManager implements InRangeObjectsFetcher {

    // TODO: optimize by more suitable concurrent protection
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    private final KafkaConfig config;
    private final BrokerServer broker;
    private final Map<Long/*stream id*/, Map<Long/*end offset*/, List<GetObjectsTask>>> pendingGetObjectsTasks;
    private final ExecutorService pendingExecutorService;
    // TODO: we just need the version of streams metadata, not the whole image
    private volatile OffsetAndEpoch version;
    private S3StreamsMetadataImage streamsImage;
    private S3ObjectsImage objectsImage;

    public StreamMetadataManager(BrokerServer broker, KafkaConfig config) {
        this.config = config;
        this.broker = broker;
        MetadataImage currentImage = this.broker.metadataCache().currentImage();
        this.streamsImage = currentImage.streamsMetadata();
        this.objectsImage = currentImage.objectsMetadata();
        this.version = currentImage.highestOffsetAndEpoch();
        this.broker.metadataListener().registerStreamMetadataListener(this::onImageChanged);
        // TODO: optimize by more suitable data structure for pending tasks
        this.pendingGetObjectsTasks = new HashMap<>();
        this.pendingExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pending-get-objects-task-executor"));
    }

    private void onImageChanged(MetadataDelta delta, MetadataImage newImage) {
        if (newImage.highestOffsetAndEpoch().equals(this.version)) {
            return;
        }
        synchronized (this) {
            // update version
            this.version = newImage.highestOffsetAndEpoch();
            // update image
            this.streamsImage = newImage.streamsMetadata();
            this.objectsImage = newImage.objectsMetadata();
            // remove all catch up pending tasks
            List<GetObjectsTask> retryTasks = removePendingTasks();
            // retry all pending tasks
            if (retryTasks == null || retryTasks.isEmpty()) {
                return;
            }
            this.pendingExecutorService.submit(() -> {
                retryPendingTasks(retryTasks);
            });
        }
    }

    public CompletableFuture<List<S3ObjectMetadata>> getWALObjects() {
        synchronized (this) {
            List<S3ObjectMetadata> s3ObjectMetadataList = this.streamsImage.getWALObjects(config.brokerId()).stream()
                    .map(object -> {
                        S3Object s3Object = this.objectsImage.getObjectMetadata(object.objectId());
                        return new S3ObjectMetadata(object.objectId(), object.objectType(),
                                new ArrayList<>(object.offsetRanges().values()), object.dataTimeInMs(),
                                s3Object.getCommittedTimeInMs(), s3Object.getObjectSize(),
                                object.orderId());
                    })
                    .collect(Collectors.toList());
            return CompletableFuture.completedFuture(s3ObjectMetadataList);
        }
    }

    // must access thread safe
    private List<GetObjectsTask> removePendingTasks() {
        if (this.pendingGetObjectsTasks == null || this.pendingGetObjectsTasks.isEmpty()) {
            return Collections.emptyList();
        }
        Set<Long> pendingStreams = pendingGetObjectsTasks.keySet();
        List<StreamOffsetRange> pendingStreamsOffsetRange = pendingStreams
                .stream()
                .map(streamsImage::offsetRange)
                .filter(offset -> offset != StreamOffsetRange.INVALID)
                .collect(Collectors.toList());
        if (pendingStreamsOffsetRange.isEmpty()) {
            return Collections.emptyList();
        }
        List<GetObjectsTask> retryTasks = new ArrayList<>();
        pendingStreamsOffsetRange.forEach(offsetRange -> {
            long streamId = offsetRange.getStreamId();
            long endOffset = offsetRange.getEndOffset();
            Map<Long, List<GetObjectsTask>> tasks = StreamMetadataManager.this.pendingGetObjectsTasks.get(streamId);
            if (tasks == null || tasks.isEmpty()) {
                return;
            }
            Iterator<Entry<Long, List<GetObjectsTask>>> iterator =
                    tasks.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<Long, List<GetObjectsTask>> entry = iterator.next();
                long pendingEndOffset = entry.getKey();
                if (pendingEndOffset > endOffset) {
                    break;
                }
                iterator.remove();
                List<GetObjectsTask> getObjectsTasks = entry.getValue();
                retryTasks.addAll(getObjectsTasks);
            }
            if (tasks.isEmpty()) {
                StreamMetadataManager.this.pendingGetObjectsTasks.remove(streamId);
            }
        });
        return retryTasks;
    }

    @Override
    public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        synchronized (StreamMetadataManager.this) {
            S3StreamMetadataImage streamImage = streamsImage.streamsMetadata().get(streamId);
            if (streamImage == null) {
                LOGGER.warn(
                        "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and streamImage is null",
                        streamId, startOffset, endOffset, limit);
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            StreamOffsetRange offsetRange = streamImage.offsetRange();
            if (offsetRange == null || offsetRange == StreamOffsetRange.INVALID) {
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            long streamStartOffset = offsetRange.getStartOffset();
            long streamEndOffset = offsetRange.getEndOffset();
            if (startOffset < streamStartOffset) {
                LOGGER.warn(
                        "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and startOffset < streamStartOffset: {}",
                        streamId, startOffset, endOffset, limit, streamStartOffset);
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            endOffset = endOffset == NOOP_OFFSET ? streamEndOffset : endOffset;
            if (endOffset > streamEndOffset) {
                // lag behind, need to wait for cache catch up
                return pendingFetch(streamId, startOffset, endOffset, limit);
            }
            return fetch0(streamId, startOffset, endOffset, limit);
        }
    }

    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        synchronized (StreamMetadataManager.this) {
            try {
                List<S3StreamObject> streamObjects = streamsImage.getStreamObjects(streamId, startOffset, endOffset, limit);
                List<S3ObjectMetadata> s3StreamObjectMetadataList = streamObjects.stream().map(object -> {
                    S3Object objectMetadata = objectsImage.getObjectMetadata(object.objectId());
                    long committedTimeInMs = objectMetadata.getCommittedTimeInMs();
                    long objectSize = objectMetadata.getObjectSize();
                    return new S3ObjectMetadata(object.objectId(), object.objectType(), List.of(object.streamOffsetRange()), object.dataTimeInMs(),
                        committedTimeInMs, objectSize, S3StreamConstant.INVALID_ORDER_ID);
                }).collect(Collectors.toList());
                return CompletableFuture.completedFuture(s3StreamObjectMetadataList);
            } catch (Exception e) {
                LOGGER.warn(
                        "[GetStreamObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with exception: {}",
                        streamId, startOffset, endOffset, limit, e.getMessage());
                return CompletableFuture.failedFuture(e);
            }
        }
    }

    // must access thread safe
    private CompletableFuture<InRangeObjects> pendingFetch(long streamId, long startOffset, long endOffset, int limit) {
        GetObjectsTask task = GetObjectsTask.of(streamId, startOffset, endOffset, limit);
        Map<Long, List<GetObjectsTask>> tasks = StreamMetadataManager.this.pendingGetObjectsTasks.computeIfAbsent(task.streamId,
                k -> new TreeMap<>());
        List<GetObjectsTask> getObjectsTasks = tasks.computeIfAbsent(task.endOffset, k -> new ArrayList<>());
        getObjectsTasks.add(task);
        LOGGER.warn("[PendingFetch]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and pending fetch", streamId, startOffset, endOffset,
                limit);
        return task.cf;
    }

    // must access thread safe
    private CompletableFuture<InRangeObjects> fetch0(long streamId, long startOffset, long endOffset, int limit) {
        InRangeObjects cachedInRangeObjects = streamsImage.getObjects(streamId, startOffset, endOffset, limit);
        if (cachedInRangeObjects == null || cachedInRangeObjects == InRangeObjects.INVALID) {
            LOGGER.warn(
                    "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                    streamId, startOffset, endOffset, limit);
            return CompletableFuture.completedFuture(InRangeObjects.INVALID);
        }
        // fill the objects' size and committed-timestamp
        for (S3ObjectMetadata object : cachedInRangeObjects.objects()) {
            S3Object objectMetadata = objectsImage.getObjectMetadata(object.objectId());
            if (objectMetadata == null) {
                // should not happen
                LOGGER.error(
                        "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                        streamId, startOffset, endOffset, limit);
                return CompletableFuture.completedFuture(InRangeObjects.INVALID);
            }
            object.setObjectSize(objectMetadata.getObjectSize());
            object.setCommittedTimestamp(objectMetadata.getCommittedTimeInMs());
        }
        LOGGER.trace(
                "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache success with result: {}",
                streamId, startOffset, endOffset, limit, cachedInRangeObjects);
        return CompletableFuture.completedFuture(cachedInRangeObjects);
    }

    public void retryPendingTasks(List<GetObjectsTask> tasks) {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        LOGGER.info("[RetryPendingTasks]: retry tasks count: {}", tasks.size());
        tasks.forEach(task -> {
            long streamId = task.streamId;
            long startOffset = task.startOffset;
            long endOffset = task.endOffset;
            int limit = task.limit;
            CompletableFuture<InRangeObjects> newCf = this.fetch(streamId, startOffset, endOffset, limit);
            FutureUtil.propagate(newCf, task.cf);
        });
    }

    static class GetObjectsTask {

        private final CompletableFuture<InRangeObjects> cf;
        private final long streamId;
        private final long startOffset;
        private final long endOffset;
        private final int limit;

        public static GetObjectsTask of(long streamId, long startOffset, long endOffset, int limit) {
            CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
            GetObjectsTask task = new GetObjectsTask(cf, streamId, startOffset, endOffset, limit);
            return task;
        }

        private GetObjectsTask(CompletableFuture<InRangeObjects> cf, long streamId, long startOffset, long endOffset, int limit) {
            this.cf = cf;
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.limit = limit;
        }
    }

    public interface StreamMetadataListener {

        void onChange(MetadataDelta delta, MetadataImage image);
    }

}
