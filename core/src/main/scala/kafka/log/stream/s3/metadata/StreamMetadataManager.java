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

package kafka.log.stream.s3.metadata;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.utils.FutureUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import kafka.server.BrokerServer;
import kafka.server.KafkaConfig;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.automq.stream.utils.FutureUtil.exec;

public class StreamMetadataManager implements InRangeObjectsFetcher {

    // TODO: optimize by more suitable concurrent protection
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    private final KafkaConfig config;
    private final BrokerServer broker;
    private final List<GetObjectsTask> pendingGetObjectsTasks;
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
        this.broker.metadataListener().registerMetadataListener(this::onImageChanged);
        this.pendingGetObjectsTasks = new LinkedList<>();
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

            // retry all pending tasks
            retryPendingTasks();
        }
    }

    public CompletableFuture<List<S3ObjectMetadata>> getStreamSetObjects() {
        synchronized (this) {
            List<S3ObjectMetadata> s3ObjectMetadataList = this.streamsImage.getStreamSetObjects(config.brokerId()).stream()
                    .map(object -> {
                        S3Object s3Object = this.objectsImage.getObjectMetadata(object.objectId());
                        return new S3ObjectMetadata(object.objectId(), object.objectType(),
                                object.offsetRangeList(), object.dataTimeInMs(),
                                s3Object.getCommittedTimeInMs(), s3Object.getObjectSize(),
                                object.orderId());
                    })
                    .collect(Collectors.toList());
            return CompletableFuture.completedFuture(s3ObjectMetadataList);
        }
    }

    @Override
    public synchronized CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        // TODO: cache the object list for next search
        return exec(() -> fetch0(streamId, startOffset, endOffset, limit), LOGGER, "fetchObjects").thenApply(rst -> {
            rst.objects().forEach(object -> {
                S3Object objectMetadata = objectsImage.getObjectMetadata(object.objectId());
                if (objectMetadata == null) {
                    // should not happen
                    LOGGER.error(
                            "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache failed with empty result",
                            streamId, startOffset, endOffset, limit);
                    throw new IllegalStateException("cannt find object metadata for object: " + object.objectId());
                }
                object.setObjectSize(objectMetadata.getObjectSize());
                object.setCommittedTimestamp(objectMetadata.getCommittedTimeInMs());

            });
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                        "[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, and search in metadataCache success with result: {}",
                        streamId, startOffset, endOffset, limit, rst);
            }
            return rst;
        });
    }

    private synchronized CompletableFuture<InRangeObjects> fetch0(long streamId, long startOffset, long endOffset, int limit) {
        InRangeObjects rst = streamsImage.getObjects(streamId, startOffset, endOffset, limit);
        if (rst.objects().size() >= limit || rst.endOffset() >= endOffset || rst == InRangeObjects.INVALID) {
            return CompletableFuture.completedFuture(rst);
        }
        if (rst.endOffset() >= endOffset || rst.objects().size() >= limit) {
            return CompletableFuture.completedFuture(rst);
        }
        LOGGER.info("[FetchObjects],[PENDING],streamId={} startOffset={} endOffset={} limit={}", streamId, startOffset, endOffset, limit);
        CompletableFuture<Void> pendingCf = pendingFetch();
        CompletableFuture<InRangeObjects> rstCf = new CompletableFuture<>();
        FutureUtil.propagate(pendingCf.thenCompose(nil -> fetch0(streamId, startOffset, endOffset, limit)), rstCf);
        return rstCf.whenComplete((r, ex) -> LOGGER.info("[FetchObjects],[COMPLETE_PENDING],streamId={} startOffset={} endOffset={} limit={}", streamId, startOffset, endOffset, limit));
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

    public List<StreamMetadata> getStreamMetadataList(List<Long> streamIds) {
        synchronized (StreamMetadataManager.this) {
            List<StreamMetadata> streamMetadataList = new ArrayList<>();
            for (Long streamId : streamIds) {
                S3StreamMetadataImage streamImage = streamsImage.streamsMetadata().get(streamId);
                if (streamImage == null) {
                    LOGGER.warn("[GetStreamMetadataList]: stream: {} not exists", streamId);
                    continue;
                }
                StreamMetadata streamMetadata = new StreamMetadata(streamId, streamImage.getEpoch(),
                        streamImage.getStartOffset(), -1L, streamImage.state()) {
                    @Override
                    public long endOffset() {
                        throw new UnsupportedOperationException();
                    }
                };
                streamMetadataList.add(streamMetadata);
            }
            return streamMetadataList;
        }
    }

    // must access thread safe
    private CompletableFuture<Void> pendingFetch() {
        GetObjectsTask task = new GetObjectsTask();
        synchronized (pendingGetObjectsTasks) {
            pendingGetObjectsTasks.add(task);
        }
        return task.cf;
    }

    void retryPendingTasks() {
        synchronized (pendingGetObjectsTasks) {
            if (pendingGetObjectsTasks.isEmpty()) {
                return;
            }
            LOGGER.info("[RetryPendingTasks]: retry tasks count: {}", pendingGetObjectsTasks.size());
            pendingGetObjectsTasks.forEach(t -> t.cf.completeAsync(() -> null, pendingExecutorService));
            pendingGetObjectsTasks.clear();
        }
    }

    static class GetObjectsTask {

        private final CompletableFuture<Void> cf;

        public GetObjectsTask() {
            this.cf = new CompletableFuture<>();
        }
    }

}
