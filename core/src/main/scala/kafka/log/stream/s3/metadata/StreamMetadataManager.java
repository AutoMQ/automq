/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.metadata;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.utils.FutureUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import kafka.server.BrokerServer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.automq.stream.utils.FutureUtil.exec;

public class StreamMetadataManager implements InRangeObjectsFetcher, MetadataPublisher {
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    private final int nodeId;
    private final List<GetObjectsTask> pendingGetObjectsTasks;
    private final ExecutorService pendingExecutorService;
    private volatile S3StreamImagePair image;

    public StreamMetadataManager(BrokerServer broker, int nodeId) {
        this.nodeId = nodeId;
        this.image = new S3StreamImagePair(broker.metadataCache().currentImage());
        this.pendingGetObjectsTasks = new LinkedList<>();
        this.pendingExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pending-get-objects-task-executor"));
        broker.metadataLoader().installPublishers(List.of(this)).join();
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (newImage.highestOffsetAndEpoch().equals(this.image.version())) {
            return;
        }

        // update image
        this.image = new S3StreamImagePair(newImage);
        // retry all pending tasks
        retryPendingTasks();
    }

    public CompletableFuture<List<S3ObjectMetadata>> getStreamSetObjects() {
        final S3StreamImagePair image = this.image;

        final S3StreamsMetadataImage streamsImage = image.streamsMetadata();
        final S3ObjectsImage objectsImage = image.objectsMetadata();

        List<S3ObjectMetadata> s3ObjectMetadataList = streamsImage.getStreamSetObjects(nodeId).stream()
                .map(object -> {
                    S3Object s3Object = objectsImage.getObjectMetadata(object.objectId());
                    return new S3ObjectMetadata(object.objectId(), object.objectType(),
                            object.offsetRangeList(), object.dataTimeInMs(),
                            s3Object.getCommittedTimeInMs(), s3Object.getObjectSize(),
                            object.orderId());
                })
                .collect(Collectors.toList());
        return CompletableFuture.completedFuture(s3ObjectMetadataList);
    }

    @Override
    public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        // TODO: cache the object list for next search
        return exec(() -> fetch0(streamId, startOffset, endOffset, limit), LOGGER, "fetchObjects")
                .thenApply(rst -> {
                    final InRangeObjects inRangeObjects = rst.getKey();
                    final S3StreamImagePair image = rst.getValue();

                    inRangeObjects.objects().forEach(object -> {
                        S3Object objectMetadata = image.objectsMetadata().getObjectMetadata(object.objectId());
                        if (objectMetadata == null) {
                            // should not happen
                            LOGGER.error("[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, " +
                                            "and search in metadataCache failed with empty result",
                                    streamId, startOffset, endOffset, limit);
                            throw new IllegalStateException("can't find object metadata for object: " + object.objectId());
                        }
                        object.setObjectSize(objectMetadata.getObjectSize());
                        object.setCommittedTimestamp(objectMetadata.getCommittedTimeInMs());

                    });

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, " +
                                        "and search in metadataCache success with result: {}",
                                streamId, startOffset, endOffset, limit, rst);
                    }

                    return inRangeObjects;
                });
    }

    private CompletableFuture<Map.Entry<InRangeObjects, S3StreamImagePair>> fetch0(long streamId, long startOffset, long endOffset, int limit) {
        final S3StreamImagePair image = this.image;
        final S3StreamsMetadataImage streamsImage = image.streamsMetadata();

        InRangeObjects rst = streamsImage.getObjects(streamId, startOffset, endOffset, limit);
        if (rst.objects().size() >= limit || rst.endOffset() >= endOffset || rst == InRangeObjects.INVALID) {
            return CompletableFuture.completedFuture(Map.entry(rst, image));
        }

        LOGGER.info("[FetchObjects],[PENDING],streamId={} startOffset={} endOffset={} limit={}",
                streamId, startOffset, endOffset, limit);

        CompletableFuture<Void> pendingCf = pendingFetch();
        CompletableFuture<Map.Entry<InRangeObjects, S3StreamImagePair>> rstCf = new CompletableFuture<>();
        FutureUtil.propagate(pendingCf.thenCompose(nil -> fetch0(streamId, startOffset, endOffset, limit)), rstCf);
        return rstCf.whenComplete((r, ex) -> {
            LOGGER.info("[FetchObjects],[COMPLETE_PENDING],streamId={} startOffset={} endOffset={} limit={}",
                    streamId, startOffset, endOffset, limit);
        });
    }

    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        final S3StreamImagePair image = this.image;

        final S3StreamsMetadataImage streamsImage = image.streamsMetadata();
        final S3ObjectsImage objectsImage = image.objectsMetadata();

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

    public List<StreamMetadata> getStreamMetadataList(List<Long> streamIds) {
        final S3StreamImagePair image = this.image;

        final S3StreamsMetadataImage streamsImage = image.streamsMetadata();

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

    public boolean isObjectExist(long objectId) {
        final S3StreamImagePair image = this.image;
        final S3ObjectsImage objectsImage = image.objectsMetadata();

        S3Object object = objectsImage.getObjectMetadata(objectId);
        if (object == null) {
            return false;
        }
        return object.getS3ObjectState() == S3ObjectState.COMMITTED;
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

    private static class S3StreamImagePair {
        private final S3StreamsMetadataImage streamsImage;
        private final S3ObjectsImage objectsImage;
        private final OffsetAndEpoch offsetAndEpoch;

        public S3StreamImagePair(MetadataImage image) {
            this.streamsImage = image.streamsMetadata();
            this.objectsImage = image.objectsMetadata();
            this.offsetAndEpoch = image.highestOffsetAndEpoch();
        }

        public S3StreamsMetadataImage streamsMetadata() {
            return streamsImage;
        }

        public S3ObjectsImage objectsMetadata() {
            return objectsImage;
        }

        OffsetAndEpoch version() {
            return this.offsetAndEpoch;
        }
    }

}
