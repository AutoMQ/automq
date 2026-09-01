/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamMetadataListener;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;

import static com.automq.stream.utils.FutureUtil.exec;

public class StreamMetadataManager implements InRangeObjectsFetcher, MetadataPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamMetadataManager.class);
    // AutoMQ inject start
    private static final int ARCHIVE_LIST_MAX_ATTEMPTS = 3;
    private static final long ARCHIVE_LIST_RETRY_DELAY_MILLIS = 100L;
    private static final long ARCHIVE_CORRUPTION_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final int MAX_ARCHIVE_CORRUPTION_LOG_KEYS = 1_024;
    private static final Map<String, Long> LAST_ARCHIVE_CORRUPTION_LOG_NANOS = new java.util.HashMap<>();
    // AutoMQ inject end
    private final int nodeId;
    private final List<GetObjectsTask> pendingGetObjectsTasks;
    private final ExecutorService pendingExecutorService;
    private MetadataImage metadataImage;
    private final ObjectReaderFactory objectReaderFactory;
    private final LocalStreamRangeIndexCache indexCache;
    private final Map<Long, StreamMetadataListener> streamMetadataListeners = new ConcurrentHashMap<>();

    public StreamMetadataManager(BrokerServer broker, int nodeId, ObjectReaderFactory objectReaderFactory,
        LocalStreamRangeIndexCache indexCache) {
        this.nodeId = nodeId;
        this.metadataImage = broker.metadataCache().retainedImage();
        this.pendingGetObjectsTasks = new LinkedList<>();
        this.objectReaderFactory = objectReaderFactory;
        this.indexCache = indexCache;
        this.pendingExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pending-get-objects-task-executor"));
        broker.metadataLoader().installPublishers(List.of(this)).join();
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        Set<Long> changedStreams;
        synchronized (this) {
            if (newImage.highestOffsetAndEpoch().equals(this.metadataImage.highestOffsetAndEpoch())) {
                return;
            }
            newImage.retain();
            MetadataImage oldImage = this.metadataImage;
            this.metadataImage = newImage;
            changedStreams = delta.getOrCreateStreamsMetadataDelta().changedStreams();
            oldImage.release();
        }
        // retry all pending tasks
        retryPendingTasks();
        this.indexCache.asyncPrune(this::getStreamSetObjectIds);
        notifyMetadataListeners(changedStreams);
    }

    public CompletableFuture<List<S3ObjectMetadata>> getStreamSetObjects() {
        try (Image image = getImage()) {
            final S3StreamsMetadataImage streamsImage = image.streamsMetadata();
            final S3ObjectsImage objectsImage = image.objectsMetadata();
            List<S3ObjectMetadata> s3ObjectMetadataList = streamsImage.getStreamSetObjects(nodeId).stream()
                .map(object -> {
                    S3Object s3Object = objectsImage.getObjectMetadata(object.objectId());
                    return new S3ObjectMetadata(object.objectId(), object.objectType(),
                        object.offsetRangeList(), object.dataTimeInMs(),
                        s3Object.getTimestamp(), s3Object.getObjectSize(),
                        object.orderId(), s3Object.getAttributes());
                })
                .collect(Collectors.toList());
            return CompletableFuture.completedFuture(s3ObjectMetadataList);
        }
    }

    public Set<Long> getStreamSetObjectIds() {
        try (Image image = getImage()) {
            return image.streamsMetadata().getStreamSetObjects(nodeId).stream()
                .map(S3StreamSetObject::objectId).collect(Collectors.toSet());
        }
    }

    @Override
    public CompletableFuture<InRangeObjects> fetch(long streamId, long startOffset, long endOffset, int limit) {
        // TODO: cache the object list for next search
        CompletableFuture<InRangeObjects> cf = new CompletableFuture<>();
        exec(() -> fetch0(cf, streamId, startOffset, endOffset, limit, false), cf, LOGGER, "fetchObjects");
        return cf;
    }

    // AutoMQ inject start
    private void fetch0(CompletableFuture<InRangeObjects> cf, long streamId,
        long startOffset, long endOffset, int limit, boolean retryFetch) {
        Image image = getImage();
        CompletableFuture<InRangeObjects> getObjectsCf;
        try {
            final S3StreamsMetadataImage streamsImage = image.streamsMetadata();
            final S3ObjectsImage objectsImage = image.objectsMetadata();
            getObjectsCf = fetchFromImage(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
        } catch (Throwable e) {
            image.close();
            cf.completeExceptionally(e);
            return;
        }
        getObjectsCf.whenComplete((rst, exception) -> {
            image.close();
            Throwable cause = FutureUtil.cause(exception);
            if (cause != null) {
                if (cause instanceof ArchiveCorruptionException) {
                    logArchiveCorruption(streamId, startOffset, endOffset, cause);
                }
                cf.completeExceptionally(cause);
                return;
            }
            try {
                if (rst.objects().size() >= limit || rst.endOffset() >= endOffset || rst == InRangeObjects.INVALID) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, " +
                                "and search in metadataCache success with result: {}",
                            streamId, startOffset, endOffset, limit, rst);
                    }
                    cf.complete(rst);
                    return;
                }

                LOGGER.info("[FetchObjects],[PENDING],streamId={} startOffset={} endOffset={} limit={} resultSize={} resultEndOffset={}",
                    streamId, startOffset, endOffset, limit, rst.objects().size(), rst.endOffset());

                CompletableFuture<Void> pendingCf = pendingFetch();
                pendingCf.thenAccept(nil -> fetch0(cf, streamId, startOffset, endOffset, limit, true));
                if (!retryFetch) {
                    cf.whenComplete((r, ex) ->
                        LOGGER.info("[FetchObjects],[COMPLETE_PENDING],streamId={} startOffset={} endOffset={} limit={}", streamId, startOffset, endOffset, limit));
                }
            } catch (Throwable e) {
                cf.completeExceptionally(e);
            }
        });
    }

    private CompletableFuture<InRangeObjects> fetchFromImage(S3StreamsMetadataImage streamsImage,
        S3ObjectsImage objectsImage, long streamId, long startOffset, long endOffset, int limit) {
        if (invalidFetchRequest(streamId, startOffset, endOffset, limit) || limit == 0) {
            return fetchOnline(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
        }
        S3StreamArchiveMetadata archive = streamsImage.getStreamArchiveMetadata(streamId);
        S3StreamMetadataImage stream = streamsImage.getStreamMetadata(streamId);
        if (shouldFetchOnlineOnly(archive, stream, startOffset)) {
            return fetchOnline(streamsImage, objectsImage, streamId, startOffset, endOffset, limit);
        }
        long archiveEndOffset = archive.archiveEndOffset();
        long archiveTargetOffset = endOffset == ObjectUtils.NOOP_OFFSET
            ? archiveEndOffset : Math.min(endOffset, archiveEndOffset);
        ObjectStorage.ListOptions options = new ObjectStorage.ListOptions(ArchiveObjectKey.manifestPrefix(streamId))
            .startAfter(ArchiveObjectKey.startAfter(streamId, startOffset))
            .maxKeys(limit);
        return listArchiveObjects(options, 1).thenCompose(listedObjects -> {
            List<S3ObjectMetadata> archivedObjects = parseArchivedObjects(streamId, startOffset,
                archiveTargetOffset, archiveEndOffset, limit, listedObjects);
            long coveredEndOffset = archivedObjects.isEmpty()
                ? startOffset : archivedObjects.get(archivedObjects.size() - 1).endOffset();
            if (archivedObjects.size() >= limit
                || (endOffset != ObjectUtils.NOOP_OFFSET && coveredEndOffset >= endOffset)) {
                return CompletableFuture.completedFuture(new InRangeObjects(streamId, archivedObjects));
            }
            if (coveredEndOffset < archiveTargetOffset) {
                return CompletableFuture.failedFuture(new ArchiveMissingCoverageException(String.format(
                    "Archive LIST for stream %d covered only to %d, expected %d", streamId, coveredEndOffset,
                    archiveTargetOffset)));
            }
            int remainingLimit = limit - archivedObjects.size();
            return fetchOnline(streamsImage, objectsImage, streamId, archiveEndOffset, endOffset, remainingLimit)
                .thenApply(online -> combineArchiveAndOnline(streamId, archiveEndOffset, archivedObjects, online));
        });
    }

    private CompletableFuture<List<ObjectStorage.ObjectInfo>> listArchiveObjects(ObjectStorage.ListOptions options,
        int attempt) {
        ObjectStorage storage = objectReaderFactory.getObjectStorage();
        CompletableFuture<List<ObjectStorage.ObjectInfo>> listFuture;
        try {
            listFuture = storage.list(options);
        } catch (Throwable e) {
            listFuture = CompletableFuture.failedFuture(e);
        }
        return listFuture.handle((result, ex) -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause == null) {
                return CompletableFuture.completedFuture(result);
            }
            if (attempt >= ARCHIVE_LIST_MAX_ATTEMPTS || !storage.isListRetriable(cause)) {
                return CompletableFuture.<List<ObjectStorage.ObjectInfo>>failedFuture(cause);
            }
            return CompletableFuture.supplyAsync(() -> null,
                    CompletableFuture.delayedExecutor(ARCHIVE_LIST_RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS))
                .thenCompose(ignored -> listArchiveObjects(options, attempt + 1));
        }).thenCompose(result -> result);
    }

    private boolean invalidFetchRequest(long streamId, long startOffset, long endOffset, int limit) {
        return streamId < 0 || limit < 0
            || (endOffset != ObjectUtils.NOOP_OFFSET && startOffset > endOffset);
    }

    private boolean shouldFetchOnlineOnly(S3StreamArchiveMetadata archive, S3StreamMetadataImage stream,
        long startOffset) {
        return archive == null || stream == null || startOffset < stream.startOffset()
            || startOffset >= archive.archiveEndOffset();
    }

    private List<S3ObjectMetadata> parseArchivedObjects(long streamId, long startOffset, long targetOffset,
        long archiveEndOffset, int limit, List<ObjectStorage.ObjectInfo> listedObjects) {
        List<S3ObjectMetadata> objects = new ArrayList<>(Math.min(limit, listedObjects.size()));
        long expectedOffset = startOffset;
        for (ObjectStorage.ObjectInfo objectInfo : listedObjects) {
            if (objects.size() >= limit || (!objects.isEmpty() && expectedOffset >= targetOffset)) {
                break;
            }
            ArchiveObjectKey.ManifestKey key = parseArchiveKey(objectInfo.key());
            validateArchiveKey(key, streamId, startOffset, expectedOffset, objects.isEmpty(), archiveEndOffset);
            int attributes = ObjectAttributes.builder().bucket(objectInfo.bucketId())
                .type(ObjectAttributes.Type.Composite).build().attributes();
            objects.add(new S3ObjectMetadata(key.objectId(), com.automq.stream.s3.metadata.S3ObjectType.COMPOSITE,
                List.of(new StreamOffsetRange(streamId, key.startOffset(), key.endOffset())),
                S3StreamConstant.INVALID_TS, objectInfo.timestamp(), objectInfo.size(),
                S3StreamConstant.INVALID_ORDER_ID, attributes, objectInfo.key()));
            expectedOffset = key.endOffset();
        }
        if (objects.isEmpty() || (expectedOffset < targetOffset && objects.size() < limit)) {
            throw new ArchiveMissingCoverageException(String.format(
                "Archive LIST for stream %d does not cover requested range [%d, %d)", streamId, startOffset,
                targetOffset));
        }
        return objects;
    }

    private ArchiveObjectKey.ManifestKey parseArchiveKey(String objectKey) {
        try {
            return ArchiveObjectKey.parseManifestKey(objectKey);
        } catch (IllegalArgumentException e) {
            throw new ArchiveMalformedKeyException("Cannot parse Archive key " + objectKey, e);
        }
    }

    private void validateArchiveKey(ArchiveObjectKey.ManifestKey key, long streamId, long requestedOffset,
        long expectedOffset, boolean firstObject, long archiveEndOffset) {
        if (key.streamId() != streamId) {
            throw new ArchiveMalformedKeyException("Archive key belongs to stream " + key.streamId()
                + " instead of " + streamId, null);
        }
        if (firstObject && (key.startOffset() > requestedOffset || key.endOffset() <= requestedOffset)) {
            throw new ArchiveMissingCoverageException(String.format(
                "First Archive range [%d, %d) does not cover requested offset %d",
                key.startOffset(), key.endOffset(), requestedOffset));
        }
        if (!firstObject && key.startOffset() != expectedOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Archive range starts at %d instead of continuous offset %d", key.startOffset(), expectedOffset));
        }
        if (key.startOffset() < archiveEndOffset && key.endOffset() > archiveEndOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Archive range [%d, %d) crosses published end offset %d", key.startOffset(), key.endOffset(),
                archiveEndOffset));
        }
    }

    private CompletableFuture<InRangeObjects> fetchOnline(S3StreamsMetadataImage streamsImage,
        S3ObjectsImage objectsImage, long streamId, long startOffset, long endOffset, int limit) {
        return streamsImage.getObjects(streamId, startOffset, endOffset, limit,
            new DefaultRangeGetter(objectsImage, objectReaderFactory), indexCache).thenApply(result -> {
                if (result == InRangeObjects.INVALID) {
                    return result;
                }
                result.objects().forEach(object -> enrichOnlineObject(objectsImage, streamId, startOffset, endOffset,
                    limit, object));
                return result;
            });
    }

    private void enrichOnlineObject(S3ObjectsImage objectsImage, long streamId, long startOffset, long endOffset,
        int limit, S3ObjectMetadata object) {
        S3Object objectMetadata = objectsImage.getObjectMetadata(object.objectId());
        if (objectMetadata == null) {
            LOGGER.error("[FetchObjects]: stream: {}, startOffset: {}, endOffset: {}, limit: {}, "
                    + "and search in metadataCache failed with empty result", streamId, startOffset, endOffset, limit);
            throw new IllegalStateException("can't find object metadata for object: " + object.objectId());
        }
        object.setObjectSize(objectMetadata.getObjectSize());
        object.setCommittedTimestamp(objectMetadata.getTimestamp());
        object.setAttributes(objectMetadata.getAttributes());
    }

    private InRangeObjects combineArchiveAndOnline(long streamId, long archiveEndOffset,
        List<S3ObjectMetadata> archivedObjects, InRangeObjects online) {
        if (online == InRangeObjects.INVALID) {
            return online;
        }
        if (!online.objects().isEmpty() && online.startOffset() != archiveEndOffset) {
            throw new ArchiveDiscontinuousRangeException(String.format(
                "Online range starts at %d instead of published Archive end %d", online.startOffset(),
                archiveEndOffset));
        }
        List<S3ObjectMetadata> combined = new ArrayList<>(archivedObjects.size() + online.objects().size());
        combined.addAll(archivedObjects);
        combined.addAll(online.objects());
        return new InRangeObjects(streamId, combined);
    }

    private static synchronized void logArchiveCorruption(long streamId, long startOffset, long endOffset,
        Throwable cause) {
        long now = System.nanoTime();
        String diagnosticKey = streamId + ":" + cause.getClass().getName() + ":" + cause.getMessage();
        Long previous = LAST_ARCHIVE_CORRUPTION_LOG_NANOS.get(diagnosticKey);
        boolean shouldLog = previous == null || now - previous >= ARCHIVE_CORRUPTION_LOG_INTERVAL_NANOS;
        if (shouldLog) {
            ensureArchiveCorruptionLogCapacity(diagnosticKey);
            LAST_ARCHIVE_CORRUPTION_LOG_NANOS.put(diagnosticKey, now);
        }
        if (shouldLog) {
            LOGGER.error("[FetchObjects],[ARCHIVE_CORRUPTION],streamId={} startOffset={} endOffset={}",
                streamId, startOffset, endOffset, cause);
        }
    }

    private static void ensureArchiveCorruptionLogCapacity(String diagnosticKey) {
        if (LAST_ARCHIVE_CORRUPTION_LOG_NANOS.containsKey(diagnosticKey)
            || LAST_ARCHIVE_CORRUPTION_LOG_NANOS.size() < MAX_ARCHIVE_CORRUPTION_LOG_KEYS) {
            return;
        }
        String oldestKey = LAST_ARCHIVE_CORRUPTION_LOG_NANOS.entrySet().stream()
            .min(Map.Entry.comparingByValue()).orElseThrow().getKey();
        LAST_ARCHIVE_CORRUPTION_LOG_NANOS.remove(oldestKey);
    }
    // AutoMQ inject end

    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        try (Image image = getImage()) {
            final S3StreamsMetadataImage streamsImage = image.streamsMetadata();
            final S3ObjectsImage objectsImage = image.objectsMetadata();
            List<S3StreamObject> streamObjects = streamsImage.getStreamObjects(streamId, startOffset, endOffset, limit);

            List<S3ObjectMetadata> s3StreamObjectMetadataList = streamObjects.stream().map(object -> {
                S3Object objectMetadata = objectsImage.getObjectMetadata(object.objectId());
                long committedTimeInMs = objectMetadata.getTimestamp();
                long objectSize = objectMetadata.getObjectSize();
                int attributes = objectMetadata.getAttributes();
                return new S3ObjectMetadata(object.objectId(), object.objectType(), List.of(object.streamOffsetRange()), objectMetadata.getTimestamp(),
                    committedTimeInMs, objectSize, S3StreamConstant.INVALID_ORDER_ID, attributes);
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
        try (Image image = getImage()) {
            final S3StreamsMetadataImage streamsImage = image.streamsMetadata();

            List<StreamMetadata> streamMetadataList = new ArrayList<>(streamIds.size());
            streamsImage.inLockRun(() -> {
                for (Long streamId : streamIds) {
                    S3StreamMetadataImage streamImage = streamsImage.timelineStreamMetadata().get(streamId);
                    if (streamImage == null) {
                        LOGGER.warn("[GetStreamMetadataList]: stream: {} not exists", streamId);
                        continue;
                    }
                    // If there is a streamImage, it means the stream exists.
                    @SuppressWarnings("OptionalGetWithoutIsPresent") long endOffset = streamsImage.streamEndOffset(streamId).getAsLong();
                    StreamMetadata streamMetadata = new StreamMetadata(streamId, streamImage.getEpoch(),
                        streamImage.getStartOffset(), endOffset, streamImage.state());
                    Optional.ofNullable(streamImage.lastRange()).ifPresent(r -> streamMetadata.nodeId(r.nodeId()));
                    streamMetadataList.add(streamMetadata);
                }
            });
            return streamMetadataList;
        }
    }

    // AutoMQ inject start
    /**
     * Returns the complete Archive state from one retained metadata Image.
     *
     * @param streamId Stream identity
     * @param streamEpoch owner epoch carried into the desired-state representation
     * @return complete locally observed Archive state
     */
    public StreamArchiveState getStreamArchive(long streamId, long streamEpoch) {
        try (Image image = getImage()) {
            S3StreamArchiveMetadata archive = image.streamsMetadata().getStreamArchiveMetadata(streamId);
            if (archive == null) {
                throw new IllegalArgumentException("Stream " + streamId + " does not exist");
            }
            return new StreamArchiveState(streamId, streamEpoch, archive.archiveStartOffset(),
                archive.archiveMetadataEndOffset(), archive.archiveEndOffset(), archive.archivePreparedEndOffset(),
                archive.archiveSize(), archive.archiveCleanupEndOffset(), archive.archiveCleanupSize(), List.of());
        }
    }
    // AutoMQ inject end

    public boolean isObjectExist(long objectId) {
        try (Image image = getImage()) {
            final S3ObjectsImage objectsImage = image.objectsMetadata();

            S3Object object = objectsImage.getObjectMetadata(objectId);
            if (object == null) {
                return false;
            }
            return object.getS3ObjectState() == S3ObjectState.COMMITTED;
        }
    }

    public int getObjectsCount() {
        try (Image image = getImage()) {
            return image.objectsMetadata().objectsCount();
        }
    }

    public synchronized StreamMetadataListener.Handle addMetadataListener(long streamId, StreamMetadataListener listener) {
        streamMetadataListeners.put(streamId, listener);
        List<StreamMetadata> list = getStreamMetadataList(List.of(streamId));
        if (!list.isEmpty()) {
            listener.onNewStreamMetadata(list.get(0));
        }
        return () -> streamMetadataListeners.remove(streamId, listener);
    }

    private synchronized void notifyMetadataListeners(Set<Long> changedStreams) {
        changedStreams.forEach(streamId -> {
            StreamMetadataListener listener = streamMetadataListeners.get(streamId);
            if (listener != null) {
                List<StreamMetadata> list = getStreamMetadataList(List.of(streamId));
                if (!list.isEmpty()) {
                    listener.onNewStreamMetadata(list.get(0));
                }
            }
        });
    }

    // must access thread safe
    private CompletableFuture<Void> pendingFetch() {
        GetObjectsTask task = new GetObjectsTask();
        synchronized (pendingGetObjectsTasks) {
            pendingGetObjectsTasks.add(task);
        }
        return task.cf;
    }

    /**
     * Registers interest in the next metadata Image, allowing a stale Archive task to stop while
     * ensuring normal metadata publication refreshes its local view before a later cycle.
     */
    public void refreshOnNextUpdate() {
        pendingFetch();
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

    /**
     * After use, the caller must call {@link Image#close()} to release the image.
     */
    private synchronized Image getImage() {
        return new Image(metadataImage);
    }

    static class GetObjectsTask {

        private final CompletableFuture<Void> cf;

        public GetObjectsTask() {
            this.cf = new CompletableFuture<>();
        }
    }

    private static class Image implements AutoCloseable {
        private final MetadataImage image;

        public Image(MetadataImage image) {
            this.image = image;
            image.retain();
        }

        public S3StreamsMetadataImage streamsMetadata() {
            return image.streamsMetadata();
        }

        public S3ObjectsImage objectsMetadata() {
            return image.objectsMetadata();
        }

        @Override
        public void close() {
            image.release();
        }
    }

    private static class DefaultRangeGetter implements S3StreamsMetadataImage.RangeGetter {
        private final S3ObjectsImage objectsImage;
        private final ObjectReaderFactory objectReaderFactory;

        public DefaultRangeGetter(S3ObjectsImage objectsImage,
            ObjectReaderFactory objectReaderFactory) {
            this.objectsImage = objectsImage;
            this.objectReaderFactory = objectReaderFactory;
        }

        @Override
        public CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId) {
            S3Object s3Object = objectsImage.getObjectMetadata(objectId);
            if (s3Object == null) {
                return FutureUtil.failedFuture(new IllegalArgumentException("Cannot find object metadata for object: " + objectId));
            }
            // The reader will be release after the find operation
            @SuppressWarnings("resource")
            ObjectReader reader = objectReaderFactory.get(new S3ObjectMetadata(objectId, s3Object.getObjectSize(), s3Object.getAttributes()));
            CompletableFuture<Optional<StreamOffsetRange>> cf = reader.basicObjectInfo().thenApply(info -> info.indexBlock().findStreamOffsetRange(streamId));
            cf.whenComplete((rst, ex) -> reader.release());
            return cf;
        }

        @Override
        public CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId) {
            ObjectStorage storage = objectReaderFactory.getObjectStorage();
            return storage.read(new ReadOptions().bucket(ObjectAttributes.MATCH_ALL_BUCKET), ObjectUtils.genIndexKey(0, nodeId));
        }
    }
}
