/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.index;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;

public class LocalStreamRangeIndexCache implements S3StreamClient.StreamLifeCycleListener {
    private static final short VERSION = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStreamRangeIndexCache.class);
    private static final int COMPACT_NUM = Systems.getEnvInt("AUTOMQ_STREAM_RANGE_INDEX_COMPACT_NUM", 3);
    public static final int MAX_INDEX_SIZE = Systems.getEnvInt("AUTOMQ_STREAM_RANGE_INDEX_MAX_SIZE", 1024 * 1024);
    private static final int DEFAULT_UPLOAD_CACHE_ON_STREAM_CLOSE_INTERVAL_MS = 5000;
    private final Map<Long, SparseRangeIndex> streamRangeIndexMap = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("upload-index", true));
    private final Queue<CompletableFuture<Void>> uploadQueue = new LinkedList<>();
    private final CompletableFuture<Void> initCf = new CompletableFuture<>();
    private final AtomicBoolean pruned = new AtomicBoolean(false);
    private long nodeId = -1;
    private ObjectStorage objectStorage;
    private int totalSize = 0;
    private CompletableFuture<Void> uploadCf = CompletableFuture.completedFuture(null);
    private long lastUploadTime = 0L;

    public LocalStreamRangeIndexCache() {
        S3StreamMetricsManager.registerLocalStreamRangeIndexCacheSizeSupplier(this::totalSize);
        S3StreamMetricsManager.registerLocalStreamRangeIndexCacheStreamNumSupplier(() -> {
            readLock.lock();
            try {
                return streamRangeIndexMap.size();
            } finally {
                readLock.unlock();
            }
        });
    }

    public void start() {
        exec(() -> {
            this.batchUpload();
            return null;
        });
    }

    public int totalSize() {
        readLock.lock();
        try {
            return totalSize;
        } finally {
            readLock.unlock();
        }
    }

    public synchronized CompletableFuture<Void> uploadOnStreamClose() {
        long now = System.currentTimeMillis();
        if (now - lastUploadTime > DEFAULT_UPLOAD_CACHE_ON_STREAM_CLOSE_INTERVAL_MS) {
            uploadCf = upload();
            lastUploadTime = now;
            LOGGER.info("Upload local index cache on stream close");
        }
        return uploadCf.orTimeout(1, TimeUnit.SECONDS);
    }

    CompletableFuture<Void> initCf() {
        return initCf;
    }

    // test only
    Map<Long, SparseRangeIndex> getStreamRangeIndexMap() {
        return streamRangeIndexMap;
    }

    public CompletableFuture<Void> upload() {
        synchronized (uploadQueue) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            uploadQueue.add(cf);
            return cf;
        }
    }

    private void batchUpload() {
        batchUpload0().whenComplete((v, ex) -> {
            executorService.schedule(this::batchUpload, 10, TimeUnit.MILLISECONDS);
        });
    }

    private CompletableFuture<Void> batchUpload0() {
        List<CompletableFuture<Void>> candidates;
        synchronized (uploadQueue) {
            if (uploadQueue.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            candidates = new ArrayList<>(uploadQueue);
            uploadQueue.clear();
        }
        return flush().whenComplete((v, ex) -> {
            for (CompletableFuture<Void> cf : candidates) {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    cf.complete(null);
                }
            }
        });
    }

    private CompletableFuture<Void> flush() {
        return execCompose(this::flush0);
    }

    private CompletableFuture<Void> flush0() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        ByteBuf buf = null;
        readLock.lock();
        try {
            if (streamRangeIndexMap.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            buf = toBuffer(streamRangeIndexMap);
            objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, ObjectUtils.genIndexKey(0, nodeId), buf)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        cf.completeExceptionally(ex);
                        return;
                    }
                    cf.complete(null);
                });
        } catch (Throwable t) {
            if (buf != null) {
                buf.release();
            }
            cf.completeExceptionally(t);
        } finally {
            readLock.unlock();
        }
        cf.whenComplete((v, ex) -> {
            if (ex != null) {
                LOGGER.error("Failed to flush index to object storage", ex);
            }
        });
        return cf;
    }

    public void init(int nodeId, ObjectStorage objectStorage) {
        writeLock.lock();
        try {
            this.nodeId = nodeId;
            this.objectStorage = objectStorage;
            this.objectStorage.read(new ReadOptions().bucket(ObjectAttributes.MATCH_ALL_BUCKET),
                    ObjectUtils.genIndexKey(0, nodeId))
                .whenComplete((data, ex) -> {
                    if (ex != null) {
                        // cache not found
                        initCf.complete(null);
                        LOGGER.info("Sparse index not found for node {}", nodeId);
                        return;
                    }
                    writeLock.lock();
                    try {
                        for (Map.Entry<Long, List<RangeIndex>> entry : LocalStreamRangeIndexCache.fromBuffer(data).entrySet()) {
                            this.streamRangeIndexMap.put(entry.getKey(), new SparseRangeIndex(COMPACT_NUM, entry.getValue()));
                            this.totalSize += entry.getValue().size() * RangeIndex.OBJECT_SIZE;
                        }
                    } finally {
                        writeLock.unlock();
                    }
                    data.release();
                    initCf.complete(null);
                    LOGGER.info("Loaded sparse index from object storage for {} streams at node {}", streamRangeIndexMap.size(), nodeId);
                });
        } finally {
            writeLock.unlock();
        }
    }

    private <T> CompletableFuture<T> exec(Callable<T> r) {
        return initCf.thenApply(v -> {
            try {
                return r.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private <T> CompletableFuture<T> execCompose(Callable<CompletableFuture<T>> r) {
        return initCf.thenCompose(v -> {
            try {
                return r.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void clear() {
        writeLock.lock();
        try {
            streamRangeIndexMap.clear();
            totalSize = 0;
        } finally {
            writeLock.unlock();
        }
    }

    public long nodeId() {
        readLock.lock();
        try {
            return nodeId;
        } finally {
            readLock.unlock();
        }
    }

    public CompletableFuture<Void> append(Map<Long, Optional<RangeIndex>> rangeIndexMap) {
        return exec(() -> {
            writeLock.lock();
            try {
                for (Map.Entry<Long, Optional<RangeIndex>> entry : rangeIndexMap.entrySet()) {
                    long streamId = entry.getKey();
                    Optional<RangeIndex> rangeIndex = entry.getValue();
                    rangeIndex.ifPresent(index -> totalSize += streamRangeIndexMap.computeIfAbsent(streamId,
                        k -> new SparseRangeIndex(COMPACT_NUM)).append(index));
                }
                evictIfNecessary();
            } finally {
                writeLock.unlock();
            }
            return null;
        });
    }

    private void evictIfNecessary() {
        if (totalSize <= MAX_INDEX_SIZE) {
            return;
        }
        boolean evicted;
        boolean hasSufficientIndex = true;
        List<Map.Entry<Long, SparseRangeIndex>> streamRangeIndexList = new ArrayList<>(streamRangeIndexMap.entrySet());
        Collections.shuffle(streamRangeIndexList);
        while (totalSize > MAX_INDEX_SIZE) {
            // try to evict from each stream in round-robin manner
            evicted = false;
            for (Map.Entry<Long, SparseRangeIndex> entry : streamRangeIndexList) {
                long streamId = entry.getKey();
                SparseRangeIndex sparseRangeIndex = entry.getValue();
                if (sparseRangeIndex.length() <= 1 + COMPACT_NUM && hasSufficientIndex) {
                    // skip evict if there is still sufficient stream to be evicted
                    continue;
                }
                totalSize -= sparseRangeIndex.evictOnce();
                evicted = true;
                if (sparseRangeIndex.length() == 0) {
                    streamRangeIndexMap.remove(streamId);
                }
                if (totalSize <= MAX_INDEX_SIZE) {
                    break;
                }
            }
            if (!evicted) {
                hasSufficientIndex = false;
            }
        }
    }

    public CompletableFuture<Void> compact(Map<Long, Optional<RangeIndex>> rangeIndexMap, Set<Long> compactedObjectIds) {
        return exec(() -> {
            writeLock.lock();
            try {
                if (rangeIndexMap == null || rangeIndexMap.isEmpty()) {
                    Iterator<Map.Entry<Long, SparseRangeIndex>> iterator = streamRangeIndexMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, SparseRangeIndex> entry = iterator.next();
                        totalSize += entry.getValue().compact(null, compactedObjectIds);
                        if (entry.getValue().length() == 0) {
                            iterator.remove();
                        }
                    }
                    return null;
                }
                for (Map.Entry<Long, Optional<RangeIndex>> entry : rangeIndexMap.entrySet()) {
                    long streamId = entry.getKey();
                    Optional<RangeIndex> rangeIndex = entry.getValue();
                    streamRangeIndexMap.compute(streamId, (k, v) -> {
                        if (v == null) {
                            v = new SparseRangeIndex(COMPACT_NUM);
                        }
                        totalSize += v.compact(rangeIndex.orElse(null), compactedObjectIds);
                        if (v.length() == 0) {
                            // remove stream with empty index
                            return null;
                        }
                        return v;
                    });
                }
            } finally {
                writeLock.unlock();
            }
            return null;
        });
    }

    public CompletableFuture<Void> updateIndexFromRequest(CommitStreamSetObjectRequest request) {
        Map<Long, Optional<RangeIndex>> rangeIndexMap = getRangeIndexMapFromRequest(request);
        if (request.getCompactedObjectIds().isEmpty()) {
            return append(rangeIndexMap);
        }
        return compact(rangeIndexMap, new HashSet<>(request.getCompactedObjectIds())).thenCompose(v -> upload());
    }

    /**
     * Create a map from stream id to the new range index from the request. An empty range index option indicates that the
     * stream set object the stream belongs to is split into stream objects and should be simply be removed from cache.
     *
     * @param request the commit stream set object request
     * @return the map from stream id to the new range index
     */
    private Map<Long, Optional<RangeIndex>> getRangeIndexMapFromRequest(CommitStreamSetObjectRequest request) {
        Map<Long, Optional<RangeIndex>> rangeIndexMap = new HashMap<>();
        for (ObjectStreamRange range : request.getStreamRanges()) {
            RangeIndex newRangeIndex = null;
            if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
                newRangeIndex = new RangeIndex(range.getStartOffset(), range.getEndOffset(), request.getObjectId());
            }
            rangeIndexMap.put(range.getStreamId(), Optional.ofNullable(newRangeIndex));
        }
        if (!request.getCompactedObjectIds().isEmpty()) {
            for (StreamObject streamObject : request.getStreamObjects()) {
                rangeIndexMap.putIfAbsent(streamObject.getStreamId(), Optional.empty());
            }
        }
        return rangeIndexMap;
    }

    public static ByteBuf toBuffer(Map<Long, SparseRangeIndex> streamRangeIndexMap) {
        AtomicInteger streamNumToWrite = new AtomicInteger(0);
        int capacity = bufferSize(streamRangeIndexMap, streamNumToWrite);
        ByteBuf buffer = ByteBufAlloc.byteBuffer(capacity);
        try {
            buffer.writeShort(VERSION);
            buffer.writeInt(streamNumToWrite.get());
            streamRangeIndexMap.forEach((streamId, sparseRangeIndex) -> {
                if (sparseRangeIndex == null || sparseRangeIndex.length() == 0) {
                    return;
                }
                buffer.writeLong(streamId);
                buffer.writeInt(sparseRangeIndex.getRangeIndexList().size());
                sparseRangeIndex.getRangeIndexList().forEach(rangeIndex -> {
                    buffer.writeLong(rangeIndex.getStartOffset());
                    buffer.writeLong(rangeIndex.getEndOffset());
                    buffer.writeLong(rangeIndex.getObjectId());
                });
            });
        } catch (Throwable t) {
            buffer.release();
            throw t;
        }
        return buffer;
    }

    private static int bufferSize(Map<Long, SparseRangeIndex> streamRangeIndexMap, AtomicInteger streamNumToWrite) {
        return Short.BYTES // version
            + Integer.BYTES // stream num
            + streamRangeIndexMap.values().stream().mapToInt(index -> {
                if (index == null || index.length() == 0) {
                    return 0;
                }
                streamNumToWrite.incrementAndGet();
                return Long.BYTES // stream id
                    + Integer.BYTES // range index num
                    + index.getRangeIndexList().size() * (3 * Long.BYTES);
            }).sum();
    }

    public static Map<Long, List<RangeIndex>> fromBuffer(ByteBuf data) {
        Map<Long, List<RangeIndex>> rangeIndexMap = new HashMap<>();
        short version = data.readShort();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unrecognized version: " + version);
        }
        int streamNum = data.readInt();
        for (int i = 0; i < streamNum; i++) {
            long streamId = data.readLong();
            int rangeIndexNum = data.readInt();
            for (int j = 0; j < rangeIndexNum; j++) {
                long startOffset = data.readLong();
                long endOffset = data.readLong();
                long objectId = data.readLong();
                rangeIndexMap.computeIfAbsent(streamId, k -> new ArrayList<>())
                    .add(new RangeIndex(startOffset, endOffset, objectId));
            }
        }
        return rangeIndexMap;
    }

    /**
     * Search for the object with the maximum start offset less than or equal to the given start offset.
     * If not found, return -1.
     */
    public CompletableFuture<Long> searchObjectId(long streamId, long startOffset) {
        return exec(() -> {
            readLock.lock();
            try {
                SparseRangeIndex sparseRangeIndex = streamRangeIndexMap.get(streamId);
                if (sparseRangeIndex == null) {
                    return -1L;
                }
                return LocalStreamRangeIndexCache.binarySearchObjectId(startOffset, sparseRangeIndex.getRangeIndexList());
            } finally {
                readLock.unlock();
            }
        });
    }

    public CompletableFuture<Void> asyncPrune(Supplier<Set<Long>> initStreamSetObjectIdsSupplier) {
        if (pruned.compareAndSet(false, true)) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            executorService.execute(() -> prune(initStreamSetObjectIdsSupplier).whenComplete((v, ex) -> {
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    cf.complete(null);
                }
            }));
            return cf;
        }
        return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> prune(Supplier<Set<Long>> initStreamSetObjectIdsSupplier) {
        if (initStreamSetObjectIdsSupplier == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("initStreamSetObjectIdsSupplier cannot be null"));
        }
        return execCompose(() -> {
            writeLock.lock();
            try {
                Set<Long> streamSetObjectIds = initStreamSetObjectIdsSupplier.get();
                Iterator<Map.Entry<Long, SparseRangeIndex>> iterator = streamRangeIndexMap.entrySet().iterator();
                boolean pruned = false;
                while (iterator.hasNext()) {
                    Map.Entry<Long, SparseRangeIndex> entry = iterator.next();
                    SparseRangeIndex sparseRangeIndex = entry.getValue();
                    Set<Long> invalidateObjectIds = new HashSet<>();
                    for (RangeIndex rangeIndex : sparseRangeIndex.getRangeIndexList()) {
                        if (!streamSetObjectIds.contains(rangeIndex.getObjectId())) {
                            invalidateObjectIds.add(rangeIndex.getObjectId());
                        }
                    }
                    if (invalidateObjectIds.isEmpty()) {
                        continue;
                    }
                    totalSize += sparseRangeIndex.compact(null, invalidateObjectIds);
                    if (sparseRangeIndex.length() == 0) {
                        iterator.remove();
                    }
                    pruned = true;
                }
                return pruned ? upload() : CompletableFuture.completedFuture(null);
            } catch (Throwable t) {
                LOGGER.error("Failed to prune local sparse index, clear all", t);
                clear();
                return CompletableFuture.failedFuture(t);
            } finally {
                writeLock.unlock();
            }
        });
    }

    public static long binarySearchObjectId(long startOffset, List<RangeIndex> rangeIndexList) {
        if (rangeIndexList == null || rangeIndexList.isEmpty()) {
            return -1L;
        }
        int index = Collections.binarySearch(rangeIndexList, new RangeIndex(startOffset, 0, 0));
        index = index < 0 ? -index - 2 : index;
        if (index < 0) {
            return -1L;
        }
        if (index >= rangeIndexList.size()) {
            return rangeIndexList.get(rangeIndexList.size() - 1).getObjectId();
        }
        return rangeIndexList.get(index).getObjectId();
    }

    @Override
    public void onStreamClose(long streamId) {
        upload();
    }
}
