/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.index;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.ThreadUtils;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStreamRangeIndexCache {
    private static final short VERSION = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStreamRangeIndexCache.class);
    private static final int COMPACT_NUM = Systems.getEnvInt("AUTOMQ_STREAM_RANGE_INDEX_COMPACT_NUM", 5);
    private static final int SPARSE_PADDING = Systems.getEnvInt("AUTOMQ_STREAM_RANGE_INDEX_SPARSE_PADDING", 1);
    private volatile static LocalStreamRangeIndexCache instance = null;
    private final Map<Long, SparseRangeIndex> streamRangeIndexMap = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("upload-index", true));
    private final Queue<CompletableFuture<Void>> uploadQueue = new LinkedList<>();
    private long nodeId = -1;
    private ObjectStorage objectStorage;
    private CompletableFuture<Void> initCf = new CompletableFuture<>();

    private LocalStreamRangeIndexCache() {
        executorService.scheduleAtFixedRate(this::batchUpload, 0, 10, TimeUnit.MILLISECONDS);
        executorService.scheduleAtFixedRate(this::flush, 0, 1, TimeUnit.MINUTES);
    }

    public static LocalStreamRangeIndexCache getInstance() {
        if (instance == null) {
            synchronized (LocalStreamRangeIndexCache.class) {
                if (instance == null) {
                    instance = new LocalStreamRangeIndexCache();
                }
            }
        }
        return instance;
    }

    // for test
    void reset() {
        writeLock.lock();
        try {
            streamRangeIndexMap.clear();
            initCf = new CompletableFuture<>();
        } finally {
            writeLock.unlock();
        }
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
        List<CompletableFuture<Void>> candidates;
        synchronized (uploadQueue) {
            if (uploadQueue.isEmpty()) {
                return;
            }
            candidates = new ArrayList<>(uploadQueue);
            uploadQueue.clear();
        }
        flush().whenComplete((v, ex) -> {
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
        CompletableFuture<Void> cf = new CompletableFuture<>();
        readLock.lock();
        try {
            objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, ObjectUtils.genIndexKey(0, nodeId), toBuffer(streamRangeIndexMap))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOGGER.error("Upload index failed", ex);
                        cf.completeExceptionally(ex);
                        return;
                    }
                    cf.complete(null);
                });
        } finally {
            readLock.unlock();
        }
        return cf;
    }

    public void init(int nodeId, ObjectStorage objectStorage) {
        writeLock.lock();
        try {
            this.nodeId = nodeId;
            this.objectStorage = objectStorage;
            this.objectStorage.read(ObjectStorage.ReadOptions.DEFAULT.bucket(this.objectStorage.bucketId()),
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
                            this.streamRangeIndexMap.put(entry.getKey(), new SparseRangeIndex(COMPACT_NUM, SPARSE_PADDING, entry.getValue()));
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

    public void clear() {
        writeLock.lock();
        try {
            streamRangeIndexMap.clear();
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

    public CompletableFuture<Void> append(Map<Long, RangeIndex> rangeIndexMap) {
        return exec(() -> {
            writeLock.lock();
            try {
                for (Map.Entry<Long, RangeIndex> entry : rangeIndexMap.entrySet()) {
                    long streamId = entry.getKey();
                    RangeIndex rangeIndex = entry.getValue();
                    streamRangeIndexMap.computeIfAbsent(streamId,
                        k -> new SparseRangeIndex(COMPACT_NUM, SPARSE_PADDING)).append(rangeIndex);
                }
            } finally {
                writeLock.unlock();
            }
            return null;
        });
    }

    public CompletableFuture<Void> compact(Map<Long, RangeIndex> rangeIndexMap, Set<Long> compactedObjectIds) {
        return exec(() -> {
            writeLock.lock();
            try {
                if (rangeIndexMap == null || rangeIndexMap.isEmpty()) {
                    Iterator<Map.Entry<Long, SparseRangeIndex>> iterator = streamRangeIndexMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, SparseRangeIndex> entry = iterator.next();
                        entry.getValue().compact(null, compactedObjectIds);
                        if (entry.getValue().size() == 0) {
                            iterator.remove();
                        }
                    }
                    return null;
                }
                for (Map.Entry<Long, RangeIndex> entry : rangeIndexMap.entrySet()) {
                    long streamId = entry.getKey();
                    RangeIndex rangeIndex = entry.getValue();
                    streamRangeIndexMap.compute(streamId, (k, v) -> {
                        if (v == null) {
                            v = new SparseRangeIndex(COMPACT_NUM, SPARSE_PADDING);
                        }
                        v.compact(rangeIndex, compactedObjectIds);
                        if (v.size() == 0) {
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
        Map<Long, RangeIndex> rangeIndexMap = new HashMap<>();
        for (ObjectStreamRange range : request.getStreamRanges()) {
            RangeIndex newRangeIndex = null;
            if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
                newRangeIndex = new RangeIndex(range.getStartOffset(), range.getEndOffset(), request.getObjectId());
            }
            rangeIndexMap.put(range.getStreamId(), newRangeIndex);
        }
        if (request.getCompactedObjectIds().isEmpty()) {
            return append(rangeIndexMap);
        }
        return compact(rangeIndexMap, new HashSet<>(request.getCompactedObjectIds()));
    }

    public static ByteBuf toBuffer(Map<Long, SparseRangeIndex> streamRangeIndexMap) {
        int capacity = Short.BYTES // version
            + Integer.BYTES // stream num
            + streamRangeIndexMap.values().stream().mapToInt(index -> Long.BYTES // stream id
            + Integer.BYTES // range index num
            + index.getRangeIndexList().size() * RangeIndex.SIZE).sum();
        ByteBuf buffer = ByteBufAlloc.byteBuffer(capacity);
        buffer.writeShort(VERSION);
        buffer.writeInt(streamRangeIndexMap.size());
        streamRangeIndexMap.forEach((streamId, sparseRangeIndex) -> {
            buffer.writeLong(streamId);
            buffer.writeInt(sparseRangeIndex.getRangeIndexList().size());
            sparseRangeIndex.getRangeIndexList().forEach(rangeIndex -> {
                buffer.writeLong(rangeIndex.getStartOffset());
                buffer.writeLong(rangeIndex.getEndOffset());
                buffer.writeLong(rangeIndex.getObjectId());
            });
        });
        return buffer;
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
}
