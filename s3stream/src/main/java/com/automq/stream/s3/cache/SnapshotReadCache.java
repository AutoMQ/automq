package com.automq.stream.s3.cache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.blockcache.EventLoopSafe;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.threads.EventLoop;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SnapshotReadCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadCache.class);
    private static final SnapshotReadCache INSTANCE = new SnapshotReadCache();
    private static final long MAX_INFLIGHT_LOAD_BYTES = 100L * 1024 * 1024;
    private final AtomicLong inflightLoadBytes = new AtomicLong();
    private final Queue<ObjectLoadTask> waitingLoadingTasks = new LinkedList<>();
    private final Queue<ObjectLoadTask> loadingTasks = new LinkedList<>();
    private final Map<Long, AtomicLong> streamNextOffsets = new HashMap<>();
    private final Cache<Long /* streamId */, Boolean> activeStreams;
    private final EventLoop eventLoop = new EventLoop("SNAPSHOT_READ_CACHE");
    private LogCache cache;
    private ObjectStorage objectStorage;

    public static SnapshotReadCache instance() {
        return INSTANCE;
    }

    public SnapshotReadCache() {
        activeStreams = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .removalListener((RemovalListener<Long, Boolean>) notification ->
                eventLoop.execute(() -> clearStream(notification.getKey())))
            .build();
    }

    public void setup(LogCache cache, ObjectStorage objectStorage) {
        this.cache = cache;
        this.objectStorage = objectStorage;
    }

    @EventLoopSafe
    public void put(CloseableIterator<StreamRecordBatch> it) {
        try (it) {
            LogCache cache = this.cache;
            if (cache == null) {
                it.forEachRemaining(StreamRecordBatch::release);
                return;
            }
            long streamId = -1L;
            AtomicLong expectedNextOffset = null;
            while (it.hasNext()) {
                StreamRecordBatch batch = it.next();
                long newStreamId = batch.getStreamId();
                if (streamId == -1L || newStreamId != streamId) {
                    streamId = newStreamId;
                    expectedNextOffset = streamNextOffsets.computeIfAbsent(streamId, k -> new AtomicLong(batch.getBaseOffset()));
                    activeStream(streamId);
                }
                if (batch.getBaseOffset() < expectedNextOffset.get()) {
                    batch.release();
                    continue;
                } else if (batch.getBaseOffset() > expectedNextOffset.get()) {
                    // The LogCacheBlock doesn't accept discontinuous record batches.
                    cache.clearStreamRecords(streamId);
                }
                if (cache.put(batch)) {
                    // the block is full
                    LogCache.LogCacheBlock cacheBlock = cache.archiveCurrentBlock();
                    cache.markFree(cacheBlock);
                }
                expectedNextOffset.set(batch.getLastOffset());
            }
        }
    }

    public synchronized CompletableFuture<Void> load(List<S3ObjectMetadata> objects) {
        if (objects.isEmpty()) {
            throw new IllegalArgumentException("The objects is an empty list");
        }
        CompletableFuture<Void> cf = new CompletableFuture<>();
        eventLoop.execute(() -> {
            ObjectLoadTask task = null;
            for (S3ObjectMetadata object : objects) {
                task = new ObjectLoadTask(ObjectReader.reader(object, objectStorage));
                waitingLoadingTasks.add(task);
            }
            if (task != null) {
                FutureUtil.propagate(task.cf, cf);
            }
            tryLoad();
        });
        return cf;
    }

    @EventLoopSafe
    private void clearStream(Long streamId) {
        cache.clearStreamRecords(streamId);
        streamNextOffsets.remove(streamId);
    }

    private void activeStream(long streamId) {
        try {
            activeStreams.get(streamId, () -> true);
        } catch (ExecutionException e) {
            // suppress
        }
    }

    @EventLoopSafe
    private void tryLoad() {
        for (; ; ) {
            if (inflightLoadBytes.get() >= MAX_INFLIGHT_LOAD_BYTES) {
                break;
            }
            ObjectLoadTask task = waitingLoadingTasks.peek();
            if (task == null) {
                break;
            }
            waitingLoadingTasks.poll();
            loadingTasks.add(task);
            task.run();
            inflightLoadBytes.addAndGet(task.reader.metadata().objectSize());
        }
    }

    @EventLoopSafe
    private void tryPutIntoCache() {
        for (; ; ) {
            ObjectLoadTask task = loadingTasks.peek();
            if (task == null) {
                break;
            }
            if (task.putIntoCache()) {
                loadingTasks.poll();
                task.close();
                inflightLoadBytes.addAndGet(-task.reader.metadata().objectSize());
                tryLoad();
            } else {
                break;
            }
        }
    }

    class ObjectLoadTask {
        final ObjectReader reader;
        final CompletableFuture<Void> cf;
        Queue<CompletableFuture<ObjectReader.DataBlockGroup>> blocks;

        public ObjectLoadTask(ObjectReader reader) {
            this.reader = reader;
            this.cf = new CompletableFuture<>();
        }

        public void run() {
            reader.basicObjectInfo().thenAcceptAsync(info -> {
                List<DataBlockIndex> blockIndexList = info.indexBlock().indexes();
                LinkedList<CompletableFuture<ObjectReader.DataBlockGroup>> blocks = new LinkedList<>();
                blockIndexList.forEach(blockIndex -> {
                    CompletableFuture<ObjectReader.DataBlockGroup> blockCf = reader.read(blockIndex);
                    blocks.add(blockCf);
                    blockCf.whenCompleteAsync((group, t) -> {
                        if (t != null) {
                            LOGGER.error("Failed to load object blocks {}", reader.metadata(), t);
                        }
                        tryPutIntoCache();
                    }, eventLoop);
                });
                this.blocks = blocks;
            }, eventLoop).exceptionally(ex -> {
                LOGGER.error("Failed to load object {}", reader.metadata(), ex);
                cf.complete(null);
                return null;
            });
        }

        /**
         * Put blocks' records into cache
         *
         * @return true if all the data is put into cache, false otherwise
         */
        @EventLoopSafe
        public boolean putIntoCache() {
            if (blocks == null) {
                return false;
            }
            for (; ; ) {
                CompletableFuture<ObjectReader.DataBlockGroup> blockCf = blocks.peek();
                if (blockCf == null) {
                    cf.complete(null);
                    return true;
                }
                if (!blockCf.isDone()) {
                    return false;
                }
                if (blockCf.isCompletedExceptionally() || blockCf.isCancelled()) {
                    blocks.poll();
                    continue;
                }
                try (ObjectReader.DataBlockGroup block = blockCf.join()) {
                    put(block.iterator());
                }
                blocks.poll();
            }
        }

        public void close() {
            reader.close();
        }
    }

}
