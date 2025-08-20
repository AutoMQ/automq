package com.automq.stream.s3.cache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.automq.stream.utils.threads.EventLoopSafe;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

public class SnapshotReadCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadCache.class);
    private static final long MAX_INFLIGHT_LOAD_BYTES = 100L * 1024 * 1024;

    private static final Metrics.HistogramBundle OPERATION_LATENCY = Metrics.instance().histogram("kafka_stream_snapshot_read_cache", "Snapshot read cache operation latency", "nanoseconds");
    private static final DeltaHistogram REPLAY_LATENCY = OPERATION_LATENCY.histogram(MetricsLevel.INFO, Attributes.of(AttributeKey.stringKey("operation"), "replay"));
    private static final DeltaHistogram READ_WAL_LATENCY = OPERATION_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "read_wal"));
    private static final DeltaHistogram DECODE_LATENCY = OPERATION_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "decode"));
    private static final DeltaHistogram PUT_INTO_CACHE_LATENCY = OPERATION_LATENCY.histogram(MetricsLevel.DEBUG, Attributes.of(AttributeKey.stringKey("operation"), "put_into_cache"));

    private final Map<Long, AtomicLong> streamNextOffsets = new HashMap<>();
    private final Cache<Long /* streamId */, Boolean> activeStreams;
    private final EventLoop eventLoop = new EventLoop("SNAPSHOT_READ_CACHE");
    private final LogCacheBlockFreeListener cacheFreeListener = new LogCacheBlockFreeListener();

    private final ObjectReplay objectReplay = new ObjectReplay();
    private final WalReplay walReplay = new WalReplay();
    private final StreamManager streamManager;
    private final LogCache cache;
    private final ObjectStorage objectStorage;
    private final Function<StreamRecordBatch, CompletableFuture<StreamRecordBatch>> linkRecordDecoder;
    private final Time time = Time.SYSTEM;

    public SnapshotReadCache(StreamManager streamManager, LogCache cache, ObjectStorage objectStorage, Function<StreamRecordBatch, CompletableFuture<StreamRecordBatch>> linkRecordDecoder) {
        activeStreams = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .removalListener((RemovalListener<Long, Boolean>) notification ->
                eventLoop.execute(() -> clearStream(notification.getKey())))
            .build();
        this.streamManager = streamManager;
        this.cache = cache;
        this.objectStorage = objectStorage;
        this.linkRecordDecoder = linkRecordDecoder;
    }

    @EventLoopSafe
    void put(CloseableIterator<StreamRecordBatch> it) {
        long startNanos = time.nanoseconds();
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
                    cacheBlock.addFreeListener(cacheFreeListener);
                    cache.markFree(cacheBlock);
                }
                expectedNextOffset.set(batch.getLastOffset());
            }
        }
        PUT_INTO_CACHE_LATENCY.record(time.nanoseconds() - startNanos);
    }

    public synchronized CompletableFuture<Void> replay(List<S3ObjectMetadata> objects) {
        return objectReplay.replay(objects);
    }

    public synchronized CompletableFuture<Void> replay(WriteAheadLog confirmWAL, RecordOffset startOffset, RecordOffset endOffset) {
        long startNanos = time.nanoseconds();
        return walReplay.replay(confirmWAL, startOffset, endOffset)
            .whenComplete((nil, ex) -> REPLAY_LATENCY.record(time.nanoseconds() - startNanos));
    }

    public void addEventListener(EventListener eventListener) {
        cacheFreeListener.addListener(eventListener);
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

    class WalReplay {
        // soft limit the inflight memory
        private final Semaphore inflightLimiter = new Semaphore(Systems.CPU_CORES * 4);
        private final Queue<WalReplayTask> waitingLoadTasks = new ConcurrentLinkedQueue<>();
        private final Queue<WalReplayTask> loadingTasks = new ConcurrentLinkedQueue<>();

        public CompletableFuture<Void> replay(WriteAheadLog wal, RecordOffset startOffset, RecordOffset endOffset) {
            inflightLimiter.acquireUninterruptibly();
            WalReplayTask task = new WalReplayTask(wal, startOffset, endOffset);
            waitingLoadTasks.add(task);
            eventLoop.submit(this::tryLoad);
            return task.replayCf.whenComplete((nil, ex) -> inflightLimiter.release());
        }

        @EventLoopSafe
        private void tryLoad() {
            for (; ; ) {
                WalReplayTask task = waitingLoadTasks.poll();
                if (task == null) {
                    break;
                }
                loadingTasks.add(task);
                task.run();
                task.loadCf.whenCompleteAsync((rst, ex) -> tryPutIntoCache(), eventLoop);
            }
        }

        @EventLoopSafe
        private void tryPutIntoCache() {
            for (; ; ) {
                WalReplayTask task = loadingTasks.peek();
                if (task == null || !task.loadCf.isDone()) {
                    break;
                }
                loadingTasks.poll();
                put(CloseableIterator.wrap(task.records.iterator()));
                task.replayCf.complete(null);
            }
        }

    }

    class WalReplayTask {
        final WriteAheadLog wal;
        final RecordOffset startOffset;
        final RecordOffset endOffset;
        final CompletableFuture<Void> loadCf;
        final CompletableFuture<Void> replayCf = new CompletableFuture<>();
        final List<StreamRecordBatch> records = new ArrayList<>();

        public WalReplayTask(WriteAheadLog wal, RecordOffset startOffset, RecordOffset endOffset) {
            this.wal = wal;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.loadCf = new CompletableFuture<>();
            loadCf.whenComplete((rst, ex) -> {
                if (ex != null) {
                    LOGGER.error("Replay WAL [{}, {}) fail", startOffset, endOffset, ex);
                }
            });
        }

        public void run() {
            long startNanos = time.nanoseconds();
            wal.get(startOffset, endOffset).thenCompose(walRecords -> {
                long readWalDoneNanos = time.nanoseconds();
                READ_WAL_LATENCY.record(readWalDoneNanos - startNanos);
                List<CompletableFuture<StreamRecordBatch>> cfList = new ArrayList<>(walRecords.size());
                for (StreamRecordBatch walRecord : walRecords) {
                    if (walRecord.getCount() >= 0) {
                        cfList.add(CompletableFuture.completedFuture(walRecord));
                    } else {
                        cfList.add(linkRecordDecoder.apply(walRecord));
                    }
                }
                return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).whenComplete((rst, ex) -> {
                    DECODE_LATENCY.record(time.nanoseconds() - readWalDoneNanos);
                    if (ex != null) {
                        loadCf.completeExceptionally(ex);
                        // release other success record
                        cfList.forEach(cf -> cf.thenAccept(StreamRecordBatch::release));
                        return;
                    }
                    records.addAll(cfList.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                    // move to mem pool
                    records.forEach(StreamRecordBatch::encoded);
                    loadCf.complete(null);
                });
            }).whenComplete((rst, ex) -> {
                if (ex != null) {
                    loadCf.completeExceptionally(ex);
                }
            });
        }
    }

    class ObjectReplay {
        private final AtomicLong inflightLoadBytes = new AtomicLong();
        private final Queue<ObjectReplayTask> waitingLoadingTasks = new LinkedList<>();
        private final Queue<ObjectReplayTask> loadingTasks = new LinkedList<>();

        public synchronized CompletableFuture<Void> replay(List<S3ObjectMetadata> objects) {
            if (objects.isEmpty()) {
                throw new IllegalArgumentException("The objects is an empty list");
            }
            CompletableFuture<Void> cf = new CompletableFuture<>();
            eventLoop.execute(() -> {
                ObjectReplayTask task = null;
                for (S3ObjectMetadata object : objects) {
                    task = new ObjectReplayTask(ObjectReader.reader(object, objectStorage));
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
        private void tryLoad() {
            for (; ; ) {
                if (inflightLoadBytes.get() >= MAX_INFLIGHT_LOAD_BYTES) {
                    break;
                }
                ObjectReplayTask task = waitingLoadingTasks.peek();
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
                ObjectReplayTask task = loadingTasks.peek();
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
    }

    class ObjectReplayTask {
        final ObjectReader reader;
        final CompletableFuture<Void> cf;
        Queue<CompletableFuture<ObjectReader.DataBlockGroup>> blocks;

        public ObjectReplayTask(ObjectReader reader) {
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
                        objectReplay.tryPutIntoCache();
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

    class LogCacheBlockFreeListener implements LogCache.FreeListener {
        private final List<EventListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onFree(List<LogCache.StreamRangeBound> bounds) {
            List<Long> streamIdList = bounds.stream().map(LogCache.StreamRangeBound::streamId).collect(Collectors.toList());
            Map<Long, LogCache.StreamRangeBound> streamMap = bounds.stream().collect(Collectors.toMap(LogCache.StreamRangeBound::streamId, Function.identity()));
            List<StreamMetadata> streamMetadataList = streamManager.getStreams(streamIdList).join();
            Set<Integer> requestCommitNodes = new HashSet<>();
            for (StreamMetadata streamMetadata : streamMetadataList) {
                LogCache.StreamRangeBound bound = streamMap.get(streamMetadata.streamId());
                if (bound.endOffset() > streamMetadata.endOffset()) {
                    requestCommitNodes.add(streamMetadata.nodeId());
                }
            }
            listeners.forEach(listener ->
                requestCommitNodes.forEach(nodeId ->
                    FutureUtil.suppress(() -> listener.onEvent(new RequestCommitEvent(nodeId)), LOGGER)));
        }

        public void addListener(EventListener listener) {
            this.listeners.add(listener);
        }
    }

    public interface EventListener {
        void onEvent(Event event);
    }

    public interface Event {
    }

    public static class RequestCommitEvent implements Event {
        private final int nodeId;

        public RequestCommitEvent(int nodeId) {
            this.nodeId = nodeId;
        }

        public int nodeId() {
            return nodeId;
        }

        @Override
        public String toString() {
            return "RequestCommitEvent{" +
                "nodeId=" + nodeId +
                '}';
        }
    }

}
