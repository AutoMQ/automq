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

package kafka.automq.table.worker;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Errors;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.PartitionMetric;
import kafka.automq.table.events.TopicMetric;
import kafka.automq.table.events.WorkerOffset;
import kafka.cluster.Partition;
import kafka.cluster.PartitionAppendListener;
import kafka.log.UnifiedLog;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogSegment;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The worker manage the topic partitions iceberg sync.
 * <p>
 * There are two paths to sync the records to iceberg:
 * 1. Advance sync: The worker will sync the 'hot data' to the writer when the dirty bytes reach the threshold.
 * 2. Commit sync: The worker receive {@link CommitRequest}, sync the records [start offset, log end) to iceberg
 * and response the result.
 * <p>
 * Thread-Safe: the event loop sequences all operations on the worker.
 */
class TopicPartitionsWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionsWorker.class);
    private static final int WRITER_LIMIT = 1000;
    private static final AtomicLong SYNC_ID = new AtomicLong(0);

    private final Semaphore commitLimiter;

    private final String topic;
    private final String name;
    private final WorkerConfig config;

    Status status = Status.IDLE;

    private final List<RequestWrapper> commitRequests = new LinkedList<>();

    List<Writer> writers = new ArrayList<>();

    /**
     * The flag to indicate the worker need reset. The worker status(writers) will be reset in the next round run.
     */
    private boolean requireReset;

    private Map<Integer, Partition> partitions = new HashMap<>();

    private double avgRecordSize = Double.MIN_VALUE;
    private double decompressedRatio = 1.0;

    final Commit commit = new Commit();
    final AdvanceSync advanceSync = new AdvanceSync();

    private final EventLoop eventLoop;
    private final EventLoops eventLoops;
    private final ExecutorService flushExecutors;
    private final Channel channel;
    private final WriterFactory writerFactory;

    public TopicPartitionsWorker(String topic, WorkerConfig config, WriterFactory writerFactory, Channel channel,
        EventLoop eventLoop, EventLoops eventLoops, ExecutorService flushExecutors, Semaphore commitLimiter) {
        this.topic = topic;
        this.name = topic;
        this.config = config;
        this.eventLoop = eventLoop;
        this.eventLoops = eventLoops;
        this.channel = channel;
        this.writerFactory = writerFactory;
        this.flushExecutors = flushExecutors;
        this.commitLimiter = commitLimiter;
    }

    public void add(Partition partition) {
        Map<Integer, Partition> newPartitions = new HashMap<>(partitions);
        newPartitions.put(partition.partitionId(), partition);
        partition.addAppendListener(advanceSync);
        this.partitions = newPartitions;
        requireReset = true;
        LOGGER.info("[ADD_PARTITION],{},{}", name, partition.partitionId());
    }

    public void remove(Partition partition) {
        Map<Integer, Partition> newPartitions = new HashMap<>(partitions);
        newPartitions.remove(partition.partitionId());
        partition.removeAppendListener(advanceSync);
        this.partitions = newPartitions;
        requireReset = true;
        LOGGER.info("[REMOVE_PARTITION],{},{}", name, partition.partitionId());
    }

    public CompletableFuture<Void> commit(CommitRequest commitRequest) {
        RequestWrapper request = new RequestWrapper(commitRequest);
        commitRequests.add(request);
        run();
        return request.cf;
    }

    public Boolean isEmpty() {
        return partitions.isEmpty();
    }

    void run() {
        if (status != Status.IDLE) {
            return;
        }
        if (partitions.isEmpty()) {
            LOGGER.info("[WORKER_EMPTY],{}", topic);
            return;
        }
        if (requireReset) {
            writers.clear();
            requireReset = false;
        }
        try {
            if (commitRequests.isEmpty() && !commit.starving) {
                advanceSync.sync();
            } else {
                commit.commit();
            }
        } catch (Throwable e) {
            transitionTo(Status.IDLE);
            requireReset = true;
            LOGGER.error("[WORKER_RUN_FAIL],{}", topic, e);
        }
    }

    private boolean transitionTo(Status status) {
        if (this.status == status) {
            return true;
        }
        if (status == Status.IDLE) {
            this.status = status;
            commitLimiter.release();
            return true;
        }
        if (commitLimiter.tryAcquire()) {
            this.status = status;
            return true;
        } else {
            return false;
        }
    }

    private ReferenceHolder<Integer> tryAcquireMoreCommitQuota(int required) {
        for (; ; ) {
            int available = commitLimiter.availablePermits();
            int real = Math.min(available, required);
            if (commitLimiter.tryAcquire(real)) {
                return new ReferenceHolder<>(real, () -> commitLimiter.release(real));
            }
        }
    }

    /**
     * Remove commited writers and clear useless writer.
     */
    static void cleanupCommitedWriters(String topic, List<WorkerOffset> offsets,
        List<Writer> writers) throws IOException {
        int matchIndex = -1;
        for (int i = 0; i < writers.size(); i++) {
            if (isOffsetMatch(writers.get(i), offsets)) {
                matchIndex = i;
            }
        }
        // clear useless writer
        if (matchIndex == -1) {
            if (!writers.isEmpty()) {
                Writer lastWriter = writers.get(writers.size() - 1);
                if (!lastWriter.isCompleted()) {
                    writers.get(writers.size() - 1).abort();
                    LOGGER.info("[WRITER_NOT_MATCH],[RESET],{}", topic);
                }
            }
            writers.clear();
        } else {
            List<Writer> newWriters = new ArrayList<>(writers.subList(matchIndex, writers.size()));
            writers.clear();
            writers.addAll(newWriters);
        }
        cleanupEmptyWriter(writers);
    }

    static void cleanupEmptyWriter(List<Writer> writers) {
        int size = writers.size();
        Iterator<Writer> it = writers.iterator();
        while (size > 0 && it.hasNext()) {
            Writer writer = it.next();
            if (writer.isCompleted() && writer.results().isEmpty()) {
                it.remove();
                size--;
            }
        }
    }

    static boolean isOffsetMatch(Writer writer, List<WorkerOffset> request) {
        for (WorkerOffset workerOffset : request) {
            Writer.OffsetRange writerOffset = writer.getOffset(workerOffset.partition());
            if (writerOffset == null || writerOffset.start() != workerOffset.offset()) {
                return false;
            }
        }
        return true;
    }

    protected double getAvgRecordSize() {
        for (Map.Entry<Integer, Partition> entry : partitions.entrySet()) {
            Partition partition = entry.getValue();
            UnifiedLog log = partition.log().get();
            LogSegment segment = log.activeSegment();
            long recordCount = log.highWatermark() - segment.baseOffset();
            if (recordCount <= 0) {
                continue;
            }
            avgRecordSize = ((double) segment.size()) / recordCount;
        }
        return avgRecordSize;
    }

    protected double getDecompressedRatio() {
        return decompressedRatio;
    }

    protected void updateDecompressedRatio(double decompressedRatio) {
        this.decompressedRatio = decompressedRatio;
    }

    SyncTask newSyncTask(String type, Map<Integer, Long> startOffsets, long priority) {
        return new SyncTask(String.format("[%s-%s-%s]", type, topic, SYNC_ID.getAndIncrement()), partitions, startOffsets, getAvgRecordSize(), priority);
    }

    class SyncTask {
        private final String logContext;
        private final Map<Integer, Partition> partitions;
        private final Map<Integer, Long> startOffsets;
        private boolean hasMoreData;
        private ReferenceHolder<Integer> moreQuota;
        private final List<Writer> writers = new ArrayList<>();
        private final double avgRecordSize;
        private final double decompressedRatio;
        private final long priority;

        final List<MicroSyncBatchTask> microSyncBatchTasks = new ArrayList<>();

        public SyncTask(String logContext, Map<Integer, Partition> partitions, Map<Integer, Long> startOffsets,
            double avgRecordSize, long taskPriority) {
            this.logContext = logContext;
            this.partitions = partitions;
            this.startOffsets = new HashMap<>(startOffsets);
            this.avgRecordSize = avgRecordSize;
            this.priority = taskPriority;
            this.decompressedRatio = getDecompressedRatio();
        }

        public boolean hasMoreData() {
            return hasMoreData;
        }

        /**
         * Split sync task to parallel micro sync batch tasks.
         */
        // visible to test
        void plan() {
            Map<Integer, OffsetBound> partition2bound = new HashMap<>();
            for (Map.Entry<Integer, Long> entry : startOffsets.entrySet()) {
                int partitionId = entry.getKey();
                Partition partition = partitions.get(partitionId);
                OffsetBound offsetBound = new OffsetBound(partition, startOffsets.get(partitionId), partition.log().get().highWatermark());
                partition2bound.put(partitionId, offsetBound);
            }
            double totalDirtyBytes = partition2bound.values().stream().mapToLong(bound -> bound.end - bound.start).sum() * avgRecordSize * decompressedRatio;
            int parallel = Math.max((int) (Math.ceil(totalDirtyBytes / config.microSyncBatchSize())), 1);
            parallel = Math.min(Math.min(parallel, eventLoops.size()), partition2bound.size());
            moreQuota = tryAcquireMoreCommitQuota(parallel - 1);
            parallel = moreQuota.value() + 1;

            List<Integer> partitionSortedBySize = partition2bound.entrySet().stream().sorted((e1, e2) -> {
                long size1 = e1.getValue().end - e1.getValue().start;
                long size2 = e2.getValue().end - e2.getValue().start;
                return Long.compare(size2, size1);
            }).map(Map.Entry::getKey).collect(Collectors.toList());

            for (int i = 0; i < parallel; i++) {
                EventLoops.EventLoopRef workerEventLoop = eventLoops.leastLoadEventLoop();
                Writer taskWriter = writerFactory.newWriter();
                writers.add(taskWriter);
                PartitionWriteTaskContext ctx = new PartitionWriteTaskContext(taskWriter, workerEventLoop, flushExecutors, config, priority);
                microSyncBatchTasks.add(new MicroSyncBatchTask(logContext, ctx));
            }
            for (int i = 0; i < partitionSortedBySize.size(); i++) {
                int partitionId = partitionSortedBySize.get(i);
                microSyncBatchTasks.get(i % parallel).addPartition(partition2bound.get(partitionId));
            }
            for (MicroSyncBatchTask task : microSyncBatchTasks) {
                task.startOffsets(startOffsets);
                task.endOffsets(config.microSyncBatchSize(), avgRecordSize * decompressedRatio);
                task.offsetBounds().forEach(bound -> startOffsets.put(bound.partition.partitionId(), bound.end));
                if (task.hasMoreData()) {
                    hasMoreData = true;
                }
            }
        }

        public CompletableFuture<SyncTask> sync() {
            TimerUtil timer = new TimerUtil();
            plan();
            return CompletableFuture.allOf(microSyncBatchTasks
                    .stream()
                    .map(t -> t.run().thenApply(rst -> {
                        if (rst.ctx.requireReset) {
                            TopicPartitionsWorker.this.requireReset = true;
                        }
                        updateDecompressedRatio();
                        return rst;
                    })).toArray(CompletableFuture[]::new)
                )
                .thenApply(nil -> this)
                .whenComplete((rst, ex) -> {
                    moreQuota.release();
                    if (ex != null) {
                        LOGGER.error("[SYNC_TASK_FAIL],{}", logContext, ex);
                        TopicPartitionsWorker.this.requireReset = true;
                    } else {
                        TopicPartitionsWorker.this.writers.addAll(writers);
                        List<OffsetBound> offsetBounds = microSyncBatchTasks.stream().flatMap(t -> t.offsetBounds().stream()).collect(Collectors.toList());
                        double size = avgRecordSize * offsetBounds.stream().mapToLong(bound -> bound.end - bound.start).sum();
                        LOGGER.info("[SYNC_TASK],{},size={},decompressedRatio={},parallel={},partitions={},cost={}ms", logContext, size, decompressedRatio, microSyncBatchTasks.size(), offsetBounds, timer.elapsedAs(TimeUnit.MILLISECONDS));
                    }
                });
        }

        private void updateDecompressedRatio() {
            MicroSyncBatchTask task = microSyncBatchTasks.get(0);
            double recordBatchSize = task.offsetBounds.stream().mapToLong(bound -> bound.end - bound.start).sum() * avgRecordSize;
            if (recordBatchSize == 0) {
                return;
            }
            TopicPartitionsWorker.this.updateDecompressedRatio(Math.max(task.ctx.writeSize / recordBatchSize, 1.0));
        }
    }

    /**
     * The micro sync batch task syncs the records in partition - [start, end) to iceberg.
     * How to use:
     * 1. #addPartition: add the partition to the task.
     * 2. #startOffsets: set start offsets for partition syncing.
     * 3. #endOffset: adjust the end offsets for partition syncing according to the batch size and record size.
     * 4. #run: run the task.
     */
    static class MicroSyncBatchTask {
        private final List<OffsetBound> offsetBounds = new ArrayList<>();
        private boolean hasMoreData;
        private final String logContext;
        private final PartitionWriteTaskContext ctx;

        public MicroSyncBatchTask(String logContext, PartitionWriteTaskContext ctx) {
            this.logContext = logContext;
            this.ctx = ctx;
        }

        public List<OffsetBound> offsetBounds() {
            return offsetBounds;
        }

        public boolean hasMoreData() {
            return hasMoreData;
        }

        public void addPartition(OffsetBound offsetBound) {
            offsetBounds.add(offsetBound);
        }

        public void startOffsets(Map<Integer, Long> startOffsets) {
            offsetBounds.forEach(bound -> bound.start = startOffsets.get(bound.partition.partitionId()));
            startOffsets.forEach(ctx.writer::setOffset);
        }

        public void endOffsets(int microSyncBatchSize, double avgRecordSize) {
            int avgRecordCountPerPartition = Math.max((int) Math.ceil(microSyncBatchSize / avgRecordSize / offsetBounds.size()), 1);
            long[] fixedEndOffsets = new long[offsetBounds.size()];
            hasMoreData = offsetBounds.stream().mapToLong(bound -> bound.end - bound.start).sum() * avgRecordSize > microSyncBatchSize;
            if (!hasMoreData) {
                return;
            }
            int remaining = 0;
            for (int i = 0; i < offsetBounds.size(); i++) {
                OffsetBound offsetBound = offsetBounds.get(i);
                int count = (int) Math.min(offsetBound.end - offsetBound.start, avgRecordCountPerPartition);
                fixedEndOffsets[i] = offsetBound.start + count;
                remaining += avgRecordCountPerPartition - count;
            }
            for (int i = 0; i < offsetBounds.size() && remaining > 0; i++) {
                OffsetBound offsetBound = offsetBounds.get(i);
                int add = (int) Math.min(offsetBound.end - fixedEndOffsets[i], remaining);
                fixedEndOffsets[i] += add;
                remaining -= add;
            }
            for (int i = 0; i < offsetBounds.size(); i++) {
                offsetBounds.get(i).end = fixedEndOffsets[i];
            }
        }

        public CompletableFuture<MicroSyncBatchTask> run() {
            List<CompletableFuture<Void>> partitionCfList = new ArrayList<>();
            offsetBounds.forEach(bound -> partitionCfList.add(runPartitionWriteTask(bound.partition, bound.start, bound.end)));
            return CompletableFuture.allOf(partitionCfList.toArray(new CompletableFuture[0]))
                .thenComposeAsync(nil -> {
                    if (ctx.requireReset) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return ctx.writer.flush(FlushMode.COMPLETE, ctx.flushExecutors, ctx.eventLoop)
                            .exceptionally(ex -> {
                                ctx.requireReset = true;
                                LOGGER.error("[FLUSH_FAIL],{}", logContext, ex);
                                return null;
                            });
                    }
                }, ctx.eventLoop).thenApply(nil -> this).whenComplete((nil, ex) -> {
                    ctx.eventLoop.release();
                });
        }

        protected CompletableFuture<Void> runPartitionWriteTask(Partition partition, long start, long end) {
            return new PartitionWriteTask(partition, start, end, ctx).run();
        }
    }

    class AdvanceSync implements PartitionAppendListener {
        /**
         * The dirty bytes which partition appends but not sync to the writer. It's used to trigger the advanced sync.
         */
        private final AtomicLong dirtyBytesCounter = new AtomicLong(0);
        private final AtomicBoolean fastNextSync = new AtomicBoolean(false);

        /**
         * Try to sync the 'hot data' to the writer.
         * <p>
         * Note: it should run in event loop.
         */
        public void sync() {
            Optional<Map<Integer, Long>> startOffsetsOpt = getStartOffsets();
            if (startOffsetsOpt.isEmpty()) {
                return;
            }
            fastNextSync.set(false);
            SyncTask syncTask = newSyncTask("ADVANCED", startOffsetsOpt.get(), System.currentTimeMillis());
            syncTask.sync().whenCompleteAsync((task, e) -> {
                transitionTo(Status.IDLE);
                fastNextSync.set(task.hasMoreData());
                run();
            }, eventLoop);

        }

        @Override
        public void onAppend(TopicPartition partition, MemoryRecords records) {
            if (dirtyBytesCounter.addAndGet(records.sizeInBytes()) * decompressedRatio > config.incrementSyncThreshold()) {
                if (fastNextSync.compareAndSet(false, true)) {
                    eventLoop.submit(TopicPartitionsWorker.this::run);
                }
            }
        }

        private Optional<Map<Integer, Long>> getStartOffsets() {
            if (!transitionTo(Status.ADVANCE_SYNC)) {
                TableWorkers.SCHEDULER.schedule(() -> eventLoop.submit(TopicPartitionsWorker.this::run), 100, TimeUnit.MILLISECONDS);
                return Optional.empty();
            }
            if (!fastNextSync.get()
                // The commit speed can't keep up with the advance sync rate.
                // We need to limit the number of writers to prevent excessive heap usage.
                || writers.size() >= WRITER_LIMIT
            ) {
                transitionTo(Status.IDLE);
                return Optional.empty();
            }
            if (writers.isEmpty()) {
                // Await for the first commit request to know the sync start offset
                transitionTo(Status.IDLE);
                return Optional.empty();
            }
            Writer lastWriter = writers.get(writers.size() - 1);
            Map<Integer, Long> startOffsets = new HashMap<>();
            lastWriter.getOffsets().forEach((partition, offset) -> startOffsets.put(partition, offset.end()));
            this.dirtyBytesCounter.set(0);
            return Optional.of(startOffsets);
        }

    }

    class Commit {
        boolean starving = false;
        private List<WorkerOffset> lastCommitRequestOffsets = Collections.emptyList();

        /**
         * Handle the pending commit requests.
         * <p>
         * Note: it should run in event loop.
         */
        void commit() throws Exception {
            if (!transitionTo(Status.COMMIT)) {
                TableWorkers.SCHEDULER.schedule(() -> eventLoop.submit(TopicPartitionsWorker.this::run), 1, TimeUnit.SECONDS);
                return;
            }
            RequestWrapper requestWrapper = preCommit();
            if (requestWrapper == null) {
                transitionTo(Status.IDLE);
                return;
            }
            if (writerFactory.partitionSpec() != null && requestWrapper.request.specId() > writerFactory.partitionSpec().specId()) {
                writerFactory.reset();
                writers.clear();
                LOGGER.info("[WORKER_APPLY_NEW_PARTITION],{},spec={}", this, config.partitionByConfig());
            }
            config.refresh();

            cleanupCommitedWriters(requestWrapper.request.offsets());
            if (requestWrapper.request.offsets().equals(lastCommitRequestOffsets) && !writers.isEmpty()) {
                // fast commit previous timeout request
                commitResponse(requestWrapper, true);
                starving = true;
                return;
            }
            starving = false;
            advanceSync.dirtyBytesCounter.set(0);
            Writer lastWriter = writers.isEmpty() ? null : writers.get(writers.size() - 1);
            Map<Integer, Long> startOffsets = requestWrapper.request.offsets().stream().collect(Collectors.toMap(
                WorkerOffset::partition,
                o -> Optional.ofNullable(lastWriter)
                    .map(w -> w.getOffsets().get(o.partition()))
                    .map(offsetRange -> offsetRange.end)
                    .orElse(o.offset())
            ));
            SyncTask syncTask = newSyncTask("COMMIT", startOffsets, requestWrapper.timestamp);
            syncTask.sync().whenCompleteAsync((task, e) -> {
                if (e == null) {
                    lastCommitRequestOffsets = requestWrapper.request.offsets();
                }
                commitResponse(requestWrapper, task.hasMoreData());
                if (task.hasMoreData()) {
                    advanceSync.fastNextSync.set(true);
                }
            }, eventLoop);
        }

        /**
         * 1. Remove outdated commit requests.
         * 2. Filter the partitions need resolved by current worker and EPOCH_MISMATCH response with whose
         * partition epoch not equals to request epoch.
         * 3. Generate a new {@link RequestWrapper} with remaining valid partitions.
         *
         * @return {@link RequestWrapper} the request needs to be handled, or null if there is no request needs to be handled.
         */
        private RequestWrapper preCommit() throws Exception {
            if (commitRequests.isEmpty()) {
                return null;
            }
            if (commitRequests.size() > 1) {
                LOGGER.warn("[DROP_COMMIT_REQUEST],{}", commitRequests.subList(0, commitRequests.size() - 1));
            }
            RequestWrapper requestWrapper = commitRequests.get(commitRequests.size() - 1);
            commitRequests.clear();

            CommitRequest request = requestWrapper.request;
            List<Partition> epochMismatchPartitions = new ArrayList<>();
            List<WorkerOffset> requestCommitPartitions = new ArrayList<>();
            // filter the partitions need resolved by current worker.
            for (WorkerOffset offset : request.offsets()) {
                Partition partition = partitions.get(offset.partition());
                if (partition == null) {
                    continue;
                }
                if (partition.getLeaderEpoch() != offset.epoch()) {
                    epochMismatchPartitions.add(partition);
                } else {
                    requestCommitPartitions.add(offset);
                }
            }
            if (!epochMismatchPartitions.isEmpty()) {
                CommitResponse commitResponse = mismatchEpochResponse(request, epochMismatchPartitions);
                channel.asyncSend(topic, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, commitResponse));
                LOGGER.info("[COMMIT_RESPONSE],[EPOCH_MISMATCH],{}", commitResponse);
            }
            if (requestCommitPartitions.isEmpty()) {
                return null;
            }
            RequestWrapper wrapper = new RequestWrapper(new CommitRequest(request.commitId(), request.topic(), request.specId(), requestCommitPartitions), requestWrapper.cf, requestWrapper.timestamp);
            if (System.currentTimeMillis() - wrapper.timestamp >= TimeUnit.SECONDS.toMillis(30)) {
                LOGGER.warn("[DROP_EXPIRED_COMMIT_REQUEST],{}", wrapper.request.commitId());
                starving = true;
                return null;
            }
            return wrapper;
        }

        private CompletableFuture<Void> commitResponse(RequestWrapper requestWrapper, boolean fastCommit) {
            CompletableFuture<Void> cf = CompletableFuture.completedFuture(null);
            cf = cf.thenComposeAsync(nil -> {
                if (requireReset) {
                    CommitResponse commitResponse = new CommitResponse(Types.StructType.of(), Errors.MORE_DATA, requestWrapper.request.commitId(),
                        topic, requestWrapper.request.offsets(), Collections.emptyList(), Collections.emptyList(), TopicMetric.NOOP, Collections.emptyList());
                    return channel.asyncSend(topic, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, commitResponse))
                        .thenAccept(rst -> LOGGER.info("[COMMIT_RESPONSE],{}", commitResponse));
                }
                Map<Integer, Integer> partition2epoch = requestWrapper.request.offsets().stream().collect(Collectors.toMap(WorkerOffset::partition, WorkerOffset::epoch));
                Map<Integer, Long> partitionNextOffsetMap = new HashMap<>();

                List<DataFile> dataFiles = new ArrayList<>();
                List<DeleteFile> deleteFiles = new ArrayList<>();
                Map<Integer, PartitionMetric> partitionMetricMap = new HashMap<>();
                long fieldCount = 0;
                for (Writer writer : writers) {
                    List<WriteResult> writeResults = writer.results();
                    writeResults.forEach(writeResult -> {
                        dataFiles.addAll(List.of(writeResult.dataFiles()));
                        deleteFiles.addAll(List.of(writeResult.deleteFiles()));
                    });
                    writer.getOffsets().forEach((partition, offsetRange) -> partitionNextOffsetMap.put(partition, offsetRange.end()));
                    writer.partitionMetrics().forEach((partition, metric) -> partitionMetricMap.compute(partition, (p, oldMetric) -> {
                        if (oldMetric == null) {
                            return metric;
                        } else {
                            return metric.watermark() > oldMetric.watermark() ? metric : oldMetric;
                        }
                    }));
                    fieldCount += writer.topicMetric().fieldCount();
                }
                List<WorkerOffset> nextOffsets = partitionNextOffsetMap.entrySet().stream()
                    .map(e -> new WorkerOffset(e.getKey(), partition2epoch.get(e.getKey()), e.getValue()))
                    .collect(Collectors.toList());
                List<PartitionMetric> partitionMetrics = partitionMetricMap.entrySet().stream()
                    .map(e -> new PartitionMetric(e.getKey(), e.getValue().watermark()))
                    .collect(Collectors.toList());
                TopicMetric topicMetric = new TopicMetric(fieldCount);

                CommitResponse commitResponse = new CommitResponse(writerFactory.partitionSpec().partitionType(), fastCommit ? Errors.MORE_DATA : Errors.NONE,
                    requestWrapper.request.commitId(), topic, nextOffsets, dataFiles, deleteFiles, topicMetric, partitionMetrics);
                return channel.asyncSend(topic, new Event(System.currentTimeMillis(), EventType.COMMIT_RESPONSE, commitResponse))
                    .thenAccept(rst -> LOGGER.info("[COMMIT_RESPONSE],{}", commitResponse));
            }, eventLoop).whenCompleteAsync((nil, ex) -> {
                if (ex != null) {
                    requireReset = true;
                    LOGGER.error("[COMMIT_RESPONSE_FAIL],{}", topic, ex);
                    requestWrapper.cf.completeExceptionally(ex);
                } else {
                    requestWrapper.cf.complete(null);
                }
                transitionTo(Status.IDLE);
                run();
            }, eventLoop);
            return cf;
        }

        private void cleanupCommitedWriters(List<WorkerOffset> offsets) throws IOException {
            TopicPartitionsWorker.cleanupCommitedWriters(topic, offsets, writers);
        }
    }

    static CommitResponse mismatchEpochResponse(CommitRequest request, List<Partition> partitions) {
        return new CommitResponse(
            Types.StructType.of(),
            Errors.EPOCH_MISMATCH,
            request.commitId(),
            request.topic(),
            partitions.stream().map(p -> new WorkerOffset(p.partitionId(), p.getLeaderEpoch(), -1)).collect(Collectors.toList()),
            Collections.emptyList(),
            Collections.emptyList(),
            TopicMetric.NOOP,
            Collections.emptyList()
        );
    }

    static class RequestWrapper {
        CommitRequest request;
        // only used for test
        CompletableFuture<Void> cf = new CompletableFuture<>();
        long timestamp;

        public RequestWrapper(CommitRequest request) {
            this.request = request;
            this.timestamp = System.currentTimeMillis();
        }

        public RequestWrapper(CommitRequest request, CompletableFuture<Void> cf, long timestamp) {
            this.request = request;
            this.cf = cf;
            this.timestamp = timestamp;
        }
    }

    enum Status {
        ADVANCE_SYNC,
        COMMIT,
        IDLE,
    }

    static class OffsetBound {
        final Partition partition;
        long start;
        long end;

        public OffsetBound(Partition partition, long start, long end) {
            this.partition = partition;
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return String.format("%s-%s-%s", partition.partitionId(), start, end);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;
            OffsetBound that = (OffsetBound) o;
            return start == that.start && end == that.end && Objects.equals(partition, that.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, start, end);
        }
    }

}
