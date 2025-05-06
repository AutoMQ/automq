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

package kafka.automq.table.coordinator;

import kafka.automq.table.Channel;
import kafka.automq.table.TableTopicMetricsManager;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Errors;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.WorkerOffset;
import kafka.automq.table.utils.PartitionUtil;
import kafka.automq.table.utils.TableIdentifierUtil;
import kafka.log.streamaspect.MetaKeyValue;
import kafka.log.streamaspect.MetaStream;
import kafka.server.MetadataCache;

import org.apache.kafka.storage.internals.log.LogConfig;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
public class TableCoordinator implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableCoordinator.class);
    private static final ScheduledExecutorService SCHEDULER = Threads.newSingleThreadScheduledExecutor("table-coordinator", true, LOGGER);
    private static final ExecutorService EXPIRE_SNAPSHOT_EXECUTOR = Threads.newFixedThreadPoolWithMonitor(Systems.CPU_CORES * 2, "table-coordinator-expire-snapshot", true, LOGGER);
    private static final String TABLE_COMMIT_CHECKPOINT_KEY = "TABLE_COMMIT_CHECKPOINT";
    private static final String SNAPSHOT_COMMIT_ID = "automq.commit.id";
    private static final String WATERMARK = "automq.watermark";
    private static final UUID NOOP_UUID = new UUID(0, 0);
    private static final Map<String, Long> WATERMARK_METRICS = new ConcurrentHashMap<>();
    private static final Map<String, Double> FIELD_PER_SECONDS_METRICS = new ConcurrentHashMap<>();
    private static final long NOOP_WATERMARK = -1L;

    static {
        TableTopicMetricsManager.setDelaySupplier(() -> {
            Map<String, Long> delay = new HashMap<>(WATERMARK_METRICS.size());
            long now = System.currentTimeMillis();
            WATERMARK_METRICS.forEach((topic, watermark) -> {
                if (watermark != NOOP_WATERMARK) {
                    delay.put(topic, now - watermark);
                }
            });
            return delay;
        });
        TableTopicMetricsManager.setFieldsPerSecondSupplier(() -> FIELD_PER_SECONDS_METRICS);
    }

    private final Catalog catalog;
    private final String topic;
    private final String name;
    private final MetaStream metaStream;
    private final Channel channel;
    private Table table;
    private final EventLoop eventLoop;
    private final MetadataCache metadataCache;
    private final TableIdentifier tableIdentifier;
    private final Time time = Time.SYSTEM;
    private final long commitTimeout = TimeUnit.SECONDS.toMillis(30);
    private volatile boolean closed = false;
    private final Supplier<LogConfig> config;

    public TableCoordinator(Catalog catalog, String topic, MetaStream metaStream, Channel channel,
        EventLoop eventLoop, MetadataCache metadataCache, Supplier<LogConfig> config) {
        this.catalog = catalog;
        this.topic = topic;
        this.name = topic;
        this.metaStream = metaStream;
        this.channel = channel;
        this.eventLoop = eventLoop;
        this.metadataCache = metadataCache;
        this.config = config;
        this.tableIdentifier = TableIdentifierUtil.of(config.get().tableTopicNamespace, topic);
    }

    private CommitStatusMachine commitStatusMachine;

    public void start() {
        WATERMARK_METRICS.put(topic, -1L);
        FIELD_PER_SECONDS_METRICS.put(topic, 0.0);

        // await for a while to avoid multi coordinators concurrent commit.
        SCHEDULER.schedule(() -> {
            eventLoop.execute(() -> {
                try {
                    // recover checkpoint from metaStream
                    Optional<ByteBuffer> buf = metaStream.get(TABLE_COMMIT_CHECKPOINT_KEY);
                    if (buf.isPresent()) {
                        Checkpoint checkpoint = Checkpoint.decode(buf.get());
                        commitStatusMachine = new CommitStatusMachine(checkpoint);
                    } else {
                        commitStatusMachine = new CommitStatusMachine();
                    }
                    this.run();
                } catch (Throwable e) {
                    LOGGER.error("[TABLE_COORDINATOR_START_FAIL],{}", this, e);
                }
            });
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        // quick close
        closed = true;
        WATERMARK_METRICS.remove(topic);
        FIELD_PER_SECONDS_METRICS.remove(topic);
        eventLoop.execute(() -> {
            if (commitStatusMachine != null) {
                commitStatusMachine.close();
            }
            LOGGER.info("[TABLE_COORDINATOR_UNLOAD],{}", topic);
        });
    }

    @Override
    public String toString() {
        return "TableCoordinator{" +
            ", topic='" + topic +
            '}';
    }

    private void run() {
        eventLoop.execute(this::run0);
    }

    private void run0() {
        try {
            if (closed) {
                return;
            }
            switch (commitStatusMachine.status) {
                case INIT:
                case COMMITTED:
                    commitStatusMachine.nextRoundCommit();
                    break;
                case REQUEST_COMMIT:
                    commitStatusMachine.tryMoveToCommitedStatus();
                    break;
                default:
                    LOGGER.error("[TABLE_COORDINATOR_UNKNOWN_STATUS],{}", commitStatusMachine.status);
            }
            SCHEDULER.schedule(this::run, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (closed) {
                LOGGER.warn("Error in table coordinator", e);
            } else {
                LOGGER.error("Error in table coordinator", e);
            }
            if (commitStatusMachine != null) {
                commitStatusMachine.close();
            }
            if (!closed) {
                // reset the coordinator and retry later
                SCHEDULER.schedule(this::start, 30, TimeUnit.SECONDS);
            }
        }
    }

    static class CommitInfo {
        UUID commitId;
        long taskOffset;
        long[] nextOffsets;

        public CommitInfo(UUID commitId, long taskOffset, long[] nextOffsets) {
            this.commitId = commitId;
            this.taskOffset = taskOffset;
            this.nextOffsets = nextOffsets;
        }
    }

    class CommitStatusMachine {
        Status status = Status.INIT;

        CommitInfo last;
        CommitInfo processing;

        long lastCommitTimestamp = 0;

        List<DataFile> dataFiles = new ArrayList<>();
        List<DeleteFile> deleteFiles = new ArrayList<>();
        private BitSet readyPartitions;
        private int unreadyPartitionCount;
        private long requestCommitTimestamp = time.milliseconds();
        private long commitFieldCount;
        private long[] partitionWatermarks = new long[0];
        private boolean fastNextCommit;
        private long polledOffset = 0;

        String lastAppliedPartitionBy;

        Channel.SubChannel subChannel;

        public CommitStatusMachine() {
            last = new CommitInfo(NOOP_UUID, 0, new long[0]);
            this.subChannel = channel.subscribeData(topic, Channel.LATEST_OFFSET);
        }

        public CommitStatusMachine(Checkpoint checkpoint) {
            status = checkpoint.status();
            lastCommitTimestamp = checkpoint.lastCommitTimestamp();
            switch (status) {
                case INIT: {
                    throw new IllegalStateException("Invalid checkpoint status: " + status);
                }
                case REQUEST_COMMIT: {
                    last = new CommitInfo(checkpoint.lastCommitId(), checkpoint.taskOffset(), copy(checkpoint.nextOffsets()));
                    processing = new CommitInfo(checkpoint.commitId(), checkpoint.taskOffset(), copy(checkpoint.nextOffsets()));
                    break;
                }
                case PRE_COMMIT: {
                    last = new CommitInfo(checkpoint.lastCommitId(), checkpoint.taskOffset(), copy(checkpoint.nextOffsets()));
                    processing = new CommitInfo(checkpoint.commitId(), checkpoint.taskOffset(), copy(checkpoint.nextOffsets()));
                    // check if the last commit is the same as the last snapshot commit
                    Optional<UUID> lastSnapshotCommitId = getLastSnapshotCommitId();
                    if (lastSnapshotCommitId.isPresent() && Objects.equals(processing.commitId, lastSnapshotCommitId.get())) {
                        // TODO: for true exactly once, iceberg should support conditional commit
                        LOGGER.info("[ALREADY_COMMITED],{}", processing.commitId);
                        last = new CommitInfo(checkpoint.commitId(), checkpoint.taskOffset(), copy(checkpoint.preCommitOffsets()));
                        processing = null;
                        status = Status.COMMITTED;
                    } else {
                        status = Status.REQUEST_COMMIT;
                    }
                    break;
                }
                case COMMITTED: {
                    last = new CommitInfo(checkpoint.lastCommitId(), checkpoint.taskOffset(), copy(checkpoint.nextOffsets()));
                    processing = null;
                    break;
                }
            }

            if (status == Status.REQUEST_COMMIT) {
                //noinspection DataFlowIssue
                int partitionCount = processing.nextOffsets.length;
                readyPartitions = new BitSet(partitionCount);
                unreadyPartitionCount = partitionCount;
                partitionWatermarks = new long[partitionCount];
                Arrays.fill(partitionWatermarks, NOOP_WATERMARK);
            }
            this.subChannel = channel.subscribeData(topic, Optional.ofNullable(processing).map(p -> p.taskOffset).orElse(last.taskOffset));
            LOGGER.info("[LOAD_CHECKPOINT],{},{}", topic, checkpoint);
        }

        public void nextRoundCommit() throws Exception {
            // drain the sub channel to avoid catchup-read
            for (; ; ) {
                Envelope envelope = subChannel.poll();
                if (envelope == null) {
                    break;
                }
                polledOffset = envelope.offset();
            }
            long nextCheck = config.get().tableTopicCommitInterval - (time.milliseconds() - lastCommitTimestamp);
            if (!fastNextCommit && nextCheck > 0) {
                return;
            }
            last.taskOffset = polledOffset;
            processing = new CommitInfo(UUID.randomUUID(), polledOffset, copy(last.nextOffsets));
            int partitionNum = (Integer) metadataCache.numPartitions(topic).get();
            handlePartitionNumChange(partitionNum);
            List<WorkerOffset> workerOffsets = new ArrayList<>(partitionNum);
            for (int partitionId = 0; partitionId < partitionNum; partitionId++) {
                int leaderEpoch = metadataCache.getPartitionInfo(topic, partitionId).get().leaderEpoch();
                long offset = processing.nextOffsets[partitionId];
                workerOffsets.add(new WorkerOffset(partitionId, leaderEpoch, offset));
            }

            status = Status.REQUEST_COMMIT;
            dataFiles = new ArrayList<>();
            deleteFiles = new ArrayList<>();
            readyPartitions = new BitSet(partitionNum);
            unreadyPartitionCount = partitionNum;
            requestCommitTimestamp = time.milliseconds();
            commitFieldCount = 0;
            fastNextCommit = false;

            Checkpoint checkpoint = new Checkpoint(status, processing.commitId, polledOffset,
                processing.nextOffsets, last.commitId, lastCommitTimestamp, new long[0]);
            metaStream.append(MetaKeyValue.of(TABLE_COMMIT_CHECKPOINT_KEY, ByteBuffer.wrap(checkpoint.encode()))).get();
            int specId = Optional.ofNullable(table).map(t -> t.spec().specId()).orElse(CommitRequest.NOOP_SPEC_ID);
            CommitRequest commitRequest = new CommitRequest(processing.commitId, topic, specId, workerOffsets);
            LOGGER.info("[SEND_COMMIT_REQUEST],{},{}", name, commitRequest);
            channel.send(topic, new Event(time.milliseconds(), EventType.COMMIT_REQUEST, commitRequest));
        }

        public void tryMoveToCommitedStatus() throws Exception {
            for (; ; ) {
                boolean awaitCommitTimeout = (time.milliseconds() - requestCommitTimestamp) > commitTimeout;
                if (!awaitCommitTimeout) {
                    for (; ; ) {
                        Envelope envelope = subChannel.poll();
                        if (envelope == null) {
                            break;
                        } else {
                            polledOffset = envelope.offset();
                            CommitResponse commitResponse = envelope.event().payload();
                            if (!processing.commitId.equals(commitResponse.commitId())) {
                                continue;
                            }
                            LOGGER.info("[RECEIVE_COMMIT_RESPONSE],{}", commitResponse);
                            dataFiles.addAll(commitResponse.dataFiles());
                            deleteFiles.addAll(commitResponse.deleteFiles());
                            boolean moveNextOffset = commitResponse.code() == Errors.NONE || commitResponse.code() == Errors.MORE_DATA;
                            for (WorkerOffset nextOffset : commitResponse.nextOffsets()) {
                                if (moveNextOffset) {
                                    processing.nextOffsets[nextOffset.partition()] = Math.max(nextOffset.offset(), processing.nextOffsets[nextOffset.partition()]);
                                }
                                if (!readyPartitions.get(nextOffset.partition())) {
                                    readyPartitions.set(nextOffset.partition());
                                    unreadyPartitionCount--;
                                }
                            }

                            commitFieldCount += commitResponse.topicMetric().fieldCount();
                            commitResponse.partitionMetrics().forEach(m -> {
                                if (m.watermark() != NOOP_WATERMARK) {
                                    partitionWatermarks[m.partition()] = Math.max(partitionWatermarks[m.partition()], m.watermark());
                                }
                            });

                            if (commitResponse.code() == Errors.MORE_DATA) {
                                fastNextCommit = true;
                            }
                        }
                    }
                    if (unreadyPartitionCount != 0) {
                        break;
                    }
                }
                TimerUtil transactionCommitTimer = new TimerUtil();
                Transaction transaction = null;
                if (!dataFiles.isEmpty() || !deleteFiles.isEmpty()) {
                    transaction = getTable().newTransaction();
                    long watermark = watermark(partitionWatermarks);
                    if (deleteFiles.isEmpty()) {
                        AppendFiles appendFiles = transaction.newAppend();
                        appendFiles.set(SNAPSHOT_COMMIT_ID, processing.commitId.toString());
                        appendFiles.set(WATERMARK, Long.toString(watermark));
                        dataFiles.forEach(appendFiles::appendFile);
                        appendFiles.commit();
                    } else {
                        RowDelta delta = transaction.newRowDelta();
                        delta.set(SNAPSHOT_COMMIT_ID, processing.commitId.toString());
                        delta.set(WATERMARK, Long.toString(watermark));
                        dataFiles.forEach(delta::addRows);
                        deleteFiles.forEach(delta::addDeletes);
                        delta.commit();
                    }
                    transaction.expireSnapshots()
                        .expireOlderThan(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
                        .retainLast(1)
                        .executeDeleteWith(EXPIRE_SNAPSHOT_EXECUTOR)
                        .commit();
                }

                recordMetrics();
                if (transaction != null) {
                    Checkpoint checkpoint = new Checkpoint(Status.PRE_COMMIT, processing.commitId, last.taskOffset,
                        last.nextOffsets, last.commitId, lastCommitTimestamp, processing.nextOffsets);
                    metaStream.append(MetaKeyValue.of(TABLE_COMMIT_CHECKPOINT_KEY, ByteBuffer.wrap(checkpoint.encode()))).get();
                    transaction.commitTransaction();
                    if (awaitCommitTimeout) {
                        LOGGER.warn("[COMMIT_AWAIT_TIMEOUT],{}", processing.commitId);
                    }
                }
                lastCommitTimestamp = time.milliseconds();

                status = Status.COMMITTED;
                Checkpoint checkpoint = new Checkpoint(status, NOOP_UUID, polledOffset,
                    processing.nextOffsets, processing.commitId, lastCommitTimestamp, new long[0]);
                metaStream.append(MetaKeyValue.of(TABLE_COMMIT_CHECKPOINT_KEY, ByteBuffer.wrap(checkpoint.encode()))).get();
                LOGGER.info("[COMMIT_COMPLETE],{},{},commitCost={}ms,total={}ms", name, processing.commitId,
                    transactionCommitTimer.elapsedAs(TimeUnit.MILLISECONDS), lastCommitTimestamp - requestCommitTimestamp);
                last = processing;
                processing = null;

                if (awaitCommitTimeout) {
                    fastNextCommit = true;
                }

                if (tryEvolvePartition()) {
                    // Let workers know the partition evolution.
                    fastNextCommit = true;
                    LOGGER.info("[TABLE_PARTITION_EVOLUTION],{}", config.get().tableTopicPartitionBy);
                }

                break;
            }
        }

        public void close() {
            if (subChannel != null) {
                subChannel.close();
            }
        }

        private Table getTable() {
            if (table == null) {
                table = catalog.loadTable(tableIdentifier);
            }
            return table;
        }

        private void handlePartitionNumChange(int partitionNum) {
            if (partitionNum > processing.nextOffsets.length) {
                long[] newNextOffsets = new long[partitionNum];
                System.arraycopy(processing.nextOffsets, 0, newNextOffsets, 0, processing.nextOffsets.length);
                processing.nextOffsets = newNextOffsets;
            }
            if (partitionNum > partitionWatermarks.length) {
                long[] newPartitionWatermarks = new long[partitionNum];
                System.arraycopy(partitionWatermarks, 0, newPartitionWatermarks, 0, partitionWatermarks.length);
                for (int i = partitionWatermarks.length; i < partitionNum; i++) {
                    newPartitionWatermarks[i] = NOOP_WATERMARK;
                }
                partitionWatermarks = newPartitionWatermarks;
            }
        }

        private void recordMetrics() {
            double fps = commitFieldCount * 1000.0 / Math.max(System.currentTimeMillis() - lastCommitTimestamp, 1);
            FIELD_PER_SECONDS_METRICS.computeIfPresent(topic, (k, v) -> fps);
            WATERMARK_METRICS.computeIfPresent(topic, (k, v) -> watermark(partitionWatermarks));
        }

        private boolean tryEvolvePartition() {
            String newPartitionBy = config.get().tableTopicPartitionBy;
            if (Objects.equals(newPartitionBy, lastAppliedPartitionBy) || StringUtils.isBlank(newPartitionBy) || table == null) {
                return false;
            }
            boolean changed = PartitionUtil.evolve(PartitionUtil.parsePartitionBy(newPartitionBy), table);
            lastAppliedPartitionBy = newPartitionBy;
            return changed;
        }

        private Optional<UUID> getLastSnapshotCommitId() {
            try {
                if (table == null) {
                    table = catalog.loadTable(tableIdentifier);
                }
                Snapshot snapshot = table.currentSnapshot();
                return Optional.ofNullable(snapshot).map(s -> s.summary().get(SNAPSHOT_COMMIT_ID)).map(UUID::fromString);
            } catch (NoSuchTableException ex) {
                return Optional.empty();
            }
        }

    }

    static long watermark(long[] watermarks) {
        boolean match = false;
        long watermark = Long.MAX_VALUE;
        for (long w : watermarks) {
            if (w != NOOP_WATERMARK) {
                match = true;
                watermark = Math.min(watermark, w);
            }
        }
        if (!match) {
            return NOOP_WATERMARK;
        }
        return watermark;
    }

    static long[] copy(long[] array) {
        return Arrays.copyOf(array, array.length);
    }
}
