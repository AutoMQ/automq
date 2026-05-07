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

package kafka.automq.partition.snapshot;

import kafka.automq.AutoMQConfig;
import kafka.cluster.LogEventListener;
import kafka.cluster.Partition;
import kafka.cluster.PartitionListener;
import kafka.log.streamaspect.ElasticLogMeta;
import kafka.log.streamaspect.ElasticStreamSegmentMeta;
import kafka.log.streamaspect.SliceRange;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotRequestData;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.LogMetadata;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.PartitionSnapshot;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.SegmentMetadata;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.StreamMappingCollection;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.Topic;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData.TopicCollection;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotRequest;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotResponse;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.netty.util.concurrent.FastThreadLocal;

public class PartitionSnapshotsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionSnapshotsManager.class);
    private static final int NOOP_SESSION_ID = 0;
    static final long LONG_POLL_TIMEOUT_MS = 100;
    private final Map<Integer, Session> sessions = new HashMap<>();
    private final List<PartitionWithVersion> snapshotVersions = new CopyOnWriteArrayList<>();
    private final Time time;
    private final ConfirmWAL confirmWAL;
    private final ScheduledExecutorService longPollExecutor = Threads.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory("automq-partition-snapshot-long-poll", true), LOGGER, true);
    private final AtomicBoolean partitionNotificationPending = new AtomicBoolean(false);

    public PartitionSnapshotsManager(Time time, AutoMQConfig config, ConfirmWAL confirmWAL,
        Supplier<AutoMQVersion> versionGetter) {
        this.time = time;
        this.confirmWAL = confirmWAL;
        if (config.zoneRouterChannels().isPresent()) {
            Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(this::cleanExpiredSessions, 1, 1, TimeUnit.MINUTES);
            Threads.COMMON_SCHEDULER.scheduleWithFixedDelay(() -> {
                // In ZERO_ZONE_V0 we need to fast commit the WAL data to KRaft,
                // then another nodes could replay the SSO to support snapshot read.
                if (!versionGetter.get().isZeroZoneV2Supported()) {
                    confirmWAL.commit(0, false);
                }
            }, 1, 1, TimeUnit.SECONDS);
        }
    }

    public void onPartitionOpen(Partition partition) {
        PartitionWithVersion partitionWithVersion = new PartitionWithVersion(partition, PartitionSnapshotVersion.create());
        snapshotVersions.add(partitionWithVersion);
        partition.maybeAddListener(newPartitionListener(partitionWithVersion, this::notifySnapshotChanged));
        partition.addLogEventListener(newLogEventListener(partitionWithVersion, this::notifySnapshotChanged));
        notifySnapshotChanged();
    }

    public void onPartitionClose(Partition partition) {
        snapshotVersions.removeIf(p -> p.partition == partition);
        synchronized (this) {
            sessions.values().forEach(s -> s.onPartitionClose(partition));
        }
        notifySnapshotChanged();
    }

    public CompletableFuture<AutomqGetPartitionSnapshotResponse> handle(AutomqGetPartitionSnapshotRequest request) {
        Session session;
        boolean newSession = false;
        synchronized (this) {
            AutomqGetPartitionSnapshotRequestData requestData = request.data();
            int sessionId = requestData.sessionId();
            int sessionEpoch = requestData.sessionEpoch();
            session = sessions.get(sessionId);
            if (sessionId == NOOP_SESSION_ID
                || session == null
                || (sessionEpoch != session.sessionEpoch())) {
                if (session != null) {
                    sessions.remove(sessionId);
                }
                sessionId = nextSessionId();
                session = new Session(sessionId);
                sessions.put(sessionId, session);
                newSession = true;
            }
        }
        return session.snapshotsDelta(request, request.data().requestCommit() || newSession);
    }

    private synchronized int nextSessionId() {
        int id;
        do {
            id = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        }
        while (sessions.containsKey(id) || id == NOOP_SESSION_ID);
        return id;
    }

    private synchronized void cleanExpiredSessions() {
        sessions.values().removeIf(s -> {
            boolean expired = s.expired();
            if (expired) {
                s.close();
            }
            return expired;
        });
    }

    private void notifySnapshotChanged() {
        if (partitionNotificationPending.compareAndSet(false, true)) {
            longPollExecutor.execute(this::tryNotifySnapshotChanged);
        }
    }

    private void tryNotifySnapshotChanged() {
        List<Session> sessionsSnapshot;
        synchronized (this) {
            partitionNotificationPending.set(false);
            sessionsSnapshot = new ArrayList<>(sessions.values());
        }
        sessionsSnapshot.forEach(Session::tryCompletePendingLongPoll);
    }

    /*
     * A session returns partition/WAL deltas immediately when available. Otherwise, the request is parked as a
     * pending long-poll and is completed by the first later partition/WAL change that produces deltas, or by timeout.
     *
     * Partition-change notifications are coalesced at the manager level, while WAL append notifications are
     * coalesced per session. Both paths enqueue at most one async longPollExecutor wakeup while a prior notification is
     * pending.
     *
     * Session mutable state is serialized by the Session monitor. The manager monitor is only used for the session
     * map; session wakeups run after the manager lock is released.
     */
    class Session {
        private static final short ZERO_ZONE_V0_REQUEST_VERSION = (short) 0;
        private static final FastThreadLocal<List<CompletableFuture<Void>>> COMPLETE_CF_LIST_LOCAL = new FastThreadLocal<>() {
            @Override
            protected List<CompletableFuture<Void>> initialValue() {
                return new ArrayList<>();
            }
        };
        private final int sessionId;
        private int sessionEpoch = 0;
        private final Map<Partition, PartitionSnapshotVersion> synced = new HashMap<>();
        private final List<Partition> removed = new ArrayList<>();
        private long lastGetSnapshotsTimestamp = time.milliseconds();
        private final Set<CompletableFuture<Void>> inflightCommitCfSet = ConcurrentHashMap.newKeySet();
        private final ConfirmWalDataDelta delta;
        private final AtomicBoolean walAppendNotificationPending = new AtomicBoolean(false);
        private PendingLongPoll pendingLongPoll;

        public Session(int sessionId) {
            this.sessionId = sessionId;
            this.delta = new ConfirmWalDataDelta(confirmWAL,
                this::notifyWalAppended);
        }

        public synchronized void close() {
            delta.close();
            completePendingLongPoll();
        }

        public synchronized int sessionEpoch() {
            return sessionEpoch;
        }

        public synchronized CompletableFuture<AutomqGetPartitionSnapshotResponse> snapshotsDelta(
            AutomqGetPartitionSnapshotRequest request, boolean requestCommit) {
            if (pendingLongPoll != null) {
                // Same-session requests with the current epoch are retries of the outstanding long-poll request.
                lastGetSnapshotsTimestamp = time.milliseconds();
                return pendingLongPoll.future;
            }
            if (!requestCommit && inflightCommitCfSet.isEmpty()) {
                PendingLongPoll pending = new PendingLongPoll(request, new CompletableFuture<>());
                pendingLongPoll = pending;
                pending.future.whenComplete((response, ex) -> {
                    synchronized (Session.this) {
                        cleanupPendingLongPoll(pending);
                    }
                });
                registerLongPolling(pending);
                return pending.future;
            } else {
                AutomqGetPartitionSnapshotResponseData resp = new AutomqGetPartitionSnapshotResponseData();
                return completeResponse(request, requestCommit, resp);
            }
        }

        public synchronized void onPartitionClose(Partition partition) {
            removed.add(partition);
        }

        public synchronized boolean expired() {
            return time.milliseconds() - lastGetSnapshotsTimestamp > 60000;
        }

        private void registerLongPolling(PendingLongPoll pending) {
            AutomqGetPartitionSnapshotResponseData resp = new AutomqGetPartitionSnapshotResponseData();
            CompletableFuture<Void> collectPartitionSnapshotsCf = collectPartitionSnapshots(
                pending.request.data().version(), resp);
            if (hasResponseDeltas(pending.request, resp)) {
                collectPartitionSnapshotsCf.whenComplete((nil, ex) -> {
                    synchronized (Session.this) {
                        if (pendingLongPoll == pending) {
                            completePendingResponse(pending, resp, ex);
                        }
                    }
                });
                return;
            }

            pending.timeoutTask = longPollExecutor.schedule(
                () -> {
                    synchronized (Session.this) {
                        if (pendingLongPoll == pending) {
                            completePendingLongPoll();
                        }
                    }
                },
                LONG_POLL_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
            );
            pending.triggerCf.thenCompose(nil -> {
                pending.cancelTimeout();
                AutomqGetPartitionSnapshotResponseData triggeredResp = new AutomqGetPartitionSnapshotResponseData();
                return collectPartitionSnapshots(pending.request.data().version(), triggeredResp)
                    .thenApply(ignored -> triggeredResp);
            }).whenComplete((triggeredResp, ex) -> {
                synchronized (Session.this) {
                    if (pendingLongPoll == pending) {
                        // A partition/WAL trigger is a liveness signal for the parked request: complete the long poll
                        // even when the collected response is empty because the triggering delta may have been consumed.
                        completePendingResponse(pending, triggeredResp, ex);
                    }
                }
            });
        }

        private CompletableFuture<AutomqGetPartitionSnapshotResponse> completeResponse(
            AutomqGetPartitionSnapshotRequest request,
            boolean requestCommit,
            AutomqGetPartitionSnapshotResponseData resp
        ) {
            sessionEpoch++;
            lastGetSnapshotsTimestamp = time.milliseconds();
            resp.setSessionId(sessionId);
            resp.setSessionEpoch(sessionEpoch);
            boolean newSession = sessionEpoch == 1;
            if (request.data().version() > ZERO_ZONE_V0_REQUEST_VERSION) {
                if (newSession) {
                    // return the WAL config in the session first response
                    resp.setConfirmWalConfig(confirmWAL.uri());
                }
                delta.handle(request.version(), resp);
            }
            if (requestCommit) {
                // Commit after generating the snapshots.
                // Then the snapshot-read partitions could read from snapshot-read cache or block cache.
                CompletableFuture<Void> commitCf = newSession ?
                    // The proxy node's first snapshot-read request needs to commit immediately to ensure the data could be read.
                    confirmWAL.commit(0, false)
                    // The proxy node's snapshot-read cache isn't enough to hold the 'uncommitted' data,
                    // so the proxy node request a commit to ensure the data could be read from block cache.
                    : confirmWAL.commit(1000, false);
                inflightCommitCfSet.add(commitCf);
                commitCf.whenComplete((rst, ex) -> inflightCommitCfSet.remove(commitCf));
            }
            return CompletableFuture.completedFuture(new AutomqGetPartitionSnapshotResponse(resp));
        }

        private synchronized void tryCompletePendingLongPoll() {
            completePendingLongPoll();
        }

        private void notifyWalAppended() {
            if (walAppendNotificationPending.compareAndSet(false, true)) {
                longPollExecutor.execute(this::tryNotifyWalAppended);
            }
        }

        private synchronized void tryNotifyWalAppended() {
            walAppendNotificationPending.set(false);
            completePendingLongPoll();
        }

        private void completePendingLongPoll() {
            PendingLongPoll pending = pendingLongPoll;
            if (pending == null) {
                return;
            }
            pending.triggerCf.complete(null);
        }

        private void completePendingResponse(PendingLongPoll pending, AutomqGetPartitionSnapshotResponseData resp,
            Throwable ex) {
            if (ex != null) {
                pending.future.completeExceptionally(ex);
                return;
            }
            FutureUtil.propagate(completeResponse(pending.request, false, resp), pending.future);
        }

        private void cleanupPendingLongPoll(PendingLongPoll pending) {
            if (pendingLongPoll != pending) {
                return;
            }
            pendingLongPoll = null;
            pending.cancelTimeout();
        }

        private boolean hasResponseDeltas(AutomqGetPartitionSnapshotRequest request,
            AutomqGetPartitionSnapshotResponseData resp) {
            return hasSnapshotDeltas(resp)
                || (request.data().version() > ZERO_ZONE_V0_REQUEST_VERSION && delta.hasNewEndOffset());
        }

        private CompletableFuture<Void> collectPartitionSnapshots(short funcVersion,
            AutomqGetPartitionSnapshotResponseData resp) {
            Map<Uuid, List<PartitionSnapshot>> topic2partitions = new HashMap<>();
            List<CompletableFuture<Void>> completeCfList = COMPLETE_CF_LIST_LOCAL.get();
            completeCfList.clear();
            removed.forEach(partition -> {
                PartitionSnapshotVersion version = synced.remove(partition);
                if (version != null) {
                    List<PartitionSnapshot> partitionSnapshots = topic2partitions.computeIfAbsent(partition.topicId().get(), topic -> new ArrayList<>());
                    partitionSnapshots.add(snapshot(funcVersion, partition, version, null, completeCfList));
                }
            });
            removed.clear();

            snapshotVersions.forEach(p -> {
                PartitionSnapshotVersion oldVersion = synced.get(p.partition);
                if (!Objects.equals(p.version, oldVersion)) {
                    List<PartitionSnapshot> partitionSnapshots = topic2partitions.computeIfAbsent(p.partition.topicId().get(), topic -> new ArrayList<>());
                    PartitionSnapshotVersion newVersion = p.version.copy();
                    PartitionSnapshot partitionSnapshot = snapshot(funcVersion, p.partition, oldVersion, newVersion, completeCfList);
                    partitionSnapshots.add(partitionSnapshot);
                    synced.put(p.partition, newVersion);
                }
            });
            TopicCollection topics = new TopicCollection();
            topic2partitions.forEach((topicId, partitions) -> {
                Topic topic = new Topic();
                topic.setTopicId(topicId);
                topic.setPartitions(partitions);
                topics.add(topic);
            });
            resp.setTopics(topics);
            CompletableFuture<Void> retCf = CompletableFuture.allOf(completeCfList.toArray(new CompletableFuture[0]));
            completeCfList.clear();
            return retCf.exceptionally(ex -> {
                LOGGER.warn("Partition snapshot completion failed, returning collected snapshot delta anyway. sessionId={}",
                    sessionId, ex);
                return null;
            });
        }

        private PartitionSnapshot snapshot(short funcVersion, Partition partition,
            PartitionSnapshotVersion oldVersion,
            PartitionSnapshotVersion newVersion, List<CompletableFuture<Void>> completeCfList) {
            if (newVersion == null) {
                // partition is closed
                PartitionSnapshot snapshot = new PartitionSnapshot();
                snapshot.setPartitionIndex(partition.partitionId());
                snapshot.setLeaderEpoch(partition.getLeaderEpoch());
                snapshot.setOperation(SnapshotOperation.REMOVE.code());
                return snapshot;
            }
            return partition.withReadLock(() -> {
                boolean includeSegments = oldVersion == null || oldVersion.segmentsVersion() < newVersion.segmentsVersion();
                PartitionSnapshot snapshot = new PartitionSnapshot();
                snapshot.setPartitionIndex(partition.partitionId());
                kafka.cluster.PartitionSnapshot src = partition.snapshot();
                completeCfList.add(src.completeCf());
                snapshot.setLeaderEpoch(src.leaderEpoch());
                SnapshotOperation operation = oldVersion == null ? SnapshotOperation.ADD : SnapshotOperation.PATCH;
                snapshot.setOperation(operation.code());
                snapshot.setFirstUnstableOffset(logOffsetMetadata(src.firstUnstableOffset()));
                snapshot.setLogEndOffset(logOffsetMetadata(src.logEndOffset()));
                snapshot.setStreamMetadata(src.streamEndOffsets().entrySet()
                    .stream()
                    .map(e -> new AutomqGetPartitionSnapshotResponseData.StreamMetadata().setStreamId(e.getKey()).setEndOffset(e.getValue()))
                    .collect(Collectors.toList())
                );
                if (includeSegments) {
                    snapshot.setLogMetadata(logMetadata(src.logMeta()));
                }
                if (funcVersion > ZERO_ZONE_V0_REQUEST_VERSION) {
                    snapshot.setLastTimestampOffset(timestampOffset(src.lastTimestampOffset()));
                }
                return snapshot;
            });
        }

    }

    static boolean hasSnapshotDeltas(AutomqGetPartitionSnapshotResponseData resp) {
        return resp.topics() != null && !resp.topics().isEmpty();
    }

    static AutomqGetPartitionSnapshotResponseData.LogOffsetMetadata logOffsetMetadata(LogOffsetMetadata src) {
        if (src == null) {
            return null;
        }
        return new AutomqGetPartitionSnapshotResponseData.LogOffsetMetadata().setMessageOffset(src.messageOffset).setRelativePositionInSegment(src.relativePositionInSegment);
    }

    static LogMetadata logMetadata(ElasticLogMeta src) {
        if (src == null) {
            return null;
        }
        LogMetadata logMetadata = new LogMetadata();

        StreamMappingCollection streamMappingCollection = new StreamMappingCollection();
        src.getStreamMap().forEach((streamName, streamId) -> streamMappingCollection.add(new AutomqGetPartitionSnapshotResponseData.StreamMapping().setName(streamName).setStreamId(streamId)));
        logMetadata.setStreamMap(streamMappingCollection);

        List<SegmentMetadata> segments = src.getSegmentMetas().stream().map(PartitionSnapshotsManager::segmentMetadata).collect(Collectors.toList());
        logMetadata.setSegments(segments);

        return logMetadata;
    }

    static SegmentMetadata segmentMetadata(ElasticStreamSegmentMeta src) {
        SegmentMetadata metadata = new SegmentMetadata();
        metadata.setBaseOffset(src.baseOffset())
            .setCreateTimestamp(src.createTimestamp())
            .setLastModifiedTimestamp(src.lastModifiedTimestamp())
            .setStreamSuffix(src.streamSuffix())
            .setLogSize(src.logSize())
            .setLog(sliceRange(src.log()))
            .setTime(sliceRange(src.time()))
            .setTransaction(sliceRange(src.txn()))
            .setFirstBatchTimestamp(src.firstBatchTimestamp())
            .setTimeIndexLastEntry(timestampOffset(src.timeIndexLastEntry()));
        return metadata;
    }

    static AutomqGetPartitionSnapshotResponseData.SliceRange sliceRange(SliceRange src) {
        return new AutomqGetPartitionSnapshotResponseData.SliceRange().setStart(src.start()).setEnd(src.end());
    }

    static AutomqGetPartitionSnapshotResponseData.TimestampOffsetData timestampOffset(
        ElasticStreamSegmentMeta.TimestampOffsetData src) {
        return new AutomqGetPartitionSnapshotResponseData.TimestampOffsetData().setTimestamp(src.timestamp()).setOffset(src.offset());
    }

    static AutomqGetPartitionSnapshotResponseData.TimestampOffsetData timestampOffset(
        TimestampOffset src) {
        return new AutomqGetPartitionSnapshotResponseData.TimestampOffsetData().setTimestamp(src.timestamp).setOffset(src.offset);
    }

    static class PartitionWithVersion {
        Partition partition;
        PartitionSnapshotVersion version;

        public PartitionWithVersion(Partition partition, PartitionSnapshotVersion version) {
            this.partition = partition;
            this.version = version;
        }
    }

    static PartitionListener newPartitionListener(PartitionWithVersion version, Runnable notifySnapshotChanged) {
        return new PartitionListener() {
            @Override
            public void onNewLeaderEpoch(long oldEpoch, long newEpoch) {
                version.version.incrementRecordsVersion();
                notifySnapshotChanged.run();
            }

            @Override
            public void onNewAppend(TopicPartition partition, long offset) {
                version.version.incrementRecordsVersion();
                notifySnapshotChanged.run();
            }
        };
    }

    static LogEventListener newLogEventListener(PartitionWithVersion version, Runnable notifySnapshotChanged) {
        return (segment, event) -> {
            version.version.incrementSegmentsVersion();
            notifySnapshotChanged.run();
        };
    }

    static class PendingLongPoll {
        final AutomqGetPartitionSnapshotRequest request;
        final CompletableFuture<AutomqGetPartitionSnapshotResponse> future;
        final CompletableFuture<Void> triggerCf = new CompletableFuture<>();
        ScheduledFuture<?> timeoutTask;

        PendingLongPoll(AutomqGetPartitionSnapshotRequest request,
            CompletableFuture<AutomqGetPartitionSnapshotResponse> future) {
            this.request = request;
            this.future = future;
        }

        void cancelTimeout() {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
        }
    }

}
