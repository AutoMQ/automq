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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.utils.Threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PartitionSnapshotsManager {
    private static final int NOOP_SESSION_ID = 0;
    private final Map<Integer, Session> sessions = new HashMap<>();
    private final List<PartitionWithVersion> snapshotVersions = new CopyOnWriteArrayList<>();
    private final Time time;
    private final String confirmWalConfig;
    private final ConfirmWAL confirmWAL;

    public PartitionSnapshotsManager(Time time, AutoMQConfig config, ConfirmWAL confirmWAL, Supplier<AutoMQVersion> versionGetter) {
        this.time = time;
        this.confirmWalConfig = config.walConfig();
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
        partition.maybeAddListener(newPartitionListener(partitionWithVersion));
        partition.addLogEventListener(newLogEventListener(partitionWithVersion));
    }

    public void onPartitionClose(Partition partition) {
        snapshotVersions.removeIf(p -> p.partition == partition);
        synchronized (this) {
            sessions.values().forEach(s -> s.onPartitionClose(partition));
        }
    }

    public CompletableFuture<AutomqGetPartitionSnapshotResponse> handle(AutomqGetPartitionSnapshotRequest request) {
        Session session;
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
            }
        }
        AutomqGetPartitionSnapshotResponse resp = session.snapshotsDelta(request.data().version());
        CompletableFuture<Void> commitCf = request.data().requestCommit() ? confirmWAL.commit(0, false) : CompletableFuture.completedFuture(null);
        return commitCf.exceptionally(nil -> null).thenApply(nil -> resp);
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
        sessions.values().removeIf(Session::expired);
    }

    class Session {
        private static final short ZERO_ZONE_V0_REQUEST_VERSION = (short) 0;
        private final int sessionId;
        private int sessionEpoch = 0;
        private final Map<Partition, PartitionSnapshotVersion> synced = new HashMap<>();
        private final List<Partition> removed = new ArrayList<>();
        private long lastGetSnapshotsTimestamp = time.milliseconds();

        public Session(int sessionId) {
            this.sessionId = sessionId;
        }

        public synchronized int sessionEpoch() {
            return sessionEpoch;
        }

        public synchronized AutomqGetPartitionSnapshotResponse snapshotsDelta(short requestVersion) {
            AutomqGetPartitionSnapshotResponseData resp = new AutomqGetPartitionSnapshotResponseData();
            sessionEpoch++;
            resp.setSessionId(sessionId);
            resp.setSessionEpoch(sessionEpoch);
            Map<Uuid, List<PartitionSnapshot>> topic2partitions = new HashMap<>();

            removed.forEach(partition -> {
                PartitionSnapshotVersion version = synced.remove(partition);
                if (version != null) {
                    List<PartitionSnapshot> partitionSnapshots = topic2partitions.computeIfAbsent(partition.topicId().get(), topic -> new ArrayList<>());
                    partitionSnapshots.add(snapshot(partition, version, null));
                }
            });
            removed.clear();

            snapshotVersions.forEach(p -> {
                PartitionSnapshotVersion oldVersion = synced.get(p.partition);
                if (!Objects.equals(p.version, oldVersion)) {
                    List<PartitionSnapshot> partitionSnapshots = topic2partitions.computeIfAbsent(p.partition.topicId().get(), topic -> new ArrayList<>());
                    PartitionSnapshotVersion newVersion = p.version.copy();
                    PartitionSnapshot partitionSnapshot = snapshot(p.partition, oldVersion, newVersion);
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
            if (requestVersion > ZERO_ZONE_V0_REQUEST_VERSION) {
                if (sessionEpoch == 1) {
                    // return the WAL config in the session first response
                    resp.setConfirmWalConfig(confirmWalConfig);
                }
                resp.setConfirmWalEndOffset(confirmWAL.confirmOffset().bufferAsBytes());
            }
            lastGetSnapshotsTimestamp = time.milliseconds();
            return new AutomqGetPartitionSnapshotResponse(resp);
        }

        public synchronized void onPartitionClose(Partition partition) {
            removed.add(partition);
        }

        public synchronized boolean expired() {
            return time.milliseconds() - lastGetSnapshotsTimestamp > 60000;
        }

        private PartitionSnapshot snapshot(Partition partition, PartitionSnapshotVersion oldVersion,
            PartitionSnapshotVersion newVersion) {
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
                snapshot.setLastTimestampOffset(timestampOffset(src.lastTimestampOffset()));
                return snapshot;
            });
        }

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

    static PartitionListener newPartitionListener(PartitionWithVersion version) {
        return new PartitionListener() {
            @Override
            public void onHighWatermarkUpdated(TopicPartition partition, long offset) {
                version.version.incrementRecordsVersion();
            }

            @Override
            public void onFailed(TopicPartition partition) {
            }

            @Override
            public void onDeleted(TopicPartition partition) {
            }
        };
    }

    static LogEventListener newLogEventListener(PartitionWithVersion version) {
        return (segment, event) -> version.version.incrementSegmentsVersion();
    }
}
