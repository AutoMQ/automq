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

package kafka.automq.zonerouter;

import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.automq.tmp.AsyncSender;
import kafka.cluster.Partition;
import kafka.cluster.PartitionSnapshot;
import kafka.log.streamaspect.ElasticLogMeta;
import kafka.log.streamaspect.ElasticStreamSegmentMeta;
import kafka.log.streamaspect.SliceRange;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticReplicaManager;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotRequestData;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotRequest;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.MetadataListener;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotReadPartitionsManager implements MetadataListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadPartitionsManager.class);
    private final KafkaConfig config;
    private final Time time;
    private final ElasticReplicaManager replicaManager;
    private final MetadataCache metadataCache;
    private final AsyncSender asyncSender;
    private final Map<Integer, Subscriber> subscribers = new HashMap<>();
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("AUTOMQ_SNAPSHOT_READ", true, LOGGER);
    // all snapshot read partition changes exec in a single eventloop to ensure the thread-safe.
    private final EventLoop eventLoop = new EventLoop("AUTOMQ_SNAPSHOT_READ_WORKER");

    public SnapshotReadPartitionsManager(KafkaConfig config, Metrics metrics, Time time,
        ElasticReplicaManager replicaManager, MetadataCache metadataCache) {
        this.config = config;
        this.time = time;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.asyncSender = new AsyncSender.BrokersAsyncSender(config, metrics, Time.SYSTEM, "AUTOMQ_SNAPSHOT_READ", new LogContext());
    }

    public synchronized void subscribe(Node node) {
        subscribers.computeIfAbsent(node.id(), n -> new Subscriber(node));
    }

    public synchronized void unsubscribe(int nodeId) {
        Subscriber subscriber = subscribers.remove(nodeId);
        if (subscriber != null) {
            subscriber.close();
        }
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        triggerSubscribersApply();
    }

    private synchronized void triggerSubscribersApply() {
        subscribers.forEach((nodeId, subscriber) -> subscriber.applySnapshot());
    }

    private void removePartition(TopicIdPartition topicIdPartition, Partition expected) {
        replicaManager.computeSnapshotReadPartition(topicIdPartition.topicPartition(), (tp, current) -> {
            if (current == null || expected == current) {
                expected.close();
                return null;
            }
            // The expected partition was closed when the current partition put in.
            return current;
        });
    }

    private Optional<Partition> addPartition(TopicIdPartition topicIdPartition, PartitionSnapshot snapshot) {
        AtomicReference<Partition> ref = new AtomicReference<>();
        Supplier<Partition> newPartition = () -> {
            Partition partition = Partition.apply(topicIdPartition, time, replicaManager);
            ref.set(partition);
            return partition;
        };
        replicaManager.computeSnapshotReadPartition(topicIdPartition.topicPartition(), (tp, current) -> {
            if (current == null || snapshot.leaderEpoch() > current.getLeaderEpoch()) {
                if (current != null) {
                    current.close();
                }
                return newPartition.get();
            }
            return current;
        });
        return Optional.ofNullable(ref.get());
    }

    class Subscriber {
        private static final long REQUEST_INTERVAL_MS = 500;
        private final Node node;
        private final Queue<SnapshotWithOperation> snapshotWithOperations = new LinkedBlockingQueue<>();
        private final Map<TopicIdPartition, Partition> partitions = new HashMap<>();
        private int sessionId;
        private int sessionEpoch;
        private boolean closed;
        private long lastRequestTime;

        public Subscriber(Node node) {
            this.node = node;
            run();
        }

        public void apply() {
            eventLoop.execute(this::applySnapshot);
        }

        public void close() {
            eventLoop.execute(() -> {
                closed = true;
                partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
                partitions.clear();
                snapshotWithOperations.clear();
            });
        }

        private void run() {
            eventLoop.execute(() -> {
                try {
                    this.run0();
                } catch (Throwable e) {
                    LOGGER.error("[SNAPSHOT_SUBSCRIBE_ERROR]", e);
                    scheduler.schedule(this::run, 1, TimeUnit.SECONDS);
                }
            });
        }

        private void run0() {
            if (closed) {
                return;
            }
            if (snapshotWithOperations.isEmpty()) {
                applySnapshot();
            } else {
                long elapsed = time.milliseconds() - lastRequestTime;
                if (REQUEST_INTERVAL_MS > elapsed) {
                    scheduler.schedule(this::run, REQUEST_INTERVAL_MS - elapsed, TimeUnit.MILLISECONDS);
                } else {
                    request();
                }
            }
        }

        private void reset() {
            sessionId = 0;
            sessionEpoch = 0;
            snapshotWithOperations.clear();
        }

        private void applySnapshot() {
            while (!snapshotWithOperations.isEmpty()) {
                SnapshotWithOperation snapshotWithOperation = snapshotWithOperations.peek();
                TopicIdPartition topicIdPartition = snapshotWithOperation.topicIdPartition;

                switch (snapshotWithOperation.operation) {
                    case ADD: {
                        if (isMetadataUnready(snapshotWithOperation.snapshot.streamEndOffsets())) {
                            return;
                        }
                        Optional<Partition> partition = addPartition(topicIdPartition, snapshotWithOperation.snapshot);
                        partition.ifPresent(value -> partitions.put(topicIdPartition, value));
                        snapshotWithOperations.poll();
                        break;
                    }
                    case PATCH: {
                        if (isMetadataUnready(snapshotWithOperation.snapshot.streamEndOffsets())) {
                            return;
                        }
                        Partition partition = partitions.get(topicIdPartition);
                        if (partition == null) {
                            throw new IllegalStateException(String.format( "Cannot find partition=%s", topicIdPartition));
                        }
                        if (isMetadataUnready(snapshotWithOperation.snapshot.streamEndOffsets())) {
                            return;
                        }
                        partition.snapshot(snapshotWithOperation.snapshot);
                        snapshotWithOperations.poll();
                        break;
                    }
                    case REMOVE: {
                        Partition partition = partitions.remove(topicIdPartition);
                        if (partition != null) {
                            removePartition(topicIdPartition, partition);
                        }
                        snapshotWithOperations.poll();
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("SnapshotOperation " + snapshotWithOperation.operation + " is not supported");
                }
            }
        }

        private boolean isMetadataUnready(Map<Long, Long> streamEndOffsets) {
            AtomicBoolean ready = new AtomicBoolean(true);
            streamEndOffsets.forEach((streamId, endOffset) -> {
                OptionalLong opt = metadataCache.getStreamEndOffset(streamId);
                if (opt.isEmpty()) {
                    throw new RuntimeException(String.format("Cannot find streamId=%s, the kraft metadata replay delay or the topic is deleted.", streamId));
                }
                long endOffsetInKraft = opt.getAsLong();
                if (endOffsetInKraft < endOffset) {
                    ready.set(false);
                }
            });
            return !ready.get();
        }

        private void request() {
            lastRequestTime = time.milliseconds();
            AutomqGetPartitionSnapshotRequestData data = new AutomqGetPartitionSnapshotRequestData().setSessionId(sessionId).setSessionEpoch(sessionEpoch);
            AutomqGetPartitionSnapshotRequest.Builder builder = new AutomqGetPartitionSnapshotRequest.Builder(data);
            asyncSender.sendRequest(node, builder)
                .thenAcceptAsync(this::handleResponse, eventLoop)
                .exceptionally(ex -> {
                    LOGGER.error("[SNAPSHOT_SUBSCRIBE_ERROR],node={}", node, ex);
                    scheduler.schedule(this::run, 1, TimeUnit.SECONDS);
                    return null;
                });
        }

        private void handleResponse(ClientResponse clientResponse) {
            if (closed) {
                return;
            }
            if (!clientResponse.hasResponse()) {
                LOGGER.error("[GET_SNAPSHOTS],[NO_RESPONSE],response={}", clientResponse);
                return;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("[GET_SNAPSHOTS],[RESPONSE],response={}", clientResponse);
            }
            AutomqGetPartitionSnapshotResponse zoneRouterResponse = (AutomqGetPartitionSnapshotResponse) clientResponse.responseBody();
            AutomqGetPartitionSnapshotResponseData resp = zoneRouterResponse.data();
            if (resp.errorCode() != Errors.NONE.code()) {
                LOGGER.error("[GET_SNAPSHOTS],[ERROR],response={}", resp);
                return;
            }
            if (resp.sessionId() != sessionId) {
                // switch to a new session
                partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
                partitions.clear();
                snapshotWithOperations.clear();
            }
            resp.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                String topicName = metadataCache.topicIdsToNames().get(topic.topicId());
                if (topicName == null) {
                    reset();
                    throw new RuntimeException(String.format("Cannot find topic uuid=%s, the kraft metadata replay delay or the topic is deleted.", topic.topicId()));
                }
                snapshotWithOperations.add(convert(new TopicIdPartition(topic.topicId(), partition.partitionIndex(), topicName), partition));
            }));
            sessionId = resp.sessionId();
            sessionEpoch = resp.sessionEpoch();
            run0();
        }

    }

    static SnapshotWithOperation convert(TopicIdPartition topicIdPartition,
        AutomqGetPartitionSnapshotResponseData.PartitionSnapshot src) {
        PartitionSnapshot.Builder snapshot = PartitionSnapshot.builder();
        snapshot.leaderEpoch(src.leaderEpoch());
        snapshot.logMeta(convert(src.logMetadata()));
        snapshot.firstUnstableOffset(convert(src.firstUnstableOffset()));
        snapshot.logEndOffset(convert(src.logEndOffset()));
        src.streamMetadata().forEach(m -> snapshot.streamEndOffset(m.streamId(), m.endOffset()));

        SnapshotOperation operation = SnapshotOperation.parse(src.operation());
        return new SnapshotWithOperation(topicIdPartition, snapshot.build(), operation);
    }

    static ElasticLogMeta convert(AutomqGetPartitionSnapshotResponseData.LogMetadata src) {
        if (src == null) {
            return null;
        }
        ElasticLogMeta logMeta = new ElasticLogMeta();
        logMeta.setStreamMap(src.streamMap().stream().collect(Collectors.toMap(AutomqGetPartitionSnapshotResponseData.StreamMapping::name, AutomqGetPartitionSnapshotResponseData.StreamMapping::streamId)));
        src.segments().forEach(m -> logMeta.getSegmentMetas().add(convert(m)));
        return logMeta;
    }

    static ElasticStreamSegmentMeta convert(AutomqGetPartitionSnapshotResponseData.SegmentMetadata src) {
        ElasticStreamSegmentMeta meta = new ElasticStreamSegmentMeta();
        meta.baseOffset(src.baseOffset());
        meta.createTimestamp(src.createTimestamp());
        meta.lastModifiedTimestamp(src.lastModifiedTimestamp());
        meta.streamSuffix(src.streamSuffix());
        meta.logSize(src.logSize());
        meta.log(convert(src.log()));
        meta.time(convert(src.time()));
        meta.txn(convert(src.transaction()));
        meta.firstBatchTimestamp(src.firstBatchTimestamp());
        meta.timeIndexLastEntry(convert(src.timeIndexLastEntry()));
        return meta;
    }

    static SliceRange convert(AutomqGetPartitionSnapshotResponseData.SliceRange src) {
        return SliceRange.of(src.start(), src.end());
    }

    static ElasticStreamSegmentMeta.TimestampOffsetData convert(
        AutomqGetPartitionSnapshotResponseData.TimestampOffsetData src) {
        return ElasticStreamSegmentMeta.TimestampOffsetData.of(src.timestamp(), src.offset());
    }

    static LogOffsetMetadata convert(AutomqGetPartitionSnapshotResponseData.LogOffsetMetadata src) {
        if (src == null) {
            return null;
        }
        // The segment offset should be fill in Partition#snapshot
        return new LogOffsetMetadata(src.messageOffset(), -1, src.relativePositionInSegment());
    }

    static class SnapshotWithOperation {
        final TopicIdPartition topicIdPartition;
        final PartitionSnapshot snapshot;
        final SnapshotOperation operation;

        public SnapshotWithOperation(TopicIdPartition topicIdPartition, PartitionSnapshot snapshot,
            SnapshotOperation operation) {
            this.topicIdPartition = topicIdPartition;
            this.snapshot = snapshot;
            this.operation = operation;
        }

    }

}