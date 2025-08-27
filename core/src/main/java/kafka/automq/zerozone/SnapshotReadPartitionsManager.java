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

package kafka.automq.zerozone;

import kafka.automq.partition.snapshot.SnapshotOperation;
import kafka.cluster.Partition;
import kafka.cluster.PartitionSnapshot;
import kafka.log.streamaspect.LazyStream;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticReplicaManager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.MetadataListener;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class SnapshotReadPartitionsManager implements MetadataListener, ProxyTopologyChangeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadPartitionsManager.class);
    static final long REQUEST_INTERVAL_MS = 1;
    private final KafkaConfig config;
    private final Time time;
    private final ConfirmWALProvider confirmWALProvider;
    private final ElasticReplicaManager replicaManager;
    private final MetadataCache metadataCache;
    private final AsyncSender asyncSender;
    private final Replayer replayer;
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("AUTOMQ_SNAPSHOT_READ", true, LOGGER);
    private final Map<Uuid, String> topicId2name = new ConcurrentHashMap<>();
    private final CacheEventListener cacheEventListener = new CacheEventListener();
    final Map<Integer, Subscriber> subscribers = new HashMap<>();
    // all snapshot read partition changes exec in a single eventloop to ensure the thread-safe.
    final EventLoop eventLoop = new EventLoop("AUTOMQ_SNAPSHOT_READ_WORKER");
    private AutoMQVersion version;

    public SnapshotReadPartitionsManager(KafkaConfig config, Metrics metrics, Time time, ConfirmWALProvider confirmWALProvider,
        ElasticReplicaManager replicaManager, MetadataCache metadataCache, Replayer replayer) {
        this.config = config;
        this.time = time;
        this.confirmWALProvider = confirmWALProvider;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.replayer = replayer;
        this.asyncSender = new AsyncSender.BrokersAsyncSender(config, metrics, "snapshot_read", Time.SYSTEM, "AUTOMQ_SNAPSHOT_READ", new LogContext());
    }

    // test only
    SnapshotReadPartitionsManager(KafkaConfig config, Time time, ConfirmWALProvider confirmWALProvider, ElasticReplicaManager replicaManager,
        MetadataCache metadataCache, Replayer replayer, AsyncSender asyncSender) {
        this.config = config;
        this.time = time;
        this.confirmWALProvider = confirmWALProvider;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.replayer = replayer;
        this.asyncSender = asyncSender;
    }

    @Override
    public void onChange(MetadataDelta delta, MetadataImage image) {
        if (delta.topicsDelta() != null && !delta.topicsDelta().deletedTopicIds().isEmpty()) {
            Set<Uuid> deletedTopicIds = delta.topicsDelta().deletedTopicIds();
            scheduler.schedule(() -> deletedTopicIds.forEach(topicId2name::remove), 1, TimeUnit.MINUTES);
        }
        triggerSubscribersApply();
    }

    public void setVersion(AutoMQVersion newVersion) {
        AutoMQVersion oldVersion = this.version;
        this.version = newVersion;
        if (oldVersion != null && (oldVersion.isZeroZoneV2Supported() != newVersion.isZeroZoneV2Supported())) {
            // reset the subscriber
            resetSubscribers(newVersion);
        }
    }

    private synchronized void triggerSubscribersApply() {
        subscribers.forEach((nodeId, subscriber) -> subscriber.apply());
    }

    private synchronized void resetSubscribers(AutoMQVersion version) {
        Set<Integer> nodes = subscribers.keySet();
        nodes.forEach(nodeId -> subscribers.computeIfPresent(nodeId, (id, subscribe) -> {
            subscribe.close();
            return new Subscriber(subscribe.node, version);
        }));
    }

    private void removePartition(TopicIdPartition topicIdPartition, Partition expected) {
        replicaManager.computeSnapshotReadPartition(topicIdPartition.topicPartition(), (tp, current) -> {
            if (current == null || expected == current) {
                expected.close();
                LOGGER.info("[SNAPSHOT_READ_REMOVE],tp={},epoch={}", topicIdPartition, expected.getLeaderEpoch());
                return null;
            }
            // The expected partition was closed when the current partition put in.
            return current;
        });
    }

    private Optional<Partition> addPartition(TopicIdPartition topicIdPartition, PartitionSnapshot snapshot) {
        AtomicReference<Partition> ref = new AtomicReference<>();
        Supplier<Partition> newPartition = () -> {
            Partition partition = replicaManager.newSnapshotReadPartition(topicIdPartition);
            partition.snapshot(snapshot);
            ref.set(partition);
            LOGGER.info("[SNAPSHOT_READ_ADD],tp={},epoch={}", topicIdPartition, ref.get().getLeaderEpoch());
            return partition;
        };
        replicaManager.computeSnapshotReadPartition(topicIdPartition.topicPartition(), (tp, current) -> {
            if (current == null) {
                return newPartition.get();
            }
            if (!topicIdPartition.topicId().equals(current.topicId().get())) {
                if (metadataCache.getTopicName(topicIdPartition.topicId()).isDefined()) {
                    LOGGER.warn("[SNAPSHOT_READ_ADD],[KICK_OUT_DELETED_TOPIC],tp={},snapshot={},current={}", topicIdPartition, snapshot, current.topicId());
                    current.close();
                    return newPartition.get();
                } else {
                    LOGGER.warn("[SNAPSHOT_READ_ADD],[IGNORE],tp={},snapshot={},", topicIdPartition, snapshot);
                    return current;
                }
            }
            if (snapshot.leaderEpoch() > current.getLeaderEpoch()) {
                // The partition is reassigned from N1 to N2.
                // Both N1 and N2 are subscribed by the current node.
                // The N2 ADD operation is arrived before N1 REMOVE operation.
                LOGGER.warn("[SNAPSHOT_READ_ADD],[REMOVE_OLD_EPOCH],tp={},snapshot={},currentEpoch={}", topicIdPartition, snapshot, current.getLeaderEpoch());
                current.close();
                return newPartition.get();
            } else {
                LOGGER.warn("[SNAPSHOT_READ_ADD],[OLD_EPOCH],tp={},snapshot={},currentEpoch={}", topicIdPartition, snapshot, current.getLeaderEpoch());
            }
            return current;
        });
        return Optional.ofNullable(ref.get());
    }

    @Override
    public synchronized void onChange(Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack) {
        Set<Integer> newSubscribeNodes = calSubscribeNodes(main2proxyByRack, config.nodeId());
        subscribers.entrySet().removeIf(entry -> {
            if (!newSubscribeNodes.contains(entry.getKey())) {
                entry.getValue().close();
                return true;
            } else {
                return false;
            }
        });
        newSubscribeNodes.forEach(nodeId -> {
            if (!subscribers.containsKey(nodeId)) {
                Optional<Node> opt = metadataCache.getNode(nodeId).node(config.interBrokerListenerName().value());
                if (opt.isPresent()) {
                    subscribers.put(nodeId, new Subscriber(opt.get(), version));
                } else {
                    LOGGER.error("[SNAPSHOT_READ_SUBSCRIBE],node={} not found", nodeId);
                }
            }
        });
    }

    public SnapshotReadCache.EventListener cacheEventListener() {
        return cacheEventListener;
    }

    private String getTopicName(Uuid topicId) {
        return topicId2name.computeIfAbsent(topicId, id -> metadataCache.topicIdsToNames().get(id));
    }

    // only for test
    Subscriber newSubscriber(Node node, AutoMQVersion version, SubscriberRequester requester, SubscriberReplayer dataLoader) {
        return new Subscriber(node, version, requester, dataLoader);
    }

    class Subscriber {
        final Node node;
        final Map<TopicIdPartition, Partition> partitions = new HashMap<>();
        boolean closed;
        long appliedCount = 0;
        final AtomicLong applyingCount = new AtomicLong();
        final Queue<OperationBatch> waitingMetadataReadyQueue = new LinkedList<>();
        final Queue<WaitingDataLoadTask> waitingDataLoadedQueue = new LinkedList<>();
        final Queue<SnapshotWithOperation> snapshotWithOperations = new LinkedList<>();
        private final SubscriberRequester requester;
        private final SubscriberReplayer replayer;
        private final AutoMQVersion version;

        public Subscriber(Node node, AutoMQVersion version) {
            this.node = node;
            this.version = version;
            this.requester = new SubscriberRequester(this, node, version, asyncSender, SnapshotReadPartitionsManager.this::getTopicName, eventLoop, time);
            this.replayer = new SubscriberReplayer(confirmWALProvider, SnapshotReadPartitionsManager.this.replayer, node, metadataCache);
            LOGGER.info("[SNAPSHOT_READ_SUBSCRIBE],node={}", node);
            run();
        }

        // only for test
        public Subscriber(Node node, AutoMQVersion version, SubscriberRequester requester, SubscriberReplayer replayer) {
            this.node = node;
            this.version = version;
            this.requester = requester;
            this.replayer = replayer;
        }

        public void apply() {
            applyingCount.incrementAndGet();
            eventLoop.execute(() -> {
                long applyingCount = this.applyingCount.get();
                if (this.appliedCount == applyingCount) {
                    return;
                }
                unsafeRun();
                this.appliedCount = applyingCount;
            });
        }

        public void requestCommit() {
            if (version.isZeroZoneV2Supported()) {
                eventLoop.execute(() -> requester.requestCommit = true);
            }
        }

        public void close() {
            LOGGER.info("[SNAPSHOT_READ_UNSUBSCRIBE],node={}", node);
            eventLoop.execute(() -> {
                closed = true;
                requester.close();
                partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
                partitions.clear();
                snapshotWithOperations.clear();
                replayer.close();
            });
        }

        void run() {
            eventLoop.execute(this::unsafeRun);
        }

        /**
         * Must run in eventLoop.
         */
        void unsafeRun() {
            try {
                this.run0();
            } catch (Throwable e) {
                LOGGER.error("[SNAPSHOT_SUBSCRIBE_ERROR]", e);
                reset();
                scheduler.schedule(this::run, 1, TimeUnit.SECONDS);
            }
        }

        private void run0() {
            if (closed) {
                return;
            }
            // try replay the SSO/WAL data to snapshot-read cache
            tryReplay();
            // after the metadata is ready and data is preload in snapshot-read cache,
            // then apply the snapshot to partition.
            applySnapshot();
        }

        void reset() {
            LOGGER.info("[SNAPSHOT_READ_SUBSCRIBER_RESET],node={}", node);
            partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
            partitions.clear();
            waitingMetadataReadyQueue.clear();
            snapshotWithOperations.clear();
            waitingDataLoadedQueue.clear();
            requester.reset();
        }

        void onNewWalEndOffset(String walConfig, RecordOffset endOffset) {
            replayer.onNewWalEndOffset(walConfig, endOffset);
        }

        void onNewOperationBatch(OperationBatch batch) {
            waitingMetadataReadyQueue.add(batch);
        }

        void applySnapshot() {
            while (!snapshotWithOperations.isEmpty()) {
                SnapshotWithOperation snapshotWithOperation = snapshotWithOperations.peek();
                TopicIdPartition topicIdPartition = snapshotWithOperation.topicIdPartition;

                switch (snapshotWithOperation.operation) {
                    case ADD: {
                        Optional<Partition> partition = addPartition(topicIdPartition, snapshotWithOperation.snapshot);
                        if (partition.isEmpty()) {
                            reset();
                            return;
                        }
                        partition.ifPresent(p -> partitions.put(topicIdPartition, p));
                        snapshotWithOperations.poll();
                        break;
                    }
                    case PATCH: {
                        Partition partition = partitions.get(topicIdPartition);
                        if (partition != null) {
                            partition.snapshot(snapshotWithOperation.snapshot);
                        } else {
                            LOGGER.warn("[SNAPSHOT_READ_PATCH],[SKIP],{}", snapshotWithOperation);
                        }
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

        void tryReplay() {
            // - ZERO_ZONE_V0: Collect all the operation which data metadata is ready in kraft and replay the SSO.
            // - ZERO_ZONE_V1: Directly replay the WAL.
            List<OperationBatch> batches = new ArrayList<>();
            for (; ; ) {
                OperationBatch batch = waitingMetadataReadyQueue.peek();
                if (batch == null) {
                    break;
                }
                if (version.isZeroZoneV2Supported() || checkBatchMetadataReady0(batch, metadataCache)) {
                    waitingMetadataReadyQueue.poll();
                    batches.add(batch);
                } else {
                    break;
                }
            }
            if (batches.isEmpty()) {
                return;
            }

            CompletableFuture<Void> waitingDataLoadedCf;
            if (version.isZeroZoneV2Supported()) {
                waitingDataLoadedCf = replayer.replayWal();
            } else {
                // Trigger incremental SSO data loading.
                waitingDataLoadedCf = replayer.relayObject();
            }
            WaitingDataLoadTask task = new WaitingDataLoadTask(time.milliseconds(), batches, waitingDataLoadedCf);
            waitingDataLoadedQueue.add(task);
            // After the SSO data loads to the snapshot-read cache, then apply operations.
            waitingDataLoadedCf.thenAcceptAsync(nil -> checkDataLoaded(), eventLoop);
        }

        void checkDataLoaded() {
            for (; ; ) {
                WaitingDataLoadTask task = waitingDataLoadedQueue.peek();
                if (task == null || !task.cf.isDone()) {
                    break;
                }
                task.operationBatchList.forEach(batch -> snapshotWithOperations.addAll(batch.operations));
                waitingDataLoadedQueue.poll();
                unsafeRun();
            }
        }
    }

    class CacheEventListener implements SnapshotReadCache.EventListener {
        @Override
        public void onEvent(SnapshotReadCache.Event event) {
            synchronized (SnapshotReadPartitionsManager.this) {
                if (event instanceof SnapshotReadCache.RequestCommitEvent) {
                    Subscriber subscriber = subscribers.get(((SnapshotReadCache.RequestCommitEvent) event).nodeId());
                    if (subscriber != null) {
                        subscriber.requestCommit();
                    }
                }
            }
        }
    }

    static boolean checkBatchMetadataReady0(OperationBatch batch, MetadataCache metadataCache) {
        for (; ; ) {
            if (batch.readyIndex == batch.operations.size() - 1) {
                return true;
            }
            if (isMetadataUnready(batch.operations.get(batch.readyIndex + 1).snapshot.streamEndOffsets(), metadataCache)) {
                return false;
            }
            batch.readyIndex = batch.readyIndex + 1;
        }
    }

    static boolean isMetadataUnready(Map<Long, Long> streamEndOffsets, MetadataCache metadataCache) {
        AtomicBoolean ready = new AtomicBoolean(true);
        streamEndOffsets.forEach((streamId, endOffset) -> {
            if (streamId == LazyStream.NOOP_STREAM_ID) {
                return;
            }
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

    static Set<Integer> calSubscribeNodes(Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack,
        int currentNodeId) {
        Set<Integer> nodes = new HashSet<>();
        main2proxyByRack.forEach((rack, main2proxy) -> {
            main2proxy.forEach((mainNodeId, proxy) -> {
                if (proxy.id() == currentNodeId) {
                    nodes.add(mainNodeId);
                }
            });
        });
        return nodes;
    }

    static class OperationBatch {
        final List<SnapshotWithOperation> operations;
        int readyIndex;

        public OperationBatch() {
            this.operations = new ArrayList<>();
            this.readyIndex = -1;
        }
    }

    static class WaitingDataLoadTask {
        final long timestamp;
        final List<OperationBatch> operationBatchList;
        final CompletableFuture<Void> cf;

        public WaitingDataLoadTask(long timestamp, List<OperationBatch> operationBatchList,
            CompletableFuture<Void> cf) {
            this.timestamp = timestamp;
            this.operationBatchList = operationBatchList;
            this.cf = cf;
        }
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

        @Override
        public String toString() {
            return "SnapshotWithOperation{" +
                "topicIdPartition=" + topicIdPartition +
                ", snapshot=" + snapshot +
                ", operation=" + operation +
                '}';
        }
    }
}