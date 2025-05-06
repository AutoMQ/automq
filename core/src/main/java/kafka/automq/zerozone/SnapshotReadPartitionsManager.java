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
import kafka.log.streamaspect.ElasticLogMeta;
import kafka.log.streamaspect.ElasticStreamSegmentMeta;
import kafka.log.streamaspect.LazyStream;
import kafka.log.streamaspect.SliceRange;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticReplicaManager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
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
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.loader.MetadataListener;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import com.automq.stream.s3.cache.SnapshotReadCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SnapshotReadPartitionsManager implements MetadataListener, ProxyTopologyChangeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotReadPartitionsManager.class);
    static final long REQUEST_INTERVAL_MS = 100;
    private final KafkaConfig config;
    private final Time time;
    private final ElasticReplicaManager replicaManager;
    private final MetadataCache metadataCache;
    private final AsyncSender asyncSender;
    private final Function<List<S3ObjectMetadata>, CompletableFuture<Void>> dataLoader;
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("AUTOMQ_SNAPSHOT_READ", true, LOGGER);
    private final Map<Uuid, String> topicId2name = new ConcurrentHashMap<>();
    final Map<Integer, Subscriber> subscribers = new HashMap<>();
    // all snapshot read partition changes exec in a single eventloop to ensure the thread-safe.
    final EventLoop eventLoop = new EventLoop("AUTOMQ_SNAPSHOT_READ_WORKER");

    public SnapshotReadPartitionsManager(KafkaConfig config, Metrics metrics, Time time,
        ElasticReplicaManager replicaManager, MetadataCache metadataCache) {
        this.config = config;
        this.time = time;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.dataLoader = objects -> SnapshotReadCache.instance().load(objects);
        this.asyncSender = new AsyncSender.BrokersAsyncSender(config, metrics, "snapshot_read", Time.SYSTEM, "AUTOMQ_SNAPSHOT_READ", new LogContext());
    }

    // test only
    SnapshotReadPartitionsManager(KafkaConfig config, Time time, ElasticReplicaManager replicaManager,
        MetadataCache metadataCache, Function<List<S3ObjectMetadata>, CompletableFuture<Void>> dataLoader,
        AsyncSender asyncSender) {
        this.config = config;
        this.time = time;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.dataLoader = dataLoader;
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

    private synchronized void triggerSubscribersApply() {
        subscribers.forEach((nodeId, subscriber) -> subscriber.apply());
    }

    private void removePartition(TopicIdPartition topicIdPartition, Partition expected) {
        replicaManager.computeSnapshotReadPartition(topicIdPartition.topicPartition(), (tp, current) -> {
            if (current == null || expected == current) {
                expected.close();
                LOGGER.info("[SNAPSHOT_READ_REMOVE],{}", topicIdPartition);
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
        if (ref.get() != null) {
            LOGGER.info("[SNAPSHOT_READ_ADD],{}", topicIdPartition);
        }
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
                    subscribers.put(nodeId, new Subscriber(opt.get()));
                } else {
                    LOGGER.error("[SNAPSHOT_READ_SUBSCRIBE],node={} not found", nodeId);
                }
            }
        });
    }

    private String getTopicName(Uuid topicId) {
        return topicId2name.computeIfAbsent(topicId, id -> metadataCache.topicIdsToNames().get(id));
    }

    // only for test
    Subscriber newSubscriber(Node node, SubscriberRequester requester, SubscriberDataLoader dataLoader) {
        return new Subscriber(node, requester, dataLoader);
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
        private final SubscriberDataLoader dataLoader;

        public Subscriber(Node node) {
            this.node = node;
            this.requester = new SubscriberRequester(this, node);
            this.dataLoader = new SubscriberDataLoader(node);
            LOGGER.info("[SNAPSHOT_READ_SUBSCRIBE],node={}", node);
            run();
        }

        // only for test
        public Subscriber(Node node, SubscriberRequester requester, SubscriberDataLoader dataLoader) {
            this.node = node;
            this.requester = requester;
            this.dataLoader = dataLoader;
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

        public void close() {
            LOGGER.info("[SNAPSHOT_READ_UNSUBSCRIBE],node={}", node);
            eventLoop.execute(() -> {
                closed = true;
                requester.close();
                partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
                partitions.clear();
                snapshotWithOperations.clear();
            });
        }

        private void run() {
            eventLoop.execute(this::unsafeRun);
        }

        /**
         * Must run in eventLoop.
         */
        private void unsafeRun() {
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
            // check whether the metadata is ready.
            checkMetadataReady();
            // after the metadata is ready and data is preload in snapshot-read cache,
            // then apply the snapshot to partition.
            applySnapshot();
        }

        private void reset() {
            partitions.forEach(SnapshotReadPartitionsManager.this::removePartition);
            partitions.clear();
            waitingMetadataReadyQueue.clear();
            snapshotWithOperations.clear();
            waitingDataLoadedQueue.clear();
            requester.reset();
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

        void checkMetadataReady() {
            // Collect all the operation which data metadata is ready in kraft.
            List<OperationBatch> batches = new ArrayList<>();
            for (; ; ) {
                OperationBatch batch = waitingMetadataReadyQueue.peek();
                if (batch == null) {
                    break;
                }
                if (checkBatchMetadataReady0(batch, metadataCache)) {
                    waitingMetadataReadyQueue.poll();
                    batches.add(batch);
                } else {
                    break;
                }
            }
            if (batches.isEmpty()) {
                return;
            }
            // Trigger incremental SSO data loading.
            CompletableFuture<Void> waitingDataLoadedCf = dataLoader.waitingDataLoaded();
            WaitingDataLoadTask task = new WaitingDataLoadTask(time.milliseconds(), batches, waitingDataLoadedCf);
            waitingDataLoadedQueue.add(task);
            // After the SSO data loads to the snapshot-read cache, then apply operations.
            waitingDataLoadedCf.thenAccept(nil -> checkDataLoaded());
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

    class SubscriberDataLoader {
        long loadedObjectOrderId = -1L;
        CompletableFuture<Void> lastDataLoadCf = CompletableFuture.completedFuture(null);
        private final Node node;

        public SubscriberDataLoader(Node node) {
            this.node = node;
        }

        public CompletableFuture<Void> waitingDataLoaded() {
            List<S3ObjectMetadata> newObjects = nextObjects().stream().filter(object -> {
                if (object.objectSize() > 200L * 1024 * 1024) {
                    LOGGER.warn("The object {} is bigger than 200MiB, skip load it", object);
                    return false;
                } else {
                    return true;
                }
            }).collect(Collectors.toList());
            if (newObjects.isEmpty()) {
                return lastDataLoadCf;
            }
            long loadedObjectOrderId = this.loadedObjectOrderId;
            return lastDataLoadCf = lastDataLoadCf.thenCompose(nil -> dataLoader.apply(newObjects)).thenAcceptAsync(nil -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("[LOAD_SNAPSHOT_READ_DATA],node={},loadedObjectOrderId={},newObjects={}", node, loadedObjectOrderId, newObjects);
                }
            }, eventLoop);
        }

        private List<S3ObjectMetadata> nextObjects() {
            return nextObjects0(metadataCache, node.id(), loadedObjectOrderId, value -> loadedObjectOrderId = value);
        }
    }

    class SubscriberRequester {
        private boolean closed = false;
        private long lastRequestTime;
        private int sessionId;
        private int sessionEpoch;
        private final Subscriber subscriber;
        private final Node node;

        public SubscriberRequester(Subscriber subscriber, Node node) {
            this.subscriber = subscriber;
            this.node = node;
            request();
        }

        public void reset() {
            sessionId = 0;
            sessionEpoch = 0;
        }

        public void close() {
            closed = true;
        }

        private void request() {
            eventLoop.execute(this::request0);
        }

        private void request0() {
            if (closed) {
                return;
            }
            lastRequestTime = time.milliseconds();
            AutomqGetPartitionSnapshotRequestData data = new AutomqGetPartitionSnapshotRequestData().setSessionId(sessionId).setSessionEpoch(sessionEpoch);
            AutomqGetPartitionSnapshotRequest.Builder builder = new AutomqGetPartitionSnapshotRequest.Builder(data);
            asyncSender.sendRequest(node, builder)
                .thenAcceptAsync(rst -> {
                    handleResponse(rst);
                    subscriber.unsafeRun();
                }, eventLoop)
                .exceptionally(ex -> {
                    LOGGER.error("[SNAPSHOT_SUBSCRIBE_ERROR],node={}", node, ex);
                    return null;
                }).whenComplete((nil, ex) -> {
                    long elapsed = time.milliseconds() - lastRequestTime;
                    if (REQUEST_INTERVAL_MS > elapsed) {
                        scheduler.schedule(() -> eventLoop.execute(this::request), REQUEST_INTERVAL_MS - elapsed, TimeUnit.MILLISECONDS);
                    } else {
                        request();
                    }
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
                subscriber.reset();
            }
            OperationBatch batch = new OperationBatch();
            resp.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                String topicName = getTopicName(topic.topicId());
                if (topicName == null) {
                    subscriber.reset();
                    throw new RuntimeException(String.format("Cannot find topic uuid=%s, the kraft metadata replay delay or the topic is deleted.", topic.topicId()));
                }
                batch.operations.add(convert(new TopicIdPartition(topic.topicId(), partition.partitionIndex(), topicName), partition));
            }));
            subscriber.onNewOperationBatch(batch);
            sessionId = resp.sessionId();
            sessionEpoch = resp.sessionEpoch();
        }
    }

    static List<S3ObjectMetadata> nextObjects0(MetadataCache metadataCache, int nodeId, long loadedObjectOrderId,
        LongConsumer loadedObjectOrderIdUpdater) {
        return metadataCache.safeRun(image -> {
            List<S3ObjectMetadata> newObjects = new ArrayList<>();
            List<S3StreamSetObject> streamSetObjects = image.streamsMetadata().getStreamSetObjects(nodeId);
            S3ObjectsImage objectsImage = image.objectsMetadata();
            long nextObjectOrderId = loadedObjectOrderId;
            if (loadedObjectOrderId == -1L) {
                // try to load the latest 16MB data
                long size = 0;
                for (int i = streamSetObjects.size() - 1; i >= 0 && size < 16 * 1024 * 1024 && newObjects.size() < 8; i--) {
                    S3StreamSetObject sso = streamSetObjects.get(i);
                    S3Object s3object = objectsImage.getObjectMetadata(sso.objectId());
                    size += s3object.getObjectSize();
                    newObjects.add(new S3ObjectMetadata(sso.objectId(), s3object.getObjectSize(), s3object.getAttributes()));
                    nextObjectOrderId = Math.max(nextObjectOrderId, sso.orderId());
                }
            } else {
                for (int i = streamSetObjects.size() - 1; i >= 0; i--) {
                    S3StreamSetObject sso = streamSetObjects.get(i);
                    if (sso.orderId() <= loadedObjectOrderId) {
                        break;
                    }
                    S3Object s3object = objectsImage.getObjectMetadata(sso.objectId());
                    newObjects.add(new S3ObjectMetadata(sso.objectId(), s3object.getObjectSize(), s3object.getAttributes()));
                    nextObjectOrderId = Math.max(nextObjectOrderId, sso.orderId());
                }
            }
            loadedObjectOrderIdUpdater.accept(nextObjectOrderId);
            Collections.reverse(newObjects);
            return newObjects;
        });
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
        if (src == null || src.segments().isEmpty()) {
            // the AutomqGetPartitionSnapshotResponseData's default LogMetadata is an empty LogMetadata.
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

    }
}