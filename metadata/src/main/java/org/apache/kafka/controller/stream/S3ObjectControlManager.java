/*
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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsConstants;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.apache.kafka.timeline.TimelineLong;

import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectAttributes.Type;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.LocalFileObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.utils.CollectionHelper;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The S3ObjectControlManager manages all S3Object's lifecycle, such as apply, create, destroy, etc.
 */
@SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
public class S3ObjectControlManager {

    // TODO: config it in properties
    private static final long DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS = 1000L;
    private static final long DEFAULT_INITIAL_DELAY_MS = 1000L;
    private static final int MAX_DELETE_BATCH_COUNT = 2000;

    private final QuorumController quorumController;
    private final Logger log;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objectsMetadata;

    private final String clusterId;

    private final Config config;

    /**
     * The objectId of the next object to be prepared. (start from 0)
     */
    private final TimelineLong nextAssignedObjectId;

    private TimelineHashMap<Long /* objectId */, Long /* deadline */> preparedObjects0;
    private TimelineHashMap<Long /* objectId */, Long /* deadline */> preparedObjects1;
    private long lastPreparedObjectsSwitchTimestamp;
    private final HashedWheelTimer preparedObjectsTimer = new HashedWheelTimer(1, TimeUnit.SECONDS);
    final Map<Long, Timeout> preparedObjectsTimeouts = new HashMap<>();
    final Queue<Long> waitingDeadlineCheckPreparedObjects = new ConcurrentLinkedQueue<>();

    private TimelineHashSet<Long/*objectId*/> markDestroyedObjects0;
    private TimelineHashSet<Long/*objectId*/> markDestroyedObjects1;
    private long lastMarkDestroyedObjectsSwitchTimestamp;

    private final ObjectStorage objectStorage;

    private final ScheduledExecutorService lifecycleCheckTimer;

    private final ObjectCleaner objectCleaner;
    private final TimelineLong s3ObjectSize;

    private long lastCleanStartTimestamp = 0;
    private CompletableFuture<Void> lastCleanCf = CompletableFuture.completedFuture(null);
    private final Supplier<AutoMQVersion> version;
    private final Time time;

    public S3ObjectControlManager(
        QuorumController quorumController,
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        String clusterId,
        Config config,
        ObjectStorage objectStorage,
        Supplier<AutoMQVersion> version,
        Time time) {
        this.quorumController = quorumController;
        this.log = logContext.logger(S3ObjectControlManager.class);
        this.clusterId = clusterId;
        this.config = config;
        this.nextAssignedObjectId = new TimelineLong(snapshotRegistry);
        this.objectsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.preparedObjects0 = new TimelineHashMap<>(snapshotRegistry, 0);
        this.preparedObjects1 = new TimelineHashMap<>(snapshotRegistry, 0);
        this.lastPreparedObjectsSwitchTimestamp = time.milliseconds();
        this.s3ObjectSize = new TimelineLong(snapshotRegistry);
        this.markDestroyedObjects0 = new TimelineHashSet<>(snapshotRegistry, 0);
        this.markDestroyedObjects1 = new TimelineHashSet<>(snapshotRegistry, 0);
        this.lastMarkDestroyedObjectsSwitchTimestamp = time.milliseconds();
        this.objectStorage = objectStorage;
        this.version = version;
        this.time = time;
        this.lifecycleCheckTimer = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-object-lifecycle-check-timer", true));
        this.lifecycleCheckTimer.scheduleWithFixedDelay(this::triggerCheckEvent,
            DEFAULT_INITIAL_DELAY_MS, DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.objectCleaner = new ObjectCleaner();
        S3StreamKafkaMetricsManager.setS3ObjectCountMapSupplier(() -> Map.of(
            S3StreamKafkaMetricsConstants.S3_OBJECT_PREPARED_STATE, preparedObjects0.size() + preparedObjects1.size(),
            S3StreamKafkaMetricsConstants.S3_OBJECT_MARK_DESTROYED_STATE, markDestroyedObjects0.size() + markDestroyedObjects1.size(),
            S3StreamKafkaMetricsConstants.S3_OBJECT_COMMITTED_STATE, objectsMetadata.size()
                - preparedObjects0.size() - markDestroyedObjects0.size() - preparedObjects1.size() - markDestroyedObjects1.size()));
        S3StreamKafkaMetricsManager.setS3ObjectSizeSupplier(s3ObjectSize::get);
    }

    private void triggerCheckEvent() {
        if (!quorumController.isActive()) {
            return;
        }
        ControllerRequestContext ctx = new ControllerRequestContext(
            null, null, OptionalLong.empty());
        this.quorumController.checkS3ObjectsLifecycle(ctx).whenComplete((ignore, exp) -> {
            if (exp != null) {
                log.error("Failed to check the S3Object's lifecycle", exp);
            }
        });
    }

    public Long nextAssignedObjectId() {
        return nextAssignedObjectId.get();
    }

    public ControllerResult<PrepareS3ObjectResponseData> prepareObject(PrepareS3ObjectRequestData request) {
        AutoMQVersion version = this.version.get();
        short objectRecordVersion = version.objectRecordVersion();
        // TODO: pre assigned a batch of objectIds in controller
        List<ApiMessageAndVersion> records = new ArrayList<>();
        PrepareS3ObjectResponseData response = new PrepareS3ObjectResponseData();
        int count = request.preparedCount();

        // update assigned stream id
        long newAssignedObjectId = nextAssignedObjectId.get() + count - 1;
        records.add(new ApiMessageAndVersion(new AssignedS3ObjectIdRecord()
            .setAssignedS3ObjectId(newAssignedObjectId), (short) 0));

        long firstAssignedObjectId = nextAssignedObjectId.get();
        long now = time.milliseconds();
        for (int i = 0; i < count; i++) {
            long objectId = nextAssignedObjectId.get() + i;
            long expiredTs = now + request.timeToLiveInMs();
            S3ObjectRecord record = new S3ObjectRecord()
                .setObjectId(objectId)
                .setObjectState(S3ObjectState.PREPARED.toByte());
            if (version.isHugeClusterSupported()) {
                record.setTimestamp(expiredTs);
            } else {
                record.setPreparedTimeInMs(now)
                    .setExpiredTimeInMs(expiredTs);
            }
            records.add(new ApiMessageAndVersion(record, objectRecordVersion));
        }
        response.setFirstS3ObjectId(firstAssignedObjectId);
        return ControllerResult.atomicOf(records, response);
    }

    public ControllerResult<Errors> commitObject(long objectId, long objectSize, long committedTs, int attributes) {
        if (objectId == NOOP_OBJECT_ID) {
            return ControllerResult.of(Collections.emptyList(), Errors.NONE);
        }
        S3Object object = this.objectsMetadata.get(objectId);
        if (object == null) {
            log.error("object {} not exist when commit object", objectId);
            return ControllerResult.of(Collections.emptyList(), Errors.OBJECT_NOT_EXIST);
        }
        // verify the state
        if (object.getS3ObjectState() == S3ObjectState.COMMITTED) {
            log.warn("object {} already committed", objectId);
            return ControllerResult.of(Collections.emptyList(), Errors.REDUNDANT_OPERATION);
        }
        if (object.getS3ObjectState() != S3ObjectState.PREPARED) {
            log.error("object {} is not prepared but try to commit", objectId);
            return ControllerResult.of(Collections.emptyList(), Errors.OBJECT_NOT_EXIST);
        }
        AutoMQVersion version = this.version.get();
        S3ObjectRecord record = new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(objectSize)
            .setObjectState(S3ObjectState.COMMITTED.toByte());
        if (version.isHugeClusterSupported()) {
            record.setTimestamp(committedTs);
        } else {
            record.setCommittedTimeInMs(object.getTimestamp());
        }
        if (version.isObjectAttributesSupported()) {
            record.setAttributes(attributes);
        }
        return ControllerResult.of(List.of(
            new ApiMessageAndVersion(record, version.objectRecordVersion())), Errors.NONE);
    }

    public ControllerResult<Boolean> markDestroyObjects(List<Long> objects) {
        return markDestroyObjects(objects, Collections.nCopies(objects.size(), CompactOperations.DELETE));
    }

    public ControllerResult<Boolean> markDestroyObjects(List<Long> objects, List<CompactOperations> operations) {
        AutoMQVersion version = this.version.get();
        short objectRecordVersion = version.objectRecordVersion();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        long now = time.milliseconds();
        for (int i = 0; i < objects.size(); i++) {
            Long objectId = objects.get(i);
            CompactOperations operation = operations.get(i);
            S3Object object = this.objectsMetadata.get(objectId);
            if (object == null || object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                log.error("object {} not exist when mark destroy object", objectId);
                return ControllerResult.of(Collections.emptyList(), false);
            }
            int attributes = object.getAttributes();
            if (object.getS3ObjectState() == S3ObjectState.PREPARED) {
                attributes = ObjectAttributes.builder(attributes).bucket(ObjectAttributes.MATCH_ALL_BUCKET).build().attributes();
            }

            switch (operation) {
                case DELETE: {
                    S3ObjectRecord record = new S3ObjectRecord()
                        .setObjectId(objectId)
                        .setObjectSize(object.getObjectSize())
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte());
                    if (version.isHugeClusterSupported()) {
                        record.setTimestamp(now);
                    } else {
                        record.setMarkDestroyedTimeInMs(now);
                    }
                    if (version.isCompositeObjectSupported()) {
                        record.setAttributes(attributes);
                    }
                    records.add(new ApiMessageAndVersion(record, objectRecordVersion));
                    break;
                }
                case KEEP_DATA: {
                    records.add(new ApiMessageAndVersion(new RemoveS3ObjectRecord().setObjectId(objectId), (short) 0));
                    break;
                }
                case DEEP_DELETE: {
                    S3ObjectRecord record = new S3ObjectRecord()
                        .setObjectId(objectId)
                        .setObjectSize(object.getObjectSize())
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte());
                    if (version.isHugeClusterSupported()) {
                        record.setTimestamp(now);
                    } else {
                        record.setMarkDestroyedTimeInMs(now);
                    }
                    if (version.isCompositeObjectSupported()) {
                        int newAttributes = ObjectAttributes.builder(attributes).deepDelete().build().attributes();
                        record.setAttributes(newAttributes);
                    }
                    records.add(new ApiMessageAndVersion(record, objectRecordVersion));
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }
        return ControllerResult.atomicOf(records, true);
    }

    public ControllerResult<Errors> replaceCommittedObject(long objectId, int attributes) {
        S3Object object = this.objectsMetadata.get(objectId);
        if (object == null) {
            return ControllerResult.of(Collections.emptyList(), Errors.OBJECT_NOT_EXIST);
        }
        // verify the state
        if (object.getS3ObjectState() != S3ObjectState.COMMITTED) {
            return ControllerResult.of(Collections.emptyList(), Errors.OBJECT_NOT_COMMITED);
        }
        AutoMQVersion version = this.version.get();
        S3ObjectRecord record = new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(object.getObjectSize())
            .setObjectState(S3ObjectState.COMMITTED.toByte());
        record.setTimestamp(object.getObjectSize());
        record.setAttributes(attributes);
        return ControllerResult.of(List.of(
            new ApiMessageAndVersion(record, version.objectRecordVersion())), Errors.NONE);
    }

    public void replay(AssignedS3ObjectIdRecord record) {
        nextAssignedObjectId.set(record.assignedS3ObjectId() + 1);
    }

    public void replay(S3ObjectRecord record) {
        S3Object object = S3Object.of(record);
        objectsMetadata.put(record.objectId(), object);
        long objectId = object.getObjectId();
        if (object.getS3ObjectState() == S3ObjectState.PREPARED) {
            preparedObjects0.put(objectId, object.getTimestamp());
            preparedObjectsTimeouts.put(objectId, newPreparedObjectTimeout(objectId, object.getTimestamp()));
        } else {
            removePreparedObject(objectId);
            if (object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                markDestroyedObjects0.add(object.getObjectId());
                ObjectAttributes attributes = ObjectAttributes.from(object.getAttributes());
                if (attributes.type() == Type.Composite && !attributes.deepDelete()) {
                    // It means the composite object is compacted by another composite object.
                    // So decrease size here to make the s3 size metrics more precise.
                    s3ObjectSize.set(s3ObjectSize.get() - record.objectSize());
                }
            } else {
                // object committed
                s3ObjectSize.set(s3ObjectSize.get() + record.objectSize());
            }
        }
    }

    public void replay(RemoveS3ObjectRecord record) {
        S3Object object = objectsMetadata.remove(record.objectId());
        if (object != null) {
            ObjectAttributes attributes = ObjectAttributes.from(object.getAttributes());
            // The DELETE composite object's size will be decrease when the object status becomes MARK_DESTROYED.
            if (!(attributes.type() == Type.Composite && !attributes.deepDelete())) {
                s3ObjectSize.set(s3ObjectSize.get() - object.getObjectSize());
            }
        }
        removePreparedObject(record.objectId());
        removeMarkDestroyedObject(record.objectId());
    }

    /**
     * Check the S3Object's lifecycle, mark destroy the expired objects and trigger truly deletion for the mark destroyed objects.
     *
     * @return the result of the check, contains the records which should be applied to the raft.
     */
    public ControllerResult<Void> checkS3ObjectsLifecycle() {
        if (!lastCleanCf.isDone()) {
            log.info("the last deletion[timestamp={}] is not finished, skip this check", lastCleanStartTimestamp);
            return ControllerResult.of(Collections.emptyList(), null);
        }
        AutoMQVersion version = this.version.get();
        short objectRecordVersion = version.objectRecordVersion();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        List<Long> ttlReachedObjects = new LinkedList<>();
        long now = time.milliseconds();

        // check the prepared object deadline
        for (int i = 0; i < MAX_DELETE_BATCH_COUNT; i++) {
            // only allow checking MAX_DELETE_BATCH_COUNT objects in one round to avoid blocking the event loop
            Long waitingDeadlineCheckPreparedObject = waitingDeadlineCheckPreparedObjects.poll();
            if (waitingDeadlineCheckPreparedObject == null) {
                break;
            }
            preparedObjectsTimeouts.remove(waitingDeadlineCheckPreparedObject);
            S3Object obj = objectsMetadata.get(waitingDeadlineCheckPreparedObject);
            if (obj == null) {
                continue;
            }
            if (obj.isExpired(now)) {
                S3ObjectRecord record = new S3ObjectRecord()
                    .setObjectId(obj.getObjectId())
                    .setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal())
                    .setObjectSize(obj.getObjectSize());
                if (version.isHugeClusterSupported()) {
                    record.setTimestamp(0);
                } else {
                    record.setMarkDestroyedTimeInMs(obj.getTimestamp());
                }
                if (version.isObjectAttributesSupported()) {
                    record.setAttributes(ObjectAttributes.builder().bucket(ObjectAttributes.MATCH_ALL_BUCKET).build().attributes());
                }
                ttlReachedObjects.add(obj.getObjectId());
                // generate the records which mark the expired objects as destroyed
                records.add(new ApiMessageAndVersion(record, objectRecordVersion));
            } else {
                // reschedule the deadline check
                preparedObjectsTimeouts.put(obj.getObjectId(), newPreparedObjectTimeout(obj.getObjectId(), obj.getTimestamp()));
            }
        }
        if (now - lastPreparedObjectsSwitchTimestamp > TimeUnit.MINUTES.toMillis(60)) {
            // The preparedObjectsTimeouts isn't a Timeline struct, so it's not consistent with the preparedObjects0/1.
            // Consider the following scenario:
            // 1. Prepare(obj1), add timeout check.
            // 2. Commit(obj1), cancel timeout check.
            // 3. The image is reset back to step 1, and no commit.
            // 4. The obj1 timeout check is missing.
            // So we try to add missing timeout check when swap the preparedObjects0/1.
            TimelineHashMap<Long, Long> tmp = preparedObjects1;
            preparedObjects1 = preparedObjects0;
            preparedObjects0 = tmp;
            tmp.forEach((objectId, deadline) ->
                preparedObjectsTimeouts.computeIfAbsent(objectId, id -> newPreparedObjectTimeout(objectId, deadline))
            );
            lastPreparedObjectsSwitchTimestamp = now;
        }
        if (!ttlReachedObjects.isEmpty()) {
            log.info("objects TTL is reached, objects={}", ttlReachedObjects);
        }

        // The mark destroyed object will be truly deleted after the [retention time, 2 * retention time]
        if (now - lastMarkDestroyedObjectsSwitchTimestamp > this.config.objectRetentionTimeInSecond() * 1000L) {
            TimelineHashSet<Long> expired = this.markDestroyedObjects1;
            boolean swap = true;
            if (!expired.isEmpty()) {
                List<Long> fastDeleteObjects = new ArrayList<>();
                int expiredSize = expired.size();
                if (expiredSize > MAX_DELETE_BATCH_COUNT) {
                    swap = false;
                }
                List<S3Object> requiredDeleteObjects = new ArrayList<>(Math.min(expiredSize, MAX_DELETE_BATCH_COUNT));
                int deleteCount = 0;
                for (Long objectId : expired) {
                    S3Object s3Object = objectsMetadata.get(objectId);
                    if (s3Object == null) {
                        fastDeleteObjects.add(objectId);
                    } else {
                        requiredDeleteObjects.add(s3Object);
                    }
                    if (++deleteCount >= MAX_DELETE_BATCH_COUNT) {
                        // only allow checking MAX_DELETE_BATCH_COUNT objects in one round to avoid blocking the event loop
                        break;
                    }
                }
                fastDeleteObjects.forEach(expired::remove);
                this.lastCleanCf = this.objectCleaner.clean(requiredDeleteObjects);
                this.lastCleanStartTimestamp = now;
            }
            if (swap) {
                // When the markDestroyedObjects1 could be completely deleted in MAX_DELETE_BATCH_COUNT, then swap the markDestroyedObjects0/1.
                this.markDestroyedObjects1 = markDestroyedObjects0;
                this.markDestroyedObjects0 = expired;
                this.lastMarkDestroyedObjectsSwitchTimestamp = now;
            }
        }
        return ControllerResult.of(records, null);
    }

    /**
     * Generate RemoveS3ObjectRecord for the deleted S3Objects.
     *
     * @param deletedObjectIds the deleted S3Objects' ids
     * @return the result of the generation, contains the records which should be applied to the raft.
     */
    public ControllerResult<Void> notifyS3ObjectDeleted(List<Long> deletedObjectIds) {
        List<ApiMessageAndVersion> records = deletedObjectIds
            .stream()
            .map(objectId -> new ApiMessageAndVersion(new RemoveS3ObjectRecord().setObjectId(objectId), (short) 0))
            .collect(Collectors.toList());
        return ControllerResult.of(records, null);
    }

    public Map<Long, S3Object> objectsMetadata() {
        return objectsMetadata;
    }

    public S3Object getObject(Long objectId) {
        return this.objectsMetadata.get(objectId);
    }

    public Optional<ObjectReader> objectReader(long objectId) {
        S3Object object = getObject(objectId);
        if (object == null) {
            return Optional.empty();
        }
        return Optional.of(ObjectReader.reader(new S3ObjectMetadata(objectId, object.getObjectSize(), object.getAttributes()), objectStorage));
    }

    private Timeout newPreparedObjectTimeout(long objectId, long deadline) {
        return preparedObjectsTimer.newTimeout(
            t -> handlePreparedObjectTimeout(objectId), Math.max(time.milliseconds() - deadline, TimeUnit.MINUTES.toMillis(30)),
            TimeUnit.MILLISECONDS
        );
    }

    void handlePreparedObjectTimeout(long objectId) {
        waitingDeadlineCheckPreparedObjects.add(objectId);
    }

    private void removePreparedObject(long objectId) {
        preparedObjects0.remove(objectId);
        preparedObjects1.remove(objectId);
        preparedObjectsTimeouts.computeIfPresent(objectId, (id, timeout) -> {
            timeout.cancel();
            return null;
        });
    }

    private void removeMarkDestroyedObject(long objectId) {
        markDestroyedObjects0.remove(objectId);
        markDestroyedObjects1.remove(objectId);
    }

    class ObjectCleaner {
        CompletableFuture<Void> clean(List<S3Object> objects) {
            List<S3Object> ignoredObjects = new LinkedList<>();
            List<S3Object> deepDeleteCompositeObjects = new LinkedList<>();
            List<S3Object> shallowDeleteObjects = new ArrayList<>(objects.size());
            for (S3Object object : objects) {
                ObjectAttributes attributes = ObjectAttributes.from(object.getAttributes());
                if (attributes.bucket() == LocalFileObjectStorage.BUCKET_ID) {
                    ignoredObjects.add(object);
                } else if (attributes.deepDelete() && attributes.type() == Type.Composite) {
                    deepDeleteCompositeObjects.add(object);
                } else {
                    shallowDeleteObjects.add(object);
                }
            }

            List<CompletableFuture<Void>> cfList = new LinkedList<>();
            // Delete the objects
            batchDelete(shallowDeleteObjects, this::shallowlyDelete, cfList);
            // Delete the composite object and it's linked objects
            batchDelete(deepDeleteCompositeObjects, this::deepDelete, cfList);
            // Delete the local file objects
            batchDelete(ignoredObjects, this::noopDelete, cfList);

            return CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0]));
        }

        private void batchDelete(List<S3Object> objects,
            Function<List<S3Object>, CompletableFuture<Void>> deleteFunc,
            List<CompletableFuture<Void>> cfList) {
            if (objects.isEmpty()) {
                return;
            }
            CollectionHelper.groupListByBatchSizeAsStream(objects, AwsObjectStorage.AWS_DEFAULT_BATCH_DELETE_OBJECTS_NUMBER)
                .map(deleteFunc)
                .forEach(cfList::add);
        }

        private CompletableFuture<Void> shallowlyDelete(List<S3Object> s3objects) {
            List<ObjectPath> objectPaths = s3objects.stream().map(o -> new ObjectPath(o.bucket(), o.getObjectKey())).collect(Collectors.toList());
            return objectStorage.delete(objectPaths)
                .thenAccept(rst -> {
                    List<Long> deletedObjectIds = s3objects.stream().map(S3Object::getObjectId).collect(Collectors.toList());
                    notifyS3ObjectDeleted(deletedObjectIds);
                }).exceptionally(e -> {
                    log.error("Failed to delete the S3Object from S3, objectKeys: {}",
                        objectPaths.stream().map(ObjectPath::key).collect(Collectors.joining(",")), e);
                    return null;
                });
        }

        private CompletableFuture<Void> deepDelete(List<S3Object> s3Objects) {
            if (s3Objects.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            List<CompletableFuture<Void>> cfList = new LinkedList<>();
            for (S3Object object : s3Objects) {
                S3ObjectMetadata metadata = new S3ObjectMetadata(object.getObjectId(), object.getAttributes());
                cfList.add(CompositeObject.delete(metadata, objectStorage));
            }
            CompletableFuture<Void> allCf = CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0]));
            allCf.thenAccept(rst -> {
                List<Long> deletedObjectIds = s3Objects.stream().map(S3Object::getObjectId).collect(Collectors.toList());
                notifyS3ObjectDeleted(deletedObjectIds);
            });
            return allCf;
        }

        private CompletableFuture<Void> noopDelete(List<S3Object> s3objects) {
            List<Long> deletedObjectIds = s3objects.stream().map(S3Object::getObjectId).collect(Collectors.toList());
            return CompletableFuture.completedFuture(null).thenAccept(rst -> notifyS3ObjectDeleted(deletedObjectIds));
        }

        private void notifyS3ObjectDeleted(List<Long> deletedObjectIds) {
            // notify the controller an objects deletion event to drive the removal of the objects
            ControllerRequestContext ctx = new ControllerRequestContext(
                null, null, OptionalLong.empty());
            quorumController.notifyS3ObjectDeleted(ctx, deletedObjectIds).whenComplete((ignore, exp) -> {
                if (exp != null) {
                    log.error("Failed to notify the controller the S3Object deletion event, objectIds: {}",
                        Arrays.toString(deletedObjectIds.toArray()), exp);
                }
            });
        }
    }

}
