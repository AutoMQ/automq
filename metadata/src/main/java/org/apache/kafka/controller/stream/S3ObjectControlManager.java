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

import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectAttributes.Type;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.automq.stream.utils.CollectionHelper;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
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
import org.slf4j.Logger;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

/**
 * The S3ObjectControlManager manages all S3Object's lifecycle, such as apply, create, destroy, etc.
 */
public class S3ObjectControlManager {

    // TODO: config it in properties
    private static final long DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS = 3000L;

    private static final long DEFAULT_INITIAL_DELAY_MS = 5000L;

    private final QuorumController quorumController;
    private final Logger log;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objectsMetadata;

    private final String clusterId;

    private final Config config;

    /**
     * The objectId of the next object to be prepared. (start from 0)
     */
    private final TimelineLong nextAssignedObjectId;

    private final TimelineHashSet<Long /* objectId */> preparedObjects;

    // TODO: support different deletion policies, based on time dimension or space dimension?
    private final Queue<Long/*objectId*/> markDestroyedObjects;

    private final ObjectStorage objectStorage;

    private final List<S3ObjectLifeCycleListener> lifecycleListeners;

    private final ScheduledExecutorService lifecycleCheckTimer;

    private final ObjectCleaner objectCleaner;
    private final TimelineLong s3ObjectSize;

    private long lastCleanStartTimestamp = 0;
    private CompletableFuture<Void> lastCleanCf = CompletableFuture.completedFuture(null);
    private final Supplier<AutoMQVersion> version;

    public S3ObjectControlManager(
        QuorumController quorumController,
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        String clusterId,
        Config config,
        ObjectStorage objectStorage,
        Supplier<AutoMQVersion> version) {
        this.quorumController = quorumController;
        this.log = logContext.logger(S3ObjectControlManager.class);
        this.clusterId = clusterId;
        this.config = config;
        this.nextAssignedObjectId = new TimelineLong(snapshotRegistry);
        this.objectsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.preparedObjects = new TimelineHashSet<>(snapshotRegistry, 0);
        this.s3ObjectSize = new TimelineLong(snapshotRegistry);
        this.markDestroyedObjects = new LinkedBlockingDeque<>();
        this.objectStorage = objectStorage;
        this.version = version;
        this.lifecycleListeners = new ArrayList<>();
        this.lifecycleCheckTimer = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-object-lifecycle-check-timer", true));
        this.lifecycleCheckTimer.scheduleWithFixedDelay(this::triggerCheckEvent,
            DEFAULT_INITIAL_DELAY_MS, DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.objectCleaner = new ObjectCleaner();
        S3StreamKafkaMetricsManager.setS3ObjectCountMapSupplier(() -> Map.of(
            S3StreamKafkaMetricsConstants.S3_OBJECT_PREPARED_STATE, preparedObjects.size(),
            S3StreamKafkaMetricsConstants.S3_OBJECT_MARK_DESTROYED_STATE, markDestroyedObjects.size(),
            S3StreamKafkaMetricsConstants.S3_OBJECT_COMMITTED_STATE, objectsMetadata.size()
                - preparedObjects.size() - markDestroyedObjects.size()));
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

    public void registerListener(S3ObjectLifeCycleListener listener) {
        this.lifecycleListeners.add(listener);
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
        for (int i = 0; i < count; i++) {
            Long objectId = nextAssignedObjectId.get() + i;
            long preparedTs = System.currentTimeMillis();
            long expiredTs = preparedTs + request.timeToLiveInMs();
            S3ObjectRecord record = new S3ObjectRecord()
                .setObjectId(objectId)
                .setObjectState(S3ObjectState.PREPARED.toByte())
                .setPreparedTimeInMs(preparedTs)
                .setExpiredTimeInMs(expiredTs);
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
            .setObjectState(S3ObjectState.COMMITTED.toByte())
            .setPreparedTimeInMs(object.getPreparedTimeInMs())
            .setExpiredTimeInMs(object.getExpiredTimeInMs())
            .setCommittedTimeInMs(committedTs);
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
        for (int i = 0; i < objects.size(); i++) {
            Long objectId = objects.get(i);
            CompactOperations operation = operations.get(i);
            S3Object object = this.objectsMetadata.get(objectId);
            if (object == null || object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                log.error("object {} not exist when mark destroy object", objectId);
                return ControllerResult.of(Collections.emptyList(), false);
            }
            switch (operation) {
                case DELETE: {
                    S3ObjectRecord record = new S3ObjectRecord()
                        .setObjectId(objectId)
                        .setObjectSize(object.getObjectSize())
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                        .setPreparedTimeInMs(object.getPreparedTimeInMs())
                        .setExpiredTimeInMs(object.getExpiredTimeInMs())
                        .setCommittedTimeInMs(object.getCommittedTimeInMs())
                        .setMarkDestroyedTimeInMs(System.currentTimeMillis());
                    if (version.isCompositeObjectSupported()) {
                        record.setAttributes(object.getAttributes());
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
                        .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                        .setPreparedTimeInMs(object.getPreparedTimeInMs())
                        .setExpiredTimeInMs(object.getExpiredTimeInMs())
                        .setCommittedTimeInMs(object.getCommittedTimeInMs())
                        .setMarkDestroyedTimeInMs(System.currentTimeMillis());
                    if (version.isCompositeObjectSupported()) {
                        int attributes = ObjectAttributes.builder(object.getAttributes()).deepDelete().build().attributes();
                        record.setAttributes(attributes);
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

    public void replay(AssignedS3ObjectIdRecord record) {
        nextAssignedObjectId.set(record.assignedS3ObjectId() + 1);
    }

    public void replay(S3ObjectRecord record) {
        String objectKey = ObjectUtils.genKey(0, record.objectId());
        S3Object object = new S3Object(record.objectId(), record.objectSize(), objectKey,
            record.preparedTimeInMs(), record.expiredTimeInMs(), record.committedTimeInMs(), record.markDestroyedTimeInMs(),
            S3ObjectState.fromByte(record.objectState()), record.attributes());
        objectsMetadata.put(record.objectId(), object);
        if (object.getS3ObjectState() == S3ObjectState.PREPARED) {
            preparedObjects.add(object.getObjectId());
        } else {
            preparedObjects.remove(object.getObjectId());
            if (object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                markDestroyedObjects.add(object.getObjectId());
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
        preparedObjects.remove(record.objectId());
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
        List<ApiMessageAndVersion> records = new ArrayList<>();
        List<Long> ttlReachedObjects = new LinkedList<>();
        // check the expired objects
        this.preparedObjects.stream().
            map(objectsMetadata::get).
            filter(S3Object::isExpired).
            forEach(obj -> {
                S3ObjectRecord record = new S3ObjectRecord()
                    .setObjectId(obj.getObjectId())
                    .setObjectState((byte) S3ObjectState.MARK_DESTROYED.ordinal())
                    .setObjectSize(obj.getObjectSize())
                    .setPreparedTimeInMs(obj.getPreparedTimeInMs())
                    .setExpiredTimeInMs(obj.getExpiredTimeInMs())
                    .setCommittedTimeInMs(obj.getCommittedTimeInMs())
                    .setMarkDestroyedTimeInMs(obj.getMarkDestroyedTimeInMs());
                ttlReachedObjects.add(obj.getObjectId());
                // generate the records which mark the expired objects as destroyed
                records.add(new ApiMessageAndVersion(record, (short) 0));
                // generate the records which listener reply for the object-destroy events
                lifecycleListeners.forEach(listener -> {
                    ControllerResult<Void> result = listener.onDestroy(obj.getObjectId());
                    records.addAll(result.records());
                });
            });
        if (!ttlReachedObjects.isEmpty()) {
            log.info("objects TTL is reached, objects={}", ttlReachedObjects);
        }
        // check the mark destroyed objects
        List<S3Object> requiredDeleteKeys = new LinkedList<>();
        while (true) {
            Long objectId = this.markDestroyedObjects.peek();
            if (objectId == null) {
                break;
            }
            S3Object object = this.objectsMetadata.get(objectId);
            if (object == null) {
                // markDestroyedObjects isn't Timeline structure, so it may contains dirty / duplicated objectId
                this.markDestroyedObjects.poll();
                continue;
            }
            if (object.getMarkDestroyedTimeInMs() + (this.config.objectRetentionTimeInSecond() * 1000L) < System.currentTimeMillis()) {
                this.markDestroyedObjects.poll();
                // exceed delete retention time, trigger the truly deletion
                requiredDeleteKeys.add(object);
            } else {
                // the following objects' mark destroyed time is not expired, so break the loop
                break;
            }
        }

        if (!requiredDeleteKeys.isEmpty()) {
            this.lastCleanStartTimestamp = System.currentTimeMillis();
            this.lastCleanCf = this.objectCleaner.clean(requiredDeleteKeys);
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

    public ObjectReader objectReader(long objectId) {
        S3Object object = getObject(objectId);
        if (object == null) {
            return null;
        }
        return ObjectReader.reader(new S3ObjectMetadata(objectId, object.getObjectSize(), object.getAttributes()), objectStorage);
    }

    /**
     * S3Object's lifecycle listener.
     */
    public interface S3ObjectLifeCycleListener {

        /**
         * Notify the listener that the S3Object has been destroyed.
         *
         * @param objectId the destroyed S3Object's id
         * @return the result of the listener, contains the records which should be applied to the raft.
         */
        ControllerResult<Void> onDestroy(Long objectId);
    }

    class ObjectCleaner {
        CompletableFuture<Void> clean(List<S3Object> objects) {
            List<S3Object> deepDeleteCompositeObjects = new LinkedList<>();
            List<S3Object> shallowDeleteObjects = new ArrayList<>(objects.size());
            for (S3Object object : objects) {
                ObjectAttributes attributes = ObjectAttributes.from(object.getAttributes());
                if (attributes.deepDelete() && attributes.type() == Type.Composite) {
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
                .exceptionally(e -> {
                    log.error("Failed to delete the S3Object from S3, objectKeys: {}",
                        objectPaths.stream().map(ObjectPath::key).collect(Collectors.joining(",")), e);
                    return null;
                }).thenAccept(rst -> {
                    List<Long> deletedObjectIds = s3objects.stream().map(S3Object::getObjectId).collect(Collectors.toList());
                    notifyS3ObjectDeleted(deletedObjectIds);
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
