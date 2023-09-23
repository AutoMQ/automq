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

import java.util.Arrays;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.operator.S3Operator;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.S3Config;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.apache.kafka.timeline.TimelineLong;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;


/**
 * The S3ObjectControlManager manages all S3Object's lifecycle, such as apply, create, destroy, etc.
 */
public class S3ObjectControlManager {

    // TODO: config it in properties
    private static final long DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS = 3000L;

    private static final long DEFAULT_INITIAL_DELAY_MS = 5000L;

    private final QuorumController quorumController;
    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objectsMetadata;

    private final String clusterId;

    private final S3Config config;

    /**
     * The objectId of the next object to be prepared. (start from 0)
     */
    private final TimelineLong nextAssignedObjectId;

    private final TimelineHashSet<Long /* objectId */> preparedObjects;

    // TODO: support different deletion policies, based on time dimension or space dimension?
    private final Queue<Long/*objectId*/> markDestroyedObjects;

    private final S3Operator operator;

    private final List<S3ObjectLifeCycleListener> lifecycleListeners;

    private final ScheduledExecutorService lifecycleCheckTimer;

    public S3ObjectControlManager(
            QuorumController quorumController,
            SnapshotRegistry snapshotRegistry,
            LogContext logContext,
            String clusterId,
            S3Config config,
            S3Operator operator) {
        this.quorumController = quorumController;
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(S3ObjectControlManager.class);
        this.clusterId = clusterId;
        this.config = config;
        this.nextAssignedObjectId = new TimelineLong(snapshotRegistry);
        this.objectsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.preparedObjects = new TimelineHashSet<>(snapshotRegistry, 0);
        this.markDestroyedObjects = new LinkedBlockingDeque<>();
        this.operator = operator;
        this.lifecycleListeners = new ArrayList<>();
        this.lifecycleCheckTimer = Executors.newSingleThreadScheduledExecutor();
        this.lifecycleCheckTimer.scheduleWithFixedDelay(this::triggerCheckEvent,
                DEFAULT_INITIAL_DELAY_MS, DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void triggerCheckEvent() {
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
            records.add(new ApiMessageAndVersion(record, (short) 0));
        }
        response.setFirstS3ObjectId(firstAssignedObjectId);
        return ControllerResult.atomicOf(records, response);
    }

    public ControllerResult<Errors> commitObject(long objectId, long objectSize, long committedTs) {
        if (objectId == NOOP_OBJECT_ID) {
            return ControllerResult.of(Collections.emptyList(), Errors.NONE);
        }
        S3Object object = this.objectsMetadata.get(objectId);
        if (object == null) {
            log.error("object {} not exist when commit wal object", objectId);
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
        S3ObjectRecord record = new S3ObjectRecord()
                .setObjectId(objectId)
                .setObjectSize(objectSize)
                .setObjectState(S3ObjectState.COMMITTED.toByte())
                .setPreparedTimeInMs(object.getPreparedTimeInMs())
                .setExpiredTimeInMs(object.getExpiredTimeInMs())
                .setCommittedTimeInMs(committedTs);
        return ControllerResult.of(List.of(
                new ApiMessageAndVersion(record, (short) 0)), Errors.NONE);
    }

    public ControllerResult<Boolean> markDestroyObjects(List<Long> objects) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Long objectId : objects) {
            S3Object object = this.objectsMetadata.get(objectId);
            if (object == null || object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                log.error("object {} not exist when mark destroy object", objectId);
                return ControllerResult.of(Collections.emptyList(), false);
            }
            S3ObjectRecord record = new S3ObjectRecord()
                    .setObjectId(objectId)
                    .setObjectState(S3ObjectState.MARK_DESTROYED.toByte())
                    .setPreparedTimeInMs(object.getPreparedTimeInMs())
                    .setExpiredTimeInMs(object.getExpiredTimeInMs())
                    .setCommittedTimeInMs(object.getCommittedTimeInMs())
                    .setMarkDestroyedTimeInMs(System.currentTimeMillis());
            records.add(new ApiMessageAndVersion(record, (short) 0));
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
                S3ObjectState.fromByte(record.objectState()));
        objectsMetadata.put(record.objectId(), object);
        // TODO: recover the prepared objects and mark destroyed objects when restart the controller
        if (object.getS3ObjectState() == S3ObjectState.PREPARED) {
            preparedObjects.add(object.getObjectId());
        } else {
            preparedObjects.remove(object.getObjectId());
            if (object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
                markDestroyedObjects.add(object.getObjectId());
            }
        }
    }

    public void replay(RemoveS3ObjectRecord record) {
        objectsMetadata.remove(record.objectId());
        markDestroyedObjects.remove(record.objectId());
        preparedObjects.remove(record.objectId());
    }

    /**
     * Check the S3Object's lifecycle, mark destroy the expired objects and trigger truly deletion for the mark destroyed objects.
     *
     * @return the result of the check, contains the records which should be applied to the raft.
     */
    public ControllerResult<Void> checkS3ObjectsLifecycle() {
        List<ApiMessageAndVersion> records = new ArrayList<>();
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
                // generate the records which mark the expired objects as destroyed
                records.add(new ApiMessageAndVersion(record, (short) 0));
                // generate the records which listener reply for the object-destroy events
                lifecycleListeners.forEach(listener -> {
                    ControllerResult<Void> result = listener.onDestroy(obj.getObjectId());
                    records.addAll(result.records());
                });
            });
        // check the mark destroyed objects
        List<String> destroyedObjectKeys = this.markDestroyedObjects.stream()
            .map(id -> this.objectsMetadata.get(id))
            .filter(obj -> obj.getMarkDestroyedTimeInMs() + (this.config.objectRetentionTimeInSecond() * 1000L) < System.currentTimeMillis())
            .map(S3Object::getObjectKey)
            .collect(Collectors.toList());
        if (destroyedObjectKeys == null || destroyedObjectKeys.size() == 0) {
            return ControllerResult.of(records, null);
        }
        this.operator.delete(destroyedObjectKeys).whenCompleteAsync((resp, e) -> {
            if (e != null) {
                log.error("Failed to delete the S3Object from S3, objectKeys: {}",
                        String.join(",", destroyedObjectKeys), e);
                return;
            }
            if (resp != null && !resp.isEmpty()) {
                List<Long> deletedObjectIds = resp.stream().map(key -> ObjectUtils.parseObjectId(0, key)).collect(Collectors.toList());
                // notify the controller an objects deletion event to drive the removal of the objects
                ControllerRequestContext ctx = new ControllerRequestContext(
                    null, null, OptionalLong.empty());
                this.quorumController.notifyS3ObjectDeleted(ctx, deletedObjectIds).whenComplete((ignore, exp) -> {
                    if (exp != null) {
                        log.error("Failed to notify the controller the S3Object deletion event, objectIds: {}",
                            Arrays.toString(deletedObjectIds.toArray()), exp);
                    }
                });
            }
        });
        return ControllerResult.of(records, null);
    }

    /**
     * Generate RemoveS3ObjectRecord for the deleted S3Objects.
     *
     * @param deletedObjectIds the deleted S3Objects' ids
     * @return the result of the generation, contains the records which should be applied to the raft.
     */
    public ControllerResult<Void> notifyS3ObjectDeleted(List<Long> deletedObjectIds) {
        List<ApiMessageAndVersion> records = deletedObjectIds.stream().filter(markDestroyedObjects::contains)
            .map(objectId -> new ApiMessageAndVersion(new RemoveS3ObjectRecord()
                .setObjectId(objectId), (short) 0)).collect(Collectors.toList());
        return ControllerResult.of(records, null);
    }

    public Map<Long, S3Object> objectsMetadata() {
        return objectsMetadata;
    }

    public S3Object getObject(Long objectId) {
        return this.objectsMetadata.get(objectId);
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

    static class ObjectPair {

        private final Long objectId;
        private final String objectKey;

        public ObjectPair(Long objectId, String objectKey) {
            this.objectId = objectId;
            this.objectKey = objectKey;
        }

        public Long objectId() {
            return objectId;
        }

        public String objectKey() {
            return objectKey;
        }

        @Override
        public String toString() {
            return "ObjectPair{" +
                    "objectId=" + objectId +
                    ", objectKey='" + objectKey + '\'' +
                    '}';
        }
    }

}
