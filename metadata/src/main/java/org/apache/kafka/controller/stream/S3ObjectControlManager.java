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

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.stream.S3ObjectKeyGeneratorManager.GenerateContextV0;
import org.apache.kafka.metadata.stream.S3Config;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

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
    private Long nextAssignedObjectId = 0L;

    private final Queue<Long/*objectId*/> preparedObjects;

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
        S3Config config) {
        this.quorumController = quorumController;
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(S3ObjectControlManager.class);
        this.clusterId = clusterId;
        this.config = config;
        this.objectsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.preparedObjects = new LinkedBlockingDeque<>();
        this.markDestroyedObjects = new LinkedBlockingDeque<>();
        this.operator = new DefaultS3Operator();
        this.lifecycleListeners = new ArrayList<>();
        this.lifecycleCheckTimer = Executors.newSingleThreadScheduledExecutor();
        this.lifecycleCheckTimer.scheduleWithFixedDelay(() -> {
            triggerCheckEvent();
        }, DEFAULT_INITIAL_DELAY_MS, DEFAULT_LIFECYCLE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
        return nextAssignedObjectId;
    }

    public void replay(S3ObjectRecord record) {
        GenerateContextV0 ctx = new GenerateContextV0(clusterId, record.objectId());
        String objectKey = S3ObjectKeyGeneratorManager.getByVersion(0).generate(ctx);
        S3Object object = new S3Object(record.objectId(), record.objectSize(), objectKey,
            record.appliedTimeInMs(), record.expiredTimeInMs(), record.committedTimeInMs(), record.destroyedTimeInMs(),
            S3ObjectState.fromByte(record.objectState()));
        objectsMetadata.put(record.objectId(), object);
        // TODO: recover the prepared objects and mark destroyed objects when restart the controller
        if (object.getS3ObjectState() == S3ObjectState.PREPARED) {
            preparedObjects.add(object.getObjectId());
        } else if (object.getS3ObjectState() == S3ObjectState.MARK_DESTROYED) {
            markDestroyedObjects.add(object.getObjectId());
        }
        nextAssignedObjectId = Math.max(nextAssignedObjectId, record.objectId() + 1);
    }

    public void replay(RemoveS3ObjectRecord record) {
        objectsMetadata.remove(record.objectId());
        markDestroyedObjects.remove(record.objectId());
    }

    /**
     * Check the S3Object's lifecycle, and do the corresponding actions.
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
                    .setObjectState((byte) S3ObjectState.DESTROYED.ordinal())
                    .setObjectSize(obj.getObjectSize().get())
                    .setAppliedTimeInMs(obj.getAppliedTimeInMs().get())
                    .setExpiredTimeInMs(obj.getExpiredTimeInMs().get())
                    .setCommittedTimeInMs(obj.getCommittedTimeInMs().get())
                    .setDestroyedTimeInMs(obj.getDestroyedTimeInMs().get());
                // generate the records which mark the expired objects as destroyed
                records.add(new ApiMessageAndVersion(record, (short) 0));
                // generate the records which listener reply for the object-destroy events
                lifecycleListeners.forEach(listener -> {
                    ControllerResult<Void> result = listener.onDestroy(obj.getObjectId());
                    records.addAll(result.records());
                });
            });
        // check the mark destroyed objects
        String[] destroyedObjectIds = this.markDestroyedObjects.stream()
            .map(objectsMetadata::get)
            .map(S3Object::getObjectKey)
            .toArray(String[]::new);
        // TODO: optimize this ugly implementation, now the thread will be blocked until the deletion request is finished
        CompletableFuture<Void> future = this.operator.delele(destroyedObjectIds).exceptionally(exp -> {
            log.error("Failed to delete the S3Object from S3, objectIds: {}",
                String.join(",", destroyedObjectIds), exp);
            return false;
        }).thenAccept(success -> {
            if (success) {
                // generate the records which remove the mark destroyed objects
                this.markDestroyedObjects.forEach(objectId -> {
                    records.add(new ApiMessageAndVersion(
                        new RemoveS3ObjectRecord().setObjectId(objectId), (short) 0));
                });
            }
        });
        try {
            future.get();
        } catch (Exception e) {
            log.error("Failed to get S3Object deletion operation result, objectIds: {}",
                String.join(",", destroyedObjectIds), e);
        }
        return ControllerResult.of(records, null);
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

}
