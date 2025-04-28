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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;

@Timeout(40)
@Tag("S3Unit")
public class S3ObjectControlManagerTest {

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final String CLUSTER = "kafka-on-S3_cluster";
    private static final Config S3_CONFIG = new Config().dataBuckets(BucketURI.parseBuckets("0@s3://bucket?region=us-east-1")).objectRetentionTimeInSecond(1);
    private S3ObjectControlManager manager;
    private QuorumController controller;
    private ObjectStorage objectStorage;
    private long nextObjectId;
    private Time time;

    @BeforeEach
    public void setUp() {
        controller = Mockito.mock(QuorumController.class);
        objectStorage = Mockito.mock(ObjectStorage.class);
        Mockito.when(objectStorage.delete(anyList())).then(inv -> {
            return CompletableFuture.completedFuture(null);
        });
        Mockito.when(controller.isActive()).thenReturn(true);
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        time = new MockTime();
        manager = new S3ObjectControlManager(controller, registry, logContext, CLUSTER, S3_CONFIG, objectStorage, () -> AutoMQVersion.LATEST, time);
    }

    @Test
    public void testPrepareObject() {
        // 1. prepare 3 objects
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setNodeId(BROKER0)
            .setPreparedCount(3)
            .setTimeToLiveInMs(60 * 1000));
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        assertEquals(0L, result0.response().firstS3ObjectId());
        assertEquals(4, result0.records().size());
        ApiMessage message = result0.records().get(0).message();
        assertInstanceOf(AssignedS3ObjectIdRecord.class, message);
        AssignedS3ObjectIdRecord assignedRecord = (AssignedS3ObjectIdRecord) message;
        assertEquals(2, assignedRecord.assignedS3ObjectId());
        for (int i = 0; i < 3; i++) {
            verifyPrepareObjectRecord(result0.records().get(i + 1), i, time.milliseconds() + 60 * 1000);
        }
        replay(manager, result0.records());

        // verify the 3 objects are prepared
        assertEquals(3, manager.objectsMetadata().size());
        manager.objectsMetadata().forEach((id, s3Object) -> {
            assertEquals(S3ObjectState.PREPARED, s3Object.getS3ObjectState());
            assertEquals(id, s3Object.getObjectId());
            assertEquals(time.milliseconds() + 60 * 1000, s3Object.getTimestamp());
        });
        assertEquals(3, manager.nextAssignedObjectId());

        // 2. prepare 5 objects
        ControllerResult<PrepareS3ObjectResponseData> result1 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setNodeId(BROKER1)
            .setPreparedCount(5)
            .setTimeToLiveInMs(60 * 1000));
        assertEquals(Errors.NONE.code(), result1.response().errorCode());
        assertEquals(3L, result1.response().firstS3ObjectId());
        assertEquals(6, result1.records().size());
        replay(manager, result1.records());

        // verify the 5 objects are prepared
        assertEquals(8, manager.objectsMetadata().size());
        manager.objectsMetadata().forEach((id, s3Object) -> {
            assertEquals(S3ObjectState.PREPARED, s3Object.getS3ObjectState());
            assertEquals(id, s3Object.getObjectId());
            assertEquals(time.milliseconds() + 60 * 1000, s3Object.getTimestamp());
        });
        assertEquals(8, manager.nextAssignedObjectId());
    }

    private synchronized void replay(S3ObjectControlManager manager, List<ApiMessageAndVersion> records) {
        List<ApiMessage> messages = records.stream().map(x -> x.message())
            .collect(Collectors.toList());
        for (ApiMessage message : messages) {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            switch (type) {
                case ASSIGNED_S3_OBJECT_ID_RECORD:
                    manager.replay((AssignedS3ObjectIdRecord) message);
                    break;
                case S3_OBJECT_RECORD:
                    manager.replay((S3ObjectRecord) message);
                    break;
                case REMOVE_S3_OBJECT_RECORD:
                    manager.replay((RemoveS3ObjectRecord) message);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata record type " + type);
            }
        }
    }

    private void verifyPrepareObjectRecord(ApiMessageAndVersion result, long expectedObjectId,
        long expectTimestamp) {
        ApiMessage message = result.message();
        assertInstanceOf(S3ObjectRecord.class, message);
        S3ObjectRecord record = (S3ObjectRecord) message;
        assertEquals(expectedObjectId, record.objectId());
        assertEquals(expectTimestamp, record.timestamp());
        assertEquals((byte) S3ObjectState.PREPARED.ordinal(), record.objectState());
    }

    @Test
    public void testCommitObject() {
        // 1. prepare 1 object
        prepareOneObject(60 * 1000);
        io.netty.util.Timeout timeout = manager.preparedObjectsTimeouts.get(0L);
        assertFalse(timeout.isCancelled());

        // 2. commit an object which not exist in controller
        long expectedCommittedTs = 1313L;
        ControllerResult<Errors> result1 = manager.commitObject(1, 1024, expectedCommittedTs, ObjectAttributes.DEFAULT.attributes());
        assertEquals(Errors.OBJECT_NOT_EXIST, result1.response());
        assertEquals(0, result1.records().size());

        // 3. commit an valid object
        ControllerResult<Errors> result2 = manager.commitObject(0, 1024, expectedCommittedTs, ObjectAttributes.DEFAULT.attributes());
        assertEquals(Errors.NONE, result2.response());
        assertEquals(1, result2.records().size());
        S3ObjectRecord record = (S3ObjectRecord) result2.records().get(0).message();
        manager.replay(record);
        assertFalse(manager.preparedObjectsTimeouts.containsKey(0L));
        assertTrue(timeout.isCancelled());

        // 4. commit again
        ControllerResult<Errors> result3 = manager.commitObject(0, 1024, expectedCommittedTs, ObjectAttributes.DEFAULT.attributes());
        assertEquals(Errors.REDUNDANT_OPERATION, result3.response());
        assertEquals(0, result3.records().size());

        // 5. verify the object is committed
        assertEquals(1, manager.objectsMetadata().size());
        S3Object object = manager.objectsMetadata().get(0L);
        assertEquals(S3ObjectState.COMMITTED, object.getS3ObjectState());
        assertEquals(0L, object.getObjectId());
        assertEquals(1024, object.getObjectSize());
        assertEquals(expectedCommittedTs, object.getTimestamp());
    }

    @Test
    public void testExpiredCheck() throws InterruptedException {
        Mockito.when(controller.checkS3ObjectsLifecycle(any(ControllerRequestContext.class)))
            .then(inv -> {
                ControllerResult<Void> result = manager.checkS3ObjectsLifecycle();
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });
        Mockito.when(controller.notifyS3ObjectDeleted(any(ControllerRequestContext.class), anyList()))
            .then(inv -> {
                ControllerResult<Void> result = manager.notifyS3ObjectDeleted(inv.getArgument(1));
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });
        // 1. prepare 1 object
        prepareOneObject(3 * 1000);
        assertEquals(1, manager.objectsMetadata().size());
        // 2. wait the prepared object expired, it should be marked as destroyed
        time.sleep(4000L);
        manager.handlePreparedObjectTimeout(0L);
        assertTrue(manager.preparedObjectsTimeouts.containsKey(0L));
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        assertEquals(1, manager.objectsMetadata().size());
        S3Object object = manager.objectsMetadata().get(0L);
        assertEquals(S3ObjectState.MARK_DESTROYED, object.getS3ObjectState());
        assertFalse(manager.preparedObjectsTimeouts.containsKey(0L));
        // 3. 2s later, it should be removed
        time.sleep(1100L);
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        time.sleep(1100L);
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        assertEquals(0, manager.objectsMetadata().size());
        Mockito.verify(objectStorage, times(1)).delete(anyList());
    }

    @Test
    public void testCheckS3ObjectsLifecycle_inflightLimit() throws InterruptedException {
        Mockito.when(controller.checkS3ObjectsLifecycle(any(ControllerRequestContext.class)))
            .then(inv -> {
                ControllerResult<Void> result = manager.checkS3ObjectsLifecycle();
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });
        Mockito.when(controller.notifyS3ObjectDeleted(any(ControllerRequestContext.class), anyList()))
            .then(inv -> {
                ControllerResult<Void> result = manager.notifyS3ObjectDeleted(inv.getArgument(1));
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });

        CompletableFuture<Void> delayTrigger = new CompletableFuture<>();
        Mockito.when(objectStorage.delete(anyList())).then(inv -> {
            List<String> objectKeys = inv.getArgument(0);
            return delayTrigger.thenApply(ignore -> objectKeys);
        });

        genMarkDestroyObject();
        time.sleep(1001L);
        manager.checkS3ObjectsLifecycle();
        time.sleep(1001L);
        manager.checkS3ObjectsLifecycle();
        @SuppressWarnings("unchecked") ArgumentCaptor<List<ObjectPath>> ac = ArgumentCaptor.forClass(List.class);
        Mockito.verify(objectStorage, times(1)).delete(ac.capture());
        assertEquals(List.of(ObjectUtils.genKey(0, 0L)), ac.getValue().stream().map(ObjectPath::key).collect(Collectors.toList()));

        genMarkDestroyObject();
        time.sleep(1001L);
        // the last delete is inflight, so the next check should not trigger another delete
        manager.checkS3ObjectsLifecycle();
        Mockito.verify(objectStorage, times(1)).delete(ac.capture());
        replay(manager, manager.notifyS3ObjectDeleted(List.of(0L)).records());

        // complete the last delete
        delayTrigger.complete(null);

        manager.checkS3ObjectsLifecycle();
        time.sleep(1001L);
        manager.checkS3ObjectsLifecycle();
        Mockito.verify(objectStorage, times(2)).delete(ac.capture());
        assertEquals(List.of(ObjectUtils.genKey(0, 1L)), ac.getValue().stream().map(ObjectPath::key).collect(Collectors.toList()));
    }

    private void genMarkDestroyObject() {
        // commit and destroy the object
        long objectId = prepareOneObject(60 * 1000);
        {
            long expectedCommittedTs = 1313L;
            ControllerResult<Errors> result = manager.commitObject(objectId, 1024, expectedCommittedTs, ObjectAttributes.DEFAULT.attributes());
            assertEquals(Errors.NONE, result.response());
            assertEquals(1, result.records().size());
            S3ObjectRecord record = (S3ObjectRecord) result.records().get(0).message();
            manager.replay(record);
        }

        {
            ControllerResult<Boolean> result = manager.markDestroyObjects(List.of(objectId));
            assertTrue(result.response());
            assertEquals(1, result.records().size());
            S3ObjectRecord record = (S3ObjectRecord) result.records().get(0).message();
            assertEquals(S3ObjectState.MARK_DESTROYED.toByte(), record.objectState());
            assertEquals(objectId, record.objectId());
            manager.replay(record);
        }
    }

    @Test
    public void testDeleteTooManyOneRequest() throws Exception {
        Mockito.when(controller.checkS3ObjectsLifecycle(any(ControllerRequestContext.class)))
            .then(inv -> {
                ControllerResult<Void> result = manager.checkS3ObjectsLifecycle();
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });
        Mockito.when(controller.notifyS3ObjectDeleted(any(ControllerRequestContext.class), anyList()))
            .then(inv -> {
                ControllerResult<Void> result = manager.notifyS3ObjectDeleted(inv.getArgument(1));
                replay(manager, result.records());
                return CompletableFuture.completedFuture(null);
            });
        Mockito.doAnswer(ink -> CompletableFuture.completedFuture(null)).when(objectStorage).delete(anyList());
        // 1. prepare 1700 object
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setNodeId(BROKER0)
            .setPreparedCount(1700)
            .setTimeToLiveInMs(3 * 1000));
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        replay(manager, result0.records());

        assertEquals(1700, manager.objectsMetadata().size());
        // 2. 3s later, they should be marked as destroyed
        time.sleep(4000L);
        for (int i = 0; i < 1700; i++) {
            manager.handlePreparedObjectTimeout(i);
        }
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        assertEquals(1700, manager.objectsMetadata().size());
        for (int i = 0; i < 1700; i++) {
            S3Object object = manager.objectsMetadata().get((long) i);
            assertEquals(S3ObjectState.MARK_DESTROYED, object.getS3ObjectState());
        }
        // 3. 1s later, they should be removed
        time.sleep(1100L);
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        time.sleep(1100L);
        replay(manager, manager.checkS3ObjectsLifecycle().records());
        assertEquals(0, manager.objectsMetadata().size(), "objectsMetadata: " + manager.objectsMetadata().keySet());

        // 1700 / 1000 + 1
        Mockito.verify(objectStorage, times(2)).delete(anyList());
    }

    private long prepareOneObject(long ttl) {
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setNodeId(BROKER0)
            .setPreparedCount(1)
            .setTimeToLiveInMs(ttl));
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        assertEquals(2, result0.records().size());
        ApiMessage message = result0.records().get(0).message();
        assertInstanceOf(AssignedS3ObjectIdRecord.class, message);
        AssignedS3ObjectIdRecord assignedRecord = (AssignedS3ObjectIdRecord) message;
        long objectId = nextObjectId++;
        assertEquals(objectId, assignedRecord.assignedS3ObjectId());
        verifyPrepareObjectRecord(result0.records().get(1), objectId, time.milliseconds() + ttl);
        replay(manager, result0.records());
        return objectId;
    }

}
