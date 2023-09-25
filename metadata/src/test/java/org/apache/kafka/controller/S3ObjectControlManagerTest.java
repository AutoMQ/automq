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

package org.apache.kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.automq.stream.s3.operator.S3Operator;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.S3ObjectControlManager;
import org.apache.kafka.metadata.stream.S3Config;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(40)
@Tag("S3Unit")
public class S3ObjectControlManagerTest {

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final String CLUSTER = "kafka-on-S3_cluster";
    private static final String S3_ENDPOINT = "http://localhost:4066";
    private static final String S3_REGION = "us-east-1";
    private static final String S3_BUCKET = "kafka-on-S3-bucket";

    private static final S3Config S3_CONFIG = new S3Config(S3_ENDPOINT, S3_REGION, S3_BUCKET, 5);
    private S3ObjectControlManager manager;
    private QuorumController controller;
    private S3Operator operator;

    @BeforeEach
    public void setUp() {
        controller = Mockito.mock(QuorumController.class);
        operator = Mockito.mock(S3Operator.class);
        Mockito.when(operator.delete(anyList())).then(inv -> {
            List<String> objectKeys = inv.getArgument(0);
            return CompletableFuture.completedFuture(objectKeys);
        });
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        manager = new S3ObjectControlManager(controller, registry, logContext, CLUSTER, S3_CONFIG, operator);
    }

    @Test
    public void testPrepareObject() {
        // 1. prepare 3 objects
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setBrokerId(BROKER0)
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
            verifyPrepareObjectRecord(result0.records().get(i + 1), i, 60 * 1000);
        }
        replay(manager, result0.records());

        // verify the 3 objects are prepared
        assertEquals(3, manager.objectsMetadata().size());
        manager.objectsMetadata().forEach((id, s3Object) -> {
            assertEquals(S3ObjectState.PREPARED, s3Object.getS3ObjectState());
            assertEquals(id, s3Object.getObjectId());
            assertEquals(60 * 1000, s3Object.getExpiredTimeInMs() - s3Object.getPreparedTimeInMs());
        });
        assertEquals(3, manager.nextAssignedObjectId());

        // 2. prepare 5 objects
        ControllerResult<PrepareS3ObjectResponseData> result1 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setBrokerId(BROKER1)
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
            assertEquals(60 * 1000, s3Object.getExpiredTimeInMs() - s3Object.getPreparedTimeInMs());
        });
        assertEquals(8, manager.nextAssignedObjectId());
    }

    private void replay(S3ObjectControlManager manager, List<ApiMessageAndVersion> records) {
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

    private void verifyPrepareObjectRecord(ApiMessageAndVersion result, long expectedObjectId, long expectedTimeToLiveInMs) {
        ApiMessage message = result.message();
        assertInstanceOf(S3ObjectRecord.class, message);
        S3ObjectRecord record = (S3ObjectRecord) message;
        assertEquals(expectedObjectId, record.objectId());
        assertEquals(expectedTimeToLiveInMs, record.expiredTimeInMs() - record.preparedTimeInMs());
        assertEquals((byte) S3ObjectState.PREPARED.ordinal(), record.objectState());
    }

    @Test
    public void testCommitObject() {
        // 1. prepare 1 object
        prepareOneObject(60 * 1000);

        // 2. commit an object which not exist in controller
        long expectedCommittedTs = 1313L;
        ControllerResult<Errors> result1 = manager.commitObject(1, 1024, expectedCommittedTs);
        assertEquals(Errors.OBJECT_NOT_EXIST, result1.response());
        assertEquals(0, result1.records().size());

        // 3. commit an valid object
        ControllerResult<Errors> result2 = manager.commitObject(0, 1024, expectedCommittedTs);
        assertEquals(Errors.NONE, result2.response());
        assertEquals(1, result2.records().size());
        S3ObjectRecord record = (S3ObjectRecord) result2.records().get(0).message();
        manager.replay(record);

        // 4. commit again
        ControllerResult<Errors> result3 = manager.commitObject(0, 1024, expectedCommittedTs);
        assertEquals(Errors.REDUNDANT_OPERATION, result3.response());
        assertEquals(0, result3.records().size());

        // 5. verify the object is committed
        assertEquals(1, manager.objectsMetadata().size());
        S3Object object = manager.objectsMetadata().get(0L);
        assertEquals(S3ObjectState.COMMITTED, object.getS3ObjectState());
        assertEquals(0L, object.getObjectId());
        assertEquals(1024, object.getObjectSize());
        assertEquals(expectedCommittedTs, object.getCommittedTimeInMs());
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
        // 2. 5s later, it should be marked as destroyed
        Thread.sleep(5 * 1000);
        assertEquals(1, manager.objectsMetadata().size());
        S3Object object = manager.objectsMetadata().get(0L);
        assertEquals(S3ObjectState.MARK_DESTROYED, object.getS3ObjectState());
        // 3. 5s later, it should be removed
        Thread.sleep(5 * 1000);
        assertEquals(0, manager.objectsMetadata().size());
        Mockito.verify(operator, Mockito.times(1)).delete(anyList());
    }

    private void prepareOneObject(long ttl) {
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setBrokerId(BROKER0)
            .setPreparedCount(1)
            .setTimeToLiveInMs(ttl));
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        assertEquals(2, result0.records().size());
        ApiMessage message = result0.records().get(0).message();
        assertInstanceOf(AssignedS3ObjectIdRecord.class, message);
        AssignedS3ObjectIdRecord assignedRecord = (AssignedS3ObjectIdRecord) message;
        assertEquals(0, assignedRecord.assignedS3ObjectId());
        verifyPrepareObjectRecord(result0.records().get(1), 0, ttl);
        manager.replay(assignedRecord);
        manager.replay((S3ObjectRecord) result0.records().get(1).message());
    }

}
