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

import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.S3ObjectControlManager;
import org.apache.kafka.metadata.stream.S3Config;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(40)
public class S3ObjectControlManagerTest {
    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final String CLUSTER = "kafka-on-S3_cluster";
    private static final String S3_REGION = "us-east-1";
    private static final String S3_BUCKET = "kafka-on-S3-bucket";

    private static final S3Config S3_CONFIG = new S3Config(S3_REGION, S3_BUCKET);
    private S3ObjectControlManager manager;

    private QuorumController controller;

    @BeforeEach
    public void setUp() {
        controller = Mockito.mock(QuorumController.class);
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        manager = new S3ObjectControlManager(controller, registry, logContext, CLUSTER, S3_CONFIG);
    }

    @Test
    public void testBasicPrepareObject() {
        // 1. prepare 3 objects
        ControllerResult<PrepareS3ObjectResponseData> result0 = manager.prepareObject(new PrepareS3ObjectRequestData()
            .setBrokerId(BROKER0)
            .setPreparedCount(3)
            .setTimeToLiveInMs(1000));
        assertEquals(Errors.NONE.code(), result0.response().errorCode());
        assertEquals(3, result0.records().size());
        for (int i = 0; i < 3; i++) {
            verifyPrepareObjectRecord(result0.records().get(i), i, 1000);
        }
        result0.records().stream().map(ApiMessageAndVersion::message).forEach(record -> manager.replay((S3ObjectRecord) record));

        // verify the 3 objects are prepared
        assertEquals(3, manager.objectsMetadata().size());
        manager.objectsMetadata().forEach((id, s3Object) -> {
            assertEquals(S3ObjectState.PREPARED, s3Object.getS3ObjectState());
            assertEquals(id, s3Object.getObjectId());
            assertEquals(1000, s3Object.getExpiredTimeInMs() - s3Object.getPreparedTimeInMs());
        });
        assertEquals(3, manager.nextAssignedObjectId());
    }

    private void verifyPrepareObjectRecord(ApiMessageAndVersion result, long expectedObjectId, long expectedTimeToLiveInMs) {
        ApiMessage message = result.message();
        assertInstanceOf(S3ObjectRecord.class, message);
        S3ObjectRecord record = (S3ObjectRecord) message;
        assertEquals(expectedObjectId, record.objectId());
        assertEquals(expectedTimeToLiveInMs, record.expiredTimeInMs() - record.preparedTimeInMs());
        assertEquals((byte) S3ObjectState.PREPARED.ordinal(), record.objectState());
    }

}
