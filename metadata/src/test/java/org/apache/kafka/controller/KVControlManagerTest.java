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

import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData.DeleteKVResponse;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.KVControlManager;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.INVALID_KV_RECORD_EPOCH;
import static org.apache.kafka.controller.FeatureControlManagerTest.features;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
@Tag("S3Unit")
public class KVControlManagerTest {

    private KVControlManager manager;

    @BeforeEach
    public void setUp() {
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        FeatureControlManager featureControlManager = new FeatureControlManager.Builder().
            setQuorumFeatures(features("foo", 1, 2)).
            setSnapshotRegistry(registry).
            setMetadataVersion(MetadataVersion.IBP_3_3_IV0).
            build();
        this.manager = new KVControlManager(registry, logContext, featureControlManager);
    }

    @Test
    public void testBasicReadWrite() {
        ControllerResult<PutKVResponse> result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1".getBytes()));
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals("value1", new String(result.response().value()));
        replay(manager, result.records());

        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1-1".getBytes()));
        assertEquals(0, result.records().size());
        assertEquals(Errors.KEY_EXIST.code(), result.response().errorCode());
        assertEquals("value1", new String(result.response().value()));

        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1-2".getBytes())
            .setOverwrite(true));
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals("value1-2", new String(result.response().value()));
        replay(manager, result.records());

        GetKVResponse result2 = manager.getKV(new GetKVRequest()
            .setKey("key1"));
        assertEquals("value1-2", new String(result2.value()));

        result2 = manager.getKV(new GetKVRequest()
            .setKey("key2"));
        assertNull(result2.value());

        ControllerResult<DeleteKVResponse> result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key2"));
        assertEquals(0, result3.records().size());
        assertEquals(Errors.KEY_NOT_EXIST.code(), result3.response().errorCode());

        result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1"));
        assertEquals(1, result3.records().size());
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        assertEquals("value1-2", new String(result3.response().value()));
        replay(manager, result3.records());
        // key1 is deleted
        result2 = manager.getKV(new GetKVRequest()
            .setKey("key1"));
        assertNull(result2.value());

        result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1"));
        assertEquals(0, result3.records().size());
        assertEquals(Errors.KEY_NOT_EXIST.code(), result3.response().errorCode());
    }

    @Test
    public void testNamespacedReadWrite() {
        ControllerResult<PutKVResponse> result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1".getBytes())
            .setNamespace("__automq_test")
        );
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals("value1", new String(result.response().value()));
        replay(manager, result.records());

        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1-1".getBytes())
            .setNamespace("__automq_test")
        );
        assertEquals(0, result.records().size());
        assertEquals(Errors.KEY_EXIST.code(), result.response().errorCode());
        assertEquals("value1", new String(result.response().value()));

        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1-2".getBytes())
            .setNamespace("__automq_test")
            .setOverwrite(true));
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        assertEquals("value1-2", new String(result.response().value()));
        replay(manager, result.records());

        GetKVResponse result2 = manager.getKV(new GetKVRequest()
            .setKey("key1")
            .setNamespace("__automq_test"));
        assertEquals("value1-2", new String(result2.value()));

        result2 = manager.getKV(new GetKVRequest()
            .setKey("key2")
            .setNamespace("__automq_test"));
        assertNull(result2.value());

        ControllerResult<DeleteKVResponse> result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key2")
            .setNamespace("__automq_test"));
        assertEquals(0, result3.records().size());
        assertEquals(Errors.KEY_NOT_EXIST.code(), result3.response().errorCode());

        result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1")
            .setNamespace("__automq_test")
        );
        assertEquals(1, result3.records().size());
        assertEquals(Errors.NONE.code(), result3.response().errorCode());
        assertEquals("value1-2", new String(result3.response().value()));
        replay(manager, result3.records());
        // key1 is deleted
        result2 = manager.getKV(new GetKVRequest()
            .setKey("key1")
            .setNamespace("__automq_test"));
        assertNull(result2.value());

        result3 = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1")
            .setNamespace("__automq_test"));
        assertEquals(0, result3.records().size());
        assertEquals(Errors.KEY_NOT_EXIST.code(), result3.response().errorCode());
    }

    @Test
    public void testPutWithEpochValidation() {
        ControllerResult<PutKVResponse> result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1".getBytes())
            .setNamespace("__epoch_test")
            .setEpoch(0));
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        long initialEpoch = result.response().epoch();
        assertTrue(initialEpoch > 0);
        replay(manager, result.records());
        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value2".getBytes())
            .setNamespace("__epoch_test")
            .setEpoch(initialEpoch - 1)
            .setOverwrite(true));
        assertEquals(0, result.records().size());
        assertEquals(INVALID_KV_RECORD_EPOCH.code(), result.response().errorCode());
        assertEquals(initialEpoch, result.response().epoch());
        // without overwrite, should fail
        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value2".getBytes())
            .setNamespace("__epoch_test")
            .setEpoch(initialEpoch));
        assertEquals(0, result.records().size());
        assertEquals(Errors.KEY_EXIST.code(), result.response().errorCode());
        // with overwrite, should succeed
        result = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value2".getBytes())
            .setNamespace("__epoch_test")
            .setEpoch(initialEpoch)
            .setOverwrite(true));
        assertEquals(1, result.records().size());
        assertEquals(Errors.NONE.code(), result.response().errorCode());
        long newEpoch = result.response().epoch();
        assertTrue(newEpoch > initialEpoch);
        replay(manager, result.records());
        GetKVResponse readResp = manager.getKV(new GetKVRequest()
            .setKey("key1")
            .setNamespace("__epoch_test"));
        assertEquals(newEpoch, readResp.epoch());
    }

    @Test
    public void testDeleteWithEpochValidation() {
        ControllerResult<PutKVResponse> putResult = manager.putKV(new PutKVRequest()
            .setKey("key1")
            .setValue("value1".getBytes())
            .setNamespace("__epoch_test"));
        long initialEpoch = putResult.response().epoch();
        replay(manager, putResult.records());
        ControllerResult<DeleteKVResponse> delResult = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1")
            .setNamespace("__epoch_test")
            .setEpoch(initialEpoch - 1));
        assertEquals(0, delResult.records().size());
        assertEquals(INVALID_KV_RECORD_EPOCH.code(), delResult.response().errorCode());
        assertEquals(initialEpoch, delResult.response().epoch());
        delResult = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1")
            .setNamespace("__epoch_test")
            .setEpoch(initialEpoch));
        assertEquals(1, delResult.records().size());
        assertEquals(Errors.NONE.code(), delResult.response().errorCode());
        replay(manager, delResult.records());
        GetKVResponse readResp = manager.getKV(new GetKVRequest()
            .setKey("key1")
            .setNamespace("__epoch_test"));
        assertNull(readResp.value());
    }

    private void replay(KVControlManager manager, List<ApiMessageAndVersion> records) {
        List<ApiMessage> messages = records.stream().map(x -> x.message())
            .collect(Collectors.toList());
        for (ApiMessage message : messages) {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            switch (type) {
                case KVRECORD:
                    manager.replay((KVRecord) message);
                    break;
                case REMOVE_KVRECORD:
                    manager.replay((RemoveKVRecord) message);
                    break;
                default:
                    throw new IllegalStateException("Unknown metadata record type " + type);
            }
        }
    }
}
