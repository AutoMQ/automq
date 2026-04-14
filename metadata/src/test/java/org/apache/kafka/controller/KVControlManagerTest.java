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
import org.apache.kafka.controller.stream.KVKey;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
@Tag("S3Unit")
public class KVControlManagerTest {

    private KVControlManager manager;

    @BeforeEach
    public void setUp() {
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        this.manager = new KVControlManager(registry, logContext, () -> AutoMQVersion.LATEST);
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
    public void testNamespaceIsolation() {
        // Put same key in two different namespaces
        ControllerResult<PutKVResponse> r1 = manager.putKV(new PutKVRequest()
            .setKey("key1").setValue("ns1-value".getBytes()).setNamespace("ns1"));
        assertEquals(Errors.NONE.code(), r1.response().errorCode());
        replay(manager, r1.records());

        ControllerResult<PutKVResponse> r2 = manager.putKV(new PutKVRequest()
            .setKey("key1").setValue("ns2-value".getBytes()).setNamespace("ns2"));
        assertEquals(Errors.NONE.code(), r2.response().errorCode());
        replay(manager, r2.records());

        // Each namespace sees its own value
        assertEquals("ns1-value", new String(manager.getKV(new GetKVRequest().setKey("key1").setNamespace("ns1")).value()));
        assertEquals("ns2-value", new String(manager.getKV(new GetKVRequest().setKey("key1").setNamespace("ns2")).value()));

        // Null namespace does not see namespaced entries
        assertNull(manager.getKV(new GetKVRequest().setKey("key1")).value());
    }

    @Test
    public void testNamespacedDelete() {
        ControllerResult<PutKVResponse> put = manager.putKV(new PutKVRequest()
            .setKey("key1").setValue("value".getBytes()).setNamespace("ns1"));
        replay(manager, put.records());

        // Delete from wrong namespace → not found
        ControllerResult<DeleteKVResponse> miss = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1").setNamespace("ns2"));
        assertEquals(Errors.KEY_NOT_EXIST.code(), miss.response().errorCode());
        assertEquals(0, miss.records().size());

        // Delete from correct namespace → success
        ControllerResult<DeleteKVResponse> del = manager.deleteKV(new DeleteKVRequest()
            .setKey("key1").setNamespace("ns1"));
        assertEquals(Errors.NONE.code(), del.response().errorCode());
        assertEquals("value", new String(del.response().value()));
        replay(manager, del.records());

        assertNull(manager.getKV(new GetKVRequest().setKey("key1").setNamespace("ns1")).value());
    }

    @Test
    public void testNamespacedOverwrite() {
        ControllerResult<PutKVResponse> put1 = manager.putKV(new PutKVRequest()
            .setKey("k").setValue("v1".getBytes()).setNamespace("ns"));
        replay(manager, put1.records());

        // Overwrite=false → KEY_EXIST
        ControllerResult<PutKVResponse> put2 = manager.putKV(new PutKVRequest()
            .setKey("k").setValue("v2".getBytes()).setNamespace("ns"));
        assertEquals(Errors.KEY_EXIST.code(), put2.response().errorCode());

        // Overwrite=true → success
        ControllerResult<PutKVResponse> put3 = manager.putKV(new PutKVRequest()
            .setKey("k").setValue("v2".getBytes()).setNamespace("ns").setOverwrite(true));
        assertEquals(Errors.NONE.code(), put3.response().errorCode());
        replay(manager, put3.records());

        assertEquals("v2", new String(manager.getKV(new GetKVRequest().setKey("k").setNamespace("ns")).value()));
    }

    @Test
    public void testNullAndNamespacedCoexist() {
        ControllerResult<PutKVResponse> p1 = manager.putKV(new PutKVRequest()
            .setKey("k").setValue("no-ns".getBytes()));
        replay(manager, p1.records());

        ControllerResult<PutKVResponse> p2 = manager.putKV(new PutKVRequest()
            .setKey("k").setValue("with-ns".getBytes()).setNamespace("ns"));
        replay(manager, p2.records());

        assertEquals("no-ns", new String(manager.getKV(new GetKVRequest().setKey("k")).value()));
        assertEquals("with-ns", new String(manager.getKV(new GetKVRequest().setKey("k").setNamespace("ns")).value()));
    }

    @Test
    public void testReplayRemoveKVRecord() {
        // Setup: put a null-namespace kv and a namespaced kv
        KVRecord kvRecord = new KVRecord().setKeyValues(List.of(
            new KVRecord.KeyValue().setKey("k1").setValue("v1".getBytes())));
        manager.replay(kvRecord);
        KVRecord nsKvRecord = new KVRecord().setKeyValues(List.of(
            new KVRecord.KeyValue().setKey("k1").setValue("v2".getBytes()).setNamespace("ns")));
        manager.replay(nsKvRecord);

        // 1) Null namespaces (v0 record) removes old-style null-namespace kv
        RemoveKVRecord v0Remove = new RemoveKVRecord()
            .setKeys(Collections.singletonList("k1"));
        // namespaces is null by default
        assertNull(v0Remove.namespaces());
        manager.replay(v0Remove);
        // null-namespace k1 removed, namespaced k1 still exists
        assertTrue(manager.kv().containsKey(KVKey.of("ns", "k1")));
        assertTrue(!manager.kv().containsKey(KVKey.of(null, "k1")));

        // 2) Non-null namespaces (v1 record) removes namespaced kv
        RemoveKVRecord v1Remove = new RemoveKVRecord()
            .setKeys(Collections.singletonList("k1"))
            .setNamespaces(Collections.singletonList("ns"));
        manager.replay(v1Remove);
        assertTrue(!manager.kv().containsKey(KVKey.of("ns", "k1")));

        // 3) Mismatched namespaces/keys sizes throws IllegalArgumentException
        RemoveKVRecord mismatch = new RemoveKVRecord()
            .setKeys(List.of("k1", "k2"))
            .setNamespaces(Collections.singletonList("ns"));
        assertThrows(IllegalArgumentException.class, () -> manager.replay(mismatch));
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
