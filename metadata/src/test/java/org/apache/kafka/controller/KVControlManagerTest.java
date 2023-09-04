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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.DeleteKVRequestData;
import org.apache.kafka.common.message.DeleteKVResponseData;
import org.apache.kafka.common.message.GetKVRequestData;
import org.apache.kafka.common.message.GetKVResponseData;
import org.apache.kafka.common.message.PutKVRequestData;
import org.apache.kafka.common.message.PutKVRequestData.KeyValue;
import org.apache.kafka.common.message.PutKVResponseData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.KVControlManager;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(40)
@Tag("S3Unit")
public class KVControlManagerTest {

    private KVControlManager manager;

    @BeforeEach
    public void setUp() {
        LogContext logContext = new LogContext();
        SnapshotRegistry registry = new SnapshotRegistry(logContext);
        this.manager = new KVControlManager(registry, logContext);
    }

    @Test
    public void testBasicReadWrite() {
        ControllerResult<PutKVResponseData> result0 = manager.putKV(new PutKVRequestData()
            .setKeyValues(List.of(
                new KeyValue()
                    .setKey("key1")
                    .setValue("value1".getBytes()),
                new KeyValue()
                    .setKey("key2")
                    .setValue("value2".getBytes()))));
        assertEquals(1, result0.records().size());
        replay(manager, result0.records());
        assertEquals(2, manager.kv().size());

        GetKVResponseData resp1 = manager.getKV(new GetKVRequestData()
            .setKeys(List.of("key1", "key2", "key3")));
        assertEquals(3, resp1.keyValues().size());
        assertEquals("key1", resp1.keyValues().get(0).key());
        assertEquals("value1", new String(resp1.keyValues().get(0).value()));
        assertEquals("key2", resp1.keyValues().get(1).key());
        assertEquals("value2", new String(resp1.keyValues().get(1).value()));
        assertEquals("key3", resp1.keyValues().get(2).key());
        assertNull(resp1.keyValues().get(2).value());

        ControllerResult<DeleteKVResponseData> result2 = manager.deleteKV(new DeleteKVRequestData()
            .setKeys(List.of("key1", "key3")));
        assertEquals(1, result2.records().size());
        replay(manager, result2.records());
        assertEquals(1, manager.kv().size());

        GetKVResponseData resp3 = manager.getKV(new GetKVRequestData()
            .setKeys(List.of("key1", "key2", "key3")));
        assertEquals(3, resp3.keyValues().size());
        assertEquals("key1", resp3.keyValues().get(0).key());
        assertNull(resp3.keyValues().get(0).value());
        assertEquals("key2", resp3.keyValues().get(1).key());
        assertEquals("value2", new String(resp3.keyValues().get(1).value()));
        assertEquals("key3", resp3.keyValues().get(2).key());
        assertNull(resp3.keyValues().get(2).value());
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
