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
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
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
        ControllerResult<PutKVsResponseData> result0 = manager.putKVs(new PutKVsRequestData()
            .setPutKVRequests(List.of(
                new PutKVRequest()
                    .setKey("key1")
                    .setValue("value1".getBytes()),
                new PutKVRequest()
                    .setKey("key1")
                    .setValue("value1-1".getBytes()),
                new PutKVRequest()
                    .setKey("key1")
                    .setValue("value1-2".getBytes())
                    .setOverwrite(true))));
        assertEquals(2, result0.records().size());
        replay(manager, result0.records());
        assertEquals(2, manager.kv().size());
        assertEquals(3, result0.response().putKVResponses().size());
        assertEquals(Errors.NONE.code(), result0.response().putKVResponses().get(0).errorCode());
        assertEquals("value1", new String(result0.response().putKVResponses().get(0).value()));
        assertEquals(Errors.KEY_EXIST.code(), result0.response().putKVResponses().get(1).errorCode());
        assertEquals("value1", new String(result0.response().putKVResponses().get(1).value()));
        assertEquals(Errors.NONE.code(), result0.response().putKVResponses().get(2).errorCode());
        assertEquals("value1-2", new String(result0.response().putKVResponses().get(2).value()));

        GetKVsResponseData resp1 = manager.getKVs(new GetKVsRequestData()
            .setGetKeyRequests(List.of(
                new GetKVRequest()
                    .setKey("key1"),
                new GetKVRequest()
                    .setKey("key2"))));
        assertEquals(2, resp1.getKVResponses().size());
        assertEquals("value1-2", new String(resp1.getKVResponses().get(0).value()));
        assertNull(resp1.getKVResponses().get(1).value());

        ControllerResult<DeleteKVsResponseData> result2 = manager.deleteKVs(new DeleteKVsRequestData()
            .setDeleteKVRequests(List.of(
                new DeleteKVRequest()
                    .setKey("key2"),
                new DeleteKVRequest()
                    .setKey("key1"),
                new DeleteKVRequest()
                    .setKey("key1")
            )));
        assertEquals(1, result2.records().size());
        assertEquals(Errors.KEY_NOT_EXIST.code(), result2.response().deleteKVResponses().get(0).errorCode());
        assertEquals(Errors.NONE.code(), result2.response().deleteKVResponses().get(1).errorCode());
        assertEquals("value1-2", new String(result2.response().deleteKVResponses().get(1).value()));
        assertEquals(Errors.KEY_NOT_EXIST.code(), result2.response().deleteKVResponses().get(2).errorCode());
        replay(manager, result2.records());
        assertEquals(0, manager.kv().size());

        GetKVsResponseData resp3 = manager.getKVs(new GetKVsRequestData()
            .setGetKeyRequests(List.of(new GetKVRequest()
                .setKey("key1"))));
        assertNull(resp3.getKVResponses().get(0).value());
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
