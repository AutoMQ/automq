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

import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData.DeleteKVResponse;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.KVRecord.KeyValue;
import org.apache.kafka.common.metadata.RemoveKVRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.kafka.common.protocol.Errors.KEY_EXIST;
import static org.apache.kafka.common.protocol.Errors.KEY_NOT_EXIST;

public class KVControlManager {

    private final SnapshotRegistry registry;
    private final Logger log;
    private final TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> kvByNamespace;
    private final Supplier<AutoMQVersion> autoMQVersionSupplier;

    public KVControlManager(SnapshotRegistry registry, LogContext logContext,
        Supplier<AutoMQVersion> autoMQVersionSupplier) {
        this.registry = registry;
        this.log = logContext.logger(KVControlManager.class);
        this.kvByNamespace = new TimelineHashMap<>(registry, 0);
        this.autoMQVersionSupplier = autoMQVersionSupplier;
    }

    public GetKVResponse getKV(GetKVRequest request) {
        ByteBuffer valueBuffer = getValue(request.namespace(), request.key());
        byte[] value = valueBuffer == null ? null : valueBuffer.array();
        return new GetKVResponse()
            .setValue(value);
    }

    public ControllerResult<PutKVResponse> putKV(PutKVRequest request) {
        AutoMQVersion version = autoMQVersionSupplier.get();
        String namespace = version.isKVNamespaceSupported() ? request.namespace() : null;
        String key = request.key();
        ByteBuffer value = getValue(namespace, key);
        if (value == null || request.overwrite()) {
            // generate kv record
            ApiMessageAndVersion record = new ApiMessageAndVersion(new KVRecord()
                .setKeyValues(Collections.singletonList(new KeyValue()
                    .setKey(key)
                    .setValue(request.value())
                    .setNamespace(namespace))),
                version.kvRecordVersion());
            return ControllerResult.of(Collections.singletonList(record), new PutKVResponse().setValue(request.value()));
        }
        // exist and not allow overwriting
        return ControllerResult.of(Collections.emptyList(), new PutKVResponse()
            .setErrorCode(KEY_EXIST.code())
            .setValue(value.array()));
    }

    public ControllerResult<DeleteKVResponse> deleteKV(DeleteKVRequest request) {
        log.trace("DeleteKVRequestData: {}", request);
        AutoMQVersion version = autoMQVersionSupplier.get();
        String namespace = version.isKVNamespaceSupported() ? request.namespace() : null;
        DeleteKVResponse resp = new DeleteKVResponse();
        ByteBuffer value = getValue(namespace, request.key());
        if (value != null) {
            // generate remove-kv record
            RemoveKVRecord removeRecord = new RemoveKVRecord()
                .setKeys(Collections.singletonList(request.key()));
            if (namespace != null) {
                removeRecord.setNamespaces(Collections.singletonList(namespace));
            }
            ApiMessageAndVersion record = new ApiMessageAndVersion(removeRecord,
                version.isKVNamespaceSupported() ? (short) 1 : (short) 0);
            return ControllerResult.of(Collections.singletonList(record), resp.setValue(value.array()));
        }
        return ControllerResult.of(Collections.emptyList(), resp.setErrorCode(KEY_NOT_EXIST.code()));
    }

    public void replay(KVRecord record) {
        List<KeyValue> keyValues = record.keyValues();
        for (KeyValue keyValue : keyValues) {
            createNamespaceKVsIfAbsent(keyValue.namespace()).put(keyValue.key(), ByteBuffer.wrap(keyValue.value()));
        }
    }

    public void replay(RemoveKVRecord record) {
        List<String> keys = record.keys();
        List<String> namespaces = record.namespaces();
        if (namespaces != null && namespaces.size() != keys.size()) {
            throw new IllegalArgumentException("RemoveKVRecord: namespaces length " + namespaces.size()
                + " does not match keys length " + keys.size());
        }
        for (int i = 0; i < keys.size(); i++) {
            String ns = namespaces != null ? namespaces.get(i) : null;
            TimelineHashMap<String, ByteBuffer> namespaceKVs = kvByNamespace.get(KVNamespace.of(ns));
            if (namespaceKVs != null) {
                namespaceKVs.remove(keys.get(i));
                if (namespaceKVs.isEmpty()) {
                    kvByNamespace.remove(KVNamespace.of(ns));
                }
            }
        }
    }

    public boolean containsKey(KVKey key) {
        return getValue(key.namespace(), key.key()) != null;
    }

    public Map<String, ByteBuffer> namespaceKVs(String namespace) {
        TimelineHashMap<String, ByteBuffer> namespaceKVs = kvByNamespace.get(KVNamespace.of(namespace));
        return namespaceKVs == null ? Collections.emptyMap() : namespaceKVs;
    }

    private ByteBuffer getValue(String namespace, String key) {
        TimelineHashMap<String, ByteBuffer> namespaceKVs = kvByNamespace.get(KVNamespace.of(namespace));
        return namespaceKVs == null ? null : namespaceKVs.get(key);
    }

    private TimelineHashMap<String, ByteBuffer> createNamespaceKVsIfAbsent(String namespace) {
        KVNamespace kvNamespace = KVNamespace.of(namespace);
        TimelineHashMap<String, ByteBuffer> namespaceKVs = kvByNamespace.get(kvNamespace);
        if (namespaceKVs == null) {
            namespaceKVs = new TimelineHashMap<>(registry, namespace == null ? 100000 : 0);
            kvByNamespace.put(kvNamespace, namespaceKVs);
        }
        return namespaceKVs;
    }
}
