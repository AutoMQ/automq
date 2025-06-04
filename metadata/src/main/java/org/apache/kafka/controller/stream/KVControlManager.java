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
import org.apache.kafka.controller.FeatureControlManager;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.automq.stream.utils.KVRecordUtils.buildCompositeKey;
import static org.apache.kafka.common.protocol.Errors.INVALID_KV_RECORD_EPOCH;
import static org.apache.kafka.common.protocol.Errors.KEY_EXIST;
import static org.apache.kafka.common.protocol.Errors.KEY_NOT_EXIST;

public class KVControlManager {

    private final SnapshotRegistry registry;
    private final Logger log;
    private final TimelineHashMap<String, ByteBuffer> kv;
    private final TimelineHashMap<String, KeyMetadata> keyMetadataMap;
    private final FeatureControlManager featureControl;

    public KVControlManager(SnapshotRegistry registry, LogContext logContext, FeatureControlManager featureControl) {
        this.registry = registry;
        this.log = logContext.logger(KVControlManager.class);
        this.kv = new TimelineHashMap<>(registry, 0);
        this.keyMetadataMap = new TimelineHashMap<>(registry, 0);
        this.featureControl = featureControl;
    }

    public GetKVResponse getKV(GetKVRequest request) {
        String key = buildCompositeKey(request.namespace(), request.key());
        byte[] value = kv.containsKey(key) ? kv.get(key).array() : null;
        KeyMetadata keyMetadata = null;
        if (request.namespace() != null && !request.namespace().isEmpty()) {
            keyMetadata = keyMetadataMap.get(key);
        }

        return new GetKVResponse()
            .setValue(value)
            .setNamespace(request.namespace())
            .setEpoch(keyMetadata != null ? keyMetadata.getEpoch() : 0L);
    }

    public ControllerResult<PutKVResponse> putKV(PutKVRequest request) {
        String key = buildCompositeKey(request.namespace(), request.key());
        KeyMetadata keyMetadata = keyMetadataMap.get(key);
        long currentEpoch = keyMetadata != null ? keyMetadata.getEpoch() : 0;
        if (request.epoch() > 0 && request.epoch() != currentEpoch) {
            return ControllerResult.of(Collections.emptyList(),
                new PutKVResponse()
                    .setErrorCode(INVALID_KV_RECORD_EPOCH.code())
                    .setEpoch(currentEpoch));
        }

        long newEpoch = System.currentTimeMillis();
        ByteBuffer value = kv.get(key);
        if (value == null || request.overwrite()) {
            // generate kv record
            ApiMessageAndVersion record = new ApiMessageAndVersion(new KVRecord()
                .setKeyValues(Collections.singletonList(new KeyValue()
                    .setKey(key)
                    .setValue(request.value())
                    .setNamespace(request.namespace())
                    .setEpoch(newEpoch))),
                featureControl.autoMQVersion().namespacedKVRecordVersion());
            return ControllerResult.of(Collections.singletonList(record), new PutKVResponse().setValue(request.value()).setEpoch(newEpoch));
        }
        // exist and not allow overwriting
        return ControllerResult.of(Collections.emptyList(), new PutKVResponse()
            .setErrorCode(KEY_EXIST.code())
            .setValue(value.array()));
    }

    public ControllerResult<DeleteKVResponse> deleteKV(DeleteKVRequest request) {
        String key = buildCompositeKey(request.namespace(), request.key());
        KeyMetadata keyMetadata = keyMetadataMap.get(key);
        long currentEpoch = keyMetadata != null ? keyMetadata.getEpoch() : 0;
        if (request.epoch() > 0 && request.epoch() != currentEpoch) {
            return ControllerResult.of(Collections.emptyList(),
                new DeleteKVResponse()
                    .setErrorCode(INVALID_KV_RECORD_EPOCH.code())
                    .setEpoch(currentEpoch));
        }
        log.trace("DeleteKVRequestData: {}", request);
        DeleteKVResponse resp = new DeleteKVResponse();
        ByteBuffer value = kv.get(request.key());
        if (value != null) {
            // generate remove-kv record
            ApiMessageAndVersion record = new ApiMessageAndVersion(new RemoveKVRecord()
                .setKeys(Collections.singletonList(request.key()))
                .setNamepsace(request.namespace()),
                featureControl.autoMQVersion().namespacedKVRecordVersion());
            return ControllerResult.of(Collections.singletonList(record), resp.setValue(value.array()).setEpoch(currentEpoch));
        }
        return ControllerResult.of(Collections.emptyList(), resp.setErrorCode(KEY_NOT_EXIST.code()));
    }

    public void replay(KVRecord record) {
        List<KeyValue> keyValues = record.keyValues();
        for (KeyValue keyValue : keyValues) {
            String key = buildCompositeKey(keyValue.namespace(), keyValue.key());
            kv.put(key, ByteBuffer.wrap(keyValue.value()));
            if (keyValue.namespace() != null && !keyValue.namespace().isEmpty()) {
                keyMetadataMap.put(key, new KeyMetadata(keyValue.namespace(), keyValue.epoch()));
            }
        }
    }

    public void replay(RemoveKVRecord record) {
        List<String> keys = record.keys();
        for (String key : keys) {
            String compositeKey = buildCompositeKey(record.namepsace(), key);
            kv.remove(compositeKey);
            if (record.namepsace() != null && !record.namepsace().isEmpty()) {
                keyMetadataMap.remove(compositeKey);
            }
        }
    }

    public Map<String, ByteBuffer> kv() {
        return kv;
    }

    private static class KeyMetadata {
        private final long epoch;
        private final String namespace;
        public KeyMetadata(String namespace, long epoch) {
            this.namespace = namespace;
            this.epoch = epoch;
        }

        public long getEpoch() {
            return epoch;
        }

        public String getNamespace() {
            return namespace;
        }
    }
}
