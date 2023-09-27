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

import static org.apache.kafka.common.protocol.Errors.KEY_EXIST;
import static org.apache.kafka.common.protocol.Errors.KEY_NOT_EXIST;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

public class KVControlManager {

    private final SnapshotRegistry registry;
    private final Logger log;
    private final TimelineHashMap<String, ByteBuffer> kv;

    public KVControlManager(SnapshotRegistry registry, LogContext logContext) {
        this.registry = registry;
        this.log = logContext.logger(KVControlManager.class);
        this.kv = new TimelineHashMap<>(registry, 0);
    }

    public GetKVResponse getKV(GetKVRequest request) {
        String key = request.key();
        byte[] value = kv.containsKey(key) ? kv.get(key).array() : null;
        return new GetKVResponse()
            .setValue(value);
    }

    public ControllerResult<PutKVResponse> putKV(PutKVRequest request) {
        String key = request.key();
        ByteBuffer value = kv.get(key);
        if (value == null || request.overwrite()) {
            // generate kv record
            ApiMessageAndVersion record = new ApiMessageAndVersion(new KVRecord()
                .setKeyValues(Collections.singletonList(new KeyValue()
                    .setKey(key)
                    .setValue(request.value()))), (short) 0);
            return ControllerResult.of(Collections.singletonList(record), new PutKVResponse().setValue(request.value()));
        }
        // exist and not allow overwriting
        return ControllerResult.of(Collections.emptyList(), new PutKVResponse()
            .setErrorCode(KEY_EXIST.code())
            .setValue(value.array()));
    }

    public ControllerResult<DeleteKVResponse> deleteKV(DeleteKVRequest request) {
        log.trace("DeleteKVRequestData: {}", request);
        DeleteKVResponse resp = new DeleteKVResponse();
        ByteBuffer value = kv.get(request.key());
        if (value != null) {
            // generate remove-kv record
            ApiMessageAndVersion record = new ApiMessageAndVersion(new RemoveKVRecord()
                .setKeys(Collections.singletonList(request.key())), (short) 0);
            return ControllerResult.of(Collections.singletonList(record), resp.setValue(value.array()));
        }
        return ControllerResult.of(Collections.emptyList(), resp.setErrorCode(KEY_NOT_EXIST.code()));
    }

    public void replay(KVRecord record) {
        List<KeyValue> keyValues = record.keyValues();
        for (KeyValue keyValue : keyValues) {
            kv.put(keyValue.key(), ByteBuffer.wrap(keyValue.value()));
        }
    }

    public void replay(RemoveKVRecord record) {
        List<String> keys = record.keys();
        for (String key : keys) {
            kv.remove(key);
        }
    }

    public Map<String, ByteBuffer> kv() {
        return kv;
    }
}
