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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.DeleteKVRequestData;
import org.apache.kafka.common.message.DeleteKVResponseData;
import org.apache.kafka.common.message.GetKVRequestData;
import org.apache.kafka.common.message.GetKVResponseData;
import org.apache.kafka.common.message.PutKVRequestData;
import org.apache.kafka.common.message.PutKVResponseData;
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

    public GetKVResponseData getKV(GetKVRequestData request) {
        GetKVResponseData resp = new GetKVResponseData();
        resp.setKeyValues(
            request.keys().stream().map(key -> {
                byte[] value = kv.containsKey(key) ? kv.get(key).array() : null;
                return new GetKVResponseData.KeyValue()
                    .setKey(key)
                    .setValue(value);
            }).collect(Collectors.toList()));
        log.trace("GetKVResponseData: req: {}, resp: {}", request, resp);
        // generate kv record
        return resp;
    }

    public ControllerResult<PutKVResponseData> putKV(PutKVRequestData request) {
        log.trace("PutKVRequestData: {}", request);
        PutKVResponseData resp = new PutKVResponseData();
        // generate kv record
        ApiMessageAndVersion record = new ApiMessageAndVersion(new KVRecord()
            .setKeyValues(request.keyValues().stream().map(kv -> new KeyValue()
                .setKey(kv.key())
                .setValue(kv.value())
            ).collect(Collectors.toList())), (short) 0);
        return ControllerResult.of(Collections.singletonList(record), resp);
    }

    public ControllerResult<DeleteKVResponseData> deleteKV(DeleteKVRequestData request) {
        log.trace("DeleteKVRequestData: {}", request);
        DeleteKVResponseData resp = new DeleteKVResponseData();
        // generate remove-kv record
        ApiMessageAndVersion record = new ApiMessageAndVersion(new RemoveKVRecord()
            .setKeys(request.keys()), (short) 0);
        return ControllerResult.of(Collections.singletonList(record), resp);
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
