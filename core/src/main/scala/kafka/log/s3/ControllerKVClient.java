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

package kafka.log.s3;

import com.automq.elasticstream.client.api.KVClient;
import com.automq.elasticstream.client.api.KeyValue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import kafka.log.s3.network.ControllerRequestSender;
import org.apache.kafka.common.message.DeleteKVRequestData;
import org.apache.kafka.common.message.GetKVRequestData;
import org.apache.kafka.common.message.GetKVResponseData;
import org.apache.kafka.common.message.PutKVRequestData;
import org.apache.kafka.common.message.PutKVResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.DeleteKVRequest;
import org.apache.kafka.common.requests.s3.GetKVRequest;
import org.apache.kafka.common.requests.s3.PutKVRequest;
import org.apache.kafka.common.requests.s3.PutKVRequest.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerKVClient implements KVClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerKVClient.class);
    private final ControllerRequestSender requestSender;

    public ControllerKVClient(ControllerRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public CompletableFuture<Void> putKV(List<KeyValue> list) {
        LOGGER.trace("[ControllerKVClient]: Put KV: {}", list);
        PutKVRequest.Builder requestBuilder = new Builder(
            new PutKVRequestData()
                .setKeyValues(list.stream().map(kv -> new PutKVRequestData.KeyValue()
                    .setKey(kv.key())
                    .setValue(kv.value().array())
                ).collect(Collectors.toList()))
        );
        return this.requestSender.send(requestBuilder, PutKVResponseData.class)
            .thenApply(resp -> {
                Errors code = Errors.forCode(resp.errorCode());
                switch (code) {
                    case NONE:
                        LOGGER.trace("[ControllerKVClient]: Put KV: {}, result: {}", list, resp);
                        return null;
                    default:
                        LOGGER.error("[ControllerKVClient]: Failed to Put KV: {}, code: {}", list, code);
                        throw code.exception();
                }
            });
    }

    @Override
    public CompletableFuture<List<KeyValue>> getKV(List<String> list) {
        LOGGER.trace("[ControllerKVClient]: Get KV: {}", list);
        GetKVRequest.Builder requestBuilder = new GetKVRequest.Builder(
            new GetKVRequestData()
                .setKeys(list)
        );
        return this.requestSender.send(requestBuilder, GetKVResponseData.class)
            .thenApply(resp -> {
                Errors code = Errors.forCode(resp.errorCode());
                switch (code) {
                    case NONE:
                        List<KeyValue> keyValues = resp.keyValues()
                            .stream()
                            .map(kv -> KeyValue.of(kv.key(), kv.value() != null ? ByteBuffer.wrap(kv.value()) : null))
                            .collect(Collectors.toList());
                        LOGGER.trace("[ControllerKVClient]: Get KV: {}, result: {}", list, keyValues);
                        return keyValues;
                    default:
                        LOGGER.error("[ControllerKVClient]: Failed to Get KV: {}, code: {}", String.join(",", list), code);
                        throw code.exception();
                }
            });
    }

    @Override
    public CompletableFuture<Void> delKV(List<String> list) {
        LOGGER.trace("[ControllerKVClient]: Delete KV: {}", String.join(",", list));
        DeleteKVRequest.Builder requestBuilder = new DeleteKVRequest.Builder(
            new DeleteKVRequestData()
                .setKeys(list)
        );
        return this.requestSender.send(requestBuilder, PutKVResponseData.class)
            .thenApply(resp -> {
                Errors code = Errors.forCode(resp.errorCode());
                switch (code) {
                    case NONE:
                        LOGGER.trace("[ControllerKVClient]: Delete KV: {}, result: {}", list, resp);
                        return null;
                    default:
                        LOGGER.error("[ControllerKVClient]: Failed to Delete KV: {}, code: {}", String.join(",", list), code);
                        throw code.exception();
                }
            });
    }
}
