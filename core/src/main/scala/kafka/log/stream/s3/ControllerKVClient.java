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

package kafka.log.stream.s3;

import com.automq.stream.api.KVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.stream.s3.network.request.BatchRequest;
import kafka.log.stream.s3.network.request.WrapRequest;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData.DeleteKVResponse;
import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.requests.s3.GetKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class ControllerKVClient implements KVClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerKVClient.class);
    private final ControllerRequestSender requestSender;

    public ControllerKVClient(ControllerRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue) {
        LOGGER.trace("[ControllerKVClient]: Put KV if absent: {}", keyValue);
        PutKVRequest request = new PutKVRequest()
            .setKey(keyValue.key().get())
            .setValue(keyValue.value().get().array());
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                PutKVsRequest.Builder realBuilder = (PutKVsRequest.Builder) builder;
                return realBuilder.addSubRequest(request);
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.PUT_KVS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new PutKVsRequest.Builder(
                    new PutKVsRequestData()
                ).addSubRequest(request);
            }
        };
        CompletableFuture<Value> future = new CompletableFuture<>();
        RequestTask<PutKVResponse, Value> task = new RequestTask<PutKVResponse, Value>(req, future, response -> {
            Errors code = Errors.forCode(response.errorCode());
            switch (code) {
                case NONE:
                    LOGGER.trace("[ControllerKVClient]: Put KV if absent: {}, result: {}", keyValue, response);
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                case KEY_EXIST:
                    LOGGER.warn("[ControllerKVClient]: Failed to Put KV if absent: {}, code: {}, key already exist", keyValue, code);
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Put KV if absent: {}, code: {}, retry later", keyValue, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Value> putKV(KeyValue keyValue) {
        LOGGER.trace("[ControllerKVClient]: Put KV: {}", keyValue);
        PutKVRequest request = new PutKVRequest()
            .setKey(keyValue.key().get())
            .setValue(keyValue.value().get().array())
            .setOverwrite(true);
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                PutKVsRequest.Builder realBuilder = (PutKVsRequest.Builder) builder;
                return realBuilder.addSubRequest(request);
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.PUT_KVS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new PutKVsRequest.Builder(
                    new PutKVsRequestData()
                ).addSubRequest(request);
            }
        };
        CompletableFuture<Value> future = new CompletableFuture<>();
        RequestTask<PutKVResponse, Value> task = new RequestTask<PutKVResponse, Value>(req, future, response -> {
            Errors code = Errors.forCode(response.errorCode());
            switch (code) {
                case NONE:
                    LOGGER.trace("[ControllerKVClient]: Put KV: {}, result: {}", keyValue, response);
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Put KV: {}, code: {}, retry later", keyValue, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return null;
    }

    @Override
    public CompletableFuture<Value> getKV(Key key) {
        LOGGER.trace("[ControllerKVClient]: Get KV: {}", key);
        GetKVRequest request = new GetKVRequest()
            .setKey(key.get());
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                GetKVsRequest.Builder realBuilder = (GetKVsRequest.Builder) builder;
                return realBuilder.addSubRequest(request);
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.GET_KVS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new GetKVsRequest.Builder(
                    new GetKVsRequestData()
                ).addSubRequest(request);
            }
        };
        CompletableFuture<Value> future = new CompletableFuture<>();
        RequestTask<GetKVResponse, Value> task = new RequestTask<>(req, future, response -> {
            Errors code = Errors.forCode(response.errorCode());
            switch (code) {
                case NONE:
                    Value val = Value.of(response.value());
                    LOGGER.trace("[ControllerKVClient]: Get KV: {}, result: {}", key, response);
                    return ResponseHandleResult.withSuccess(val);
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Get KV: {}, code: {}, retry later", key, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Value> delKV(Key key) {
        LOGGER.trace("[ControllerKVClient]: Delete KV: {}", key);
        DeleteKVRequest request = new DeleteKVRequest()
            .setKey(key.get());
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                DeleteKVsRequest.Builder realBuilder = (DeleteKVsRequest.Builder) builder;
                return realBuilder.addSubRequest(request);
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.DELETE_KVS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new DeleteKVsRequest.Builder(
                    new DeleteKVsRequestData()
                ).addSubRequest(request);
            }
        };

        CompletableFuture<Value> future = new CompletableFuture<>();
        RequestTask<DeleteKVResponse, Value> task = new RequestTask<>(req, future, response -> {
            Errors code = Errors.forCode(response.errorCode());
            switch (code) {
                case NONE:
                    LOGGER.trace("[ControllerKVClient]: Delete KV: {}, result: {}", key, response);
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Delete KV: {}, code: {}, retry later", key, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }
}
