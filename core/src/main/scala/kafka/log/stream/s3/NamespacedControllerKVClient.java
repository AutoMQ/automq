/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

import com.automq.stream.api.NamespacedKVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;
import com.automq.stream.api.NamespacedKeyValue;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.stream.s3.network.request.BatchRequest;
import kafka.log.stream.s3.network.request.WrapRequest;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.common.message.PutKVsResponseData.PutKVResponse;
import org.apache.kafka.common.message.GetKVsRequestData.GetKVRequest;
import org.apache.kafka.common.message.GetKVsResponseData.GetKVResponse;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.DeleteKVsResponseData.DeleteKVResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.requests.s3.GetKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A NamespacedControllerKVClient that extends ControllerKVClient to add namespace and epoch support.
 * This implementation maintains namespace isolation by prefixing keys with the namespace
 * and stores epoch information in special keys within each namespace.
 */
public class NamespacedControllerKVClient extends ControllerKVClient implements NamespacedKVClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamespacedControllerKVClient.class);
    private static final String EPOCH_KEY_PREFIX = "__epoch__:";

    public NamespacedControllerKVClient(ControllerRequestSender requestSender) {
        super(requestSender);
    }

    @Override
    public CompletableFuture<Value> putNamespacedKVIfAbsent(NamespacedKeyValue namespacedKeyValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Put namespaced KV if absent: {}", namespacedKeyValue);
        }
        
        // First check if we need to initialize or update the epoch
        return getNamespaceEpoch(namespacedKeyValue.getNamespace())
            .thenCompose(currentEpoch -> {
                if (currentEpoch != namespacedKeyValue.getEpoch()) {
                    LOGGER.warn("[NamespacedControllerKVClient]: Epoch mismatch for namespace {}: expected {}, got {}",
                        namespacedKeyValue.getNamespace(), namespacedKeyValue.getEpoch(), currentEpoch);
                    return CompletableFuture.completedFuture(Value.of((ByteBuffer) null));
                }

                String namespacedKey = namespacedKeyValue.getNamespacedKey();
                PutKVRequest request = new PutKVRequest()
                    .setKey(namespacedKey)
                    .setValue(namespacedKeyValue.getKeyValue().value().get().array());

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
                RequestTask<PutKVResponse, Value> task = new RequestTask<>(req, future, response -> {
                    Errors code = Errors.forCode(response.errorCode());
                    switch (code) {
                        case NONE:
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("[NamespacedControllerKVClient]: Put namespaced KV if absent success: {}", namespacedKeyValue);
                            }
                            return ResponseHandleResult.withSuccess(Value.of(response.value()));
                        case KEY_EXIST:
                            LOGGER.warn("[NamespacedControllerKVClient]: Failed to put namespaced KV if absent: {}, key already exists", namespacedKeyValue);
                            return ResponseHandleResult.withSuccess(Value.of(response.value()));
                        default:
                            LOGGER.error("[NamespacedControllerKVClient]: Failed to put namespaced KV if absent: {}, code: {}, retry later", namespacedKeyValue, code);
                            return ResponseHandleResult.withRetry();
                    }
                });
                getRequestSender().send(task);
                return future;
            });
    }

    @Override
    public CompletableFuture<Value> putNamespacedKV(NamespacedKeyValue namespacedKeyValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Put namespaced KV: {}", namespacedKeyValue);
        }

        return getNamespaceEpoch(namespacedKeyValue.getNamespace())
            .thenCompose(currentEpoch -> {
                if (currentEpoch != namespacedKeyValue.getEpoch()) {
                    LOGGER.warn("[NamespacedControllerKVClient]: Epoch mismatch for namespace {}: expected {}, got {}",
                        namespacedKeyValue.getNamespace(), namespacedKeyValue.getEpoch(), currentEpoch);
                    return CompletableFuture.completedFuture(Value.of((ByteBuffer) null));
                }

                String namespacedKey = namespacedKeyValue.getNamespacedKey();
                PutKVRequest request = new PutKVRequest()
                    .setKey(namespacedKey)
                    .setValue(namespacedKeyValue.getKeyValue().value().get().array())
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
                RequestTask<PutKVResponse, Value> task = new RequestTask<>(req, future, response -> {
                    Errors code = Errors.forCode(response.errorCode());
                    switch (code) {
                        case NONE:
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("[NamespacedControllerKVClient]: Put namespaced KV success: {}", namespacedKeyValue);
                            }
                            return ResponseHandleResult.withSuccess(Value.of(response.value()));
                        default:
                            LOGGER.error("[NamespacedControllerKVClient]: Failed to put namespaced KV: {}, code: {}, retry later", namespacedKeyValue, code);
                            return ResponseHandleResult.withRetry();
                    }
                });
                getRequestSender().send(task);
                return future;
            });
    }

    @Override
    public CompletableFuture<Value> getNamespacedKV(String namespace, Key key) {
        String namespacedKey = namespace + ":" + key.get();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Get namespaced KV: {}", namespacedKey);
        }

        GetKVRequest request = new GetKVRequest()
            .setKey(namespacedKey);

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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[NamespacedControllerKVClient]: Get namespaced KV success: {} = {}", namespacedKey, val);
                    }
                    return ResponseHandleResult.withSuccess(val);
                default:
                    LOGGER.error("[NamespacedControllerKVClient]: Failed to get namespaced KV: {}, code: {}, retry later", namespacedKey, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        getRequestSender().send(task);
        return future;
    }

    @Override
    public CompletableFuture<Value> delNamespacedKV(String namespace, Key key) {
        String namespacedKey = namespace + ":" + key.get();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Delete namespaced KV: {}", namespacedKey);
        }

        DeleteKVRequest request = new DeleteKVRequest()
            .setKey(namespacedKey);

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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[NamespacedControllerKVClient]: Delete namespaced KV success: {}", namespacedKey);
                    }
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                case KEY_NOT_EXIST:
                    LOGGER.info("[NamespacedControllerKVClient]: Delete namespaced KV: {}, result: KEY_NOT_EXIST", namespacedKey);
                    return ResponseHandleResult.withSuccess(Value.of((ByteBuffer) null));
                default:
                    LOGGER.error("[NamespacedControllerKVClient]: Failed to delete namespaced KV: {}, code: {}, retry later", namespacedKey, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        getRequestSender().send(task);
        return future;
    }

    @Override
    public CompletableFuture<Long> getNamespaceEpoch(String namespace) {
        String epochKey = EPOCH_KEY_PREFIX + namespace;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Get namespace epoch: {}", namespace);
        }

        return getKV(Key.of(epochKey))
            .thenApply(value -> {
                if (value == null || value.isNull()) {
                    return 0L;
                }
                return value.get().getLong();
            });
    }

    @Override
    public CompletableFuture<Long> incrementNamespaceEpoch(String namespace) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[NamespacedControllerKVClient]: Increment namespace epoch: {}", namespace);
        }

        return getNamespaceEpoch(namespace)
            .thenCompose(currentEpoch -> {
                long newEpoch = currentEpoch + 1;
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(newEpoch);
                buffer.flip();

                String epochKey = EPOCH_KEY_PREFIX + namespace;
                return putKV(KeyValue.of(epochKey, buffer))
                    .thenApply(value -> newEpoch);
            });
    }

    protected ControllerRequestSender getRequestSender() {
        return super.requestSender;
    }
} 