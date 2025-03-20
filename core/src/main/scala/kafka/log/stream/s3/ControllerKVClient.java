/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3;

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

import com.automq.stream.api.KVClient;
import com.automq.stream.api.KeyValue;
import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ControllerKVClient implements KVClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerKVClient.class);
    private final ControllerRequestSender requestSender;

    public ControllerKVClient(ControllerRequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ControllerKVClient]: Put KV if absent: {}", keyValue);
        }
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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ControllerKVClient]: Put KV if absent: {}, result: {}", keyValue, response);
                    }
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ControllerKVClient]: Put KV: {}", keyValue);
        }
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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ControllerKVClient]: Put KV: {}, result: {}", keyValue, response);
                    }
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Put KV: {}, code: {}, retry later", keyValue, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Value> getKV(Key key) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ControllerKVClient]: Get KV: {}", key);
        }
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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ControllerKVClient]: Get KV: {}, result: {}", key, response);
                    }
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ControllerKVClient]: Delete KV: {}", key);
        }
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
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("[ControllerKVClient]: Delete KV: {}, result: {}", key, response);
                    }
                    return ResponseHandleResult.withSuccess(Value.of(response.value()));
                case KEY_NOT_EXIST:
                    LOGGER.info("[ControllerKVClient]: Delete KV: {}, result: KEY_NOT_EXIST", key);
                    return ResponseHandleResult.withSuccess(Value.of((ByteBuffer) null));
                default:
                    LOGGER.error("[ControllerKVClient]: Failed to Delete KV: {}, code: {}, retry later", key, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }
}
