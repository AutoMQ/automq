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

package kafka.log.stream.s3.node;

import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.request.WrapRequest;

import org.apache.kafka.common.message.AutomqGetNodesRequestData;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;
import org.apache.kafka.common.message.AutomqRegisterNodeRequestData;
import org.apache.kafka.common.message.AutomqRegisterNodeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.s3.AutomqGetNodesRequest;
import org.apache.kafka.common.requests.s3.AutomqGetNodesResponse;
import org.apache.kafka.common.requests.s3.AutomqRegisterNodeRequest;
import org.apache.kafka.common.requests.s3.AutomqRegisterNodeResponse;
import org.apache.kafka.controller.stream.NodeMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult.withRetry;
import static kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult.withSuccess;

public class NodeManagerStub implements NodeManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeManagerStub.class);
    private final ControllerRequestSender requestSender;
    private final int nodeId;
    private final long epoch;
    private NodeMetadata nodeMetadata;
    private CompletableFuture<Void> lastCf;

    public NodeManagerStub(ControllerRequestSender requestSender, int nodeId, long epoch,
        Map<String, String> staticTags) {
        this.requestSender = requestSender;
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.lastCf = getNodeMetadata0().thenAccept(opt -> {
            synchronized (NodeManagerStub.this) {
                nodeMetadata = opt.map(o -> {
                    Map<String, String> newTags = new HashMap<>(o.getTags());
                    newTags.putAll(staticTags);
                    return new NodeMetadata(o.getNodeId(), o.getNodeEpoch(), o.getWalConfig(), newTags);
                }).orElseGet(() -> new NodeMetadata(nodeId, epoch, "", new HashMap<>(staticTags)));
            }
        });

    }

    public synchronized CompletableFuture<Void> update(Function<NodeMetadata, Optional<NodeMetadata>> updater) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        lastCf.whenComplete((rst, e) -> {
            if (e == null) {
                update0(updater, cf);
            } else {
                cf.completeExceptionally(e);
            }
        });
        this.lastCf = cf;
        return cf;
    }

    public void update0(Function<NodeMetadata, Optional<NodeMetadata>> updater, CompletableFuture<Void> cf) {
        try {
            Optional<NodeMetadata> newNodeMetadataOpt = updater.apply(nodeMetadata);
            if (newNodeMetadataOpt.isEmpty()) {
                cf.complete(null);
                return;
            }
            NodeMetadata newNodeMetadata = newNodeMetadataOpt.get();
            newNodeMetadata.setNodeEpoch(epoch);
            this.nodeMetadata = newNodeMetadata;
            AutomqRegisterNodeRequestData.TagCollection tagCollection = new AutomqRegisterNodeRequestData.TagCollection();
            newNodeMetadata.getTags().forEach((k, v) -> tagCollection.add(new AutomqRegisterNodeRequestData.Tag().setKey(k).setValue(v)));
            AutomqRegisterNodeRequestData request = new AutomqRegisterNodeRequestData()
                .setNodeId(nodeId)
                .setNodeEpoch(epoch)
                .setWalConfig(newNodeMetadata.getWalConfig())
                .setTags(tagCollection);

            WrapRequest req = new WrapRequest() {
                @Override
                public ApiKeys apiKey() {
                    return ApiKeys.AUTOMQ_REGISTER_NODE;
                }

                @Override
                public AbstractRequest.Builder<AutomqRegisterNodeRequest> toRequestBuilder() {
                    return new AutomqRegisterNodeRequest.Builder(request);
                }

                @Override
                public String toString() {
                    return request.toString();
                }
            };

            ControllerRequestSender.RequestTask<AutomqRegisterNodeResponse, Void> task = new ControllerRequestSender.RequestTask<>(req, cf,
                response -> {
                    AutomqRegisterNodeResponseData resp = response.data();
                    Errors code = Errors.forCode(resp.errorCode());
                    switch (code) {
                        case NONE:
                            return withSuccess(null);
                        case NODE_EPOCH_EXPIRED:
                            LOGGER.error("Node epoch expired: {}, code: {}", req, code);
                            throw code.exception();
                        default:
                            LOGGER.error("Error while AUTOMQ_REGISTER_NODE: {}, code: {}, retry later", req, code);
                            return withRetry();
                    }
                });
            this.requestSender.send(task);
        } catch (Throwable e) {
            cf.completeExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<NodeMetadata> getNodeMetadata() {
        return lastCf.thenApply(nil -> this.nodeMetadata);
    }

    private CompletableFuture<Optional<NodeMetadata>> getNodeMetadata0() {
        AutomqGetNodesRequestData request = new AutomqGetNodesRequestData().setNodeIds(List.of(nodeId));

        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.AUTOMQ_GET_NODES;
            }

            @Override
            public AbstractRequest.Builder<AutomqGetNodesRequest> toRequestBuilder() {
                return new AutomqGetNodesRequest.Builder(request);
            }

            @Override
            public String toString() {
                return request.toString();
            }
        };

        CompletableFuture<Optional<NodeMetadata>> future = new CompletableFuture<>();
        ControllerRequestSender.RequestTask<AutomqGetNodesResponse, Optional<NodeMetadata>> task = new ControllerRequestSender.RequestTask<>(req, future,
            response -> {
                AutomqGetNodesResponseData resp = response.data();
                Errors code = Errors.forCode(resp.errorCode());
                switch (code) {
                    case NONE:
                        return withSuccess(resp.nodes().stream().map(NodeManagerStub::from).filter(n -> n.getNodeId() == nodeId).findAny());
                    default:
                        LOGGER.error("Error while AUTOMQ_GET_NODES: {}, code: {}, retry later", req, code);
                        return withRetry();
                }
            });
        this.requestSender.send(task);
        return future;
    }

    static NodeMetadata from(AutomqGetNodesResponseData.NodeMetadata src) {
        return new NodeMetadata(
            src.nodeId(),
            src.nodeEpoch(),
            src.walConfig(),
            src.tags().stream().collect(Collectors.toMap(AutomqGetNodesResponseData.Tag::key, AutomqGetNodesResponseData.Tag::value))
        );
    }
}
