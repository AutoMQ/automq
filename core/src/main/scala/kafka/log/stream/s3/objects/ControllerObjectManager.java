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

package kafka.log.stream.s3.objects;


import com.automq.stream.s3.objects.CommitStreamObjectRequest;
import com.automq.stream.s3.objects.CommitSSTObjectRequest;
import com.automq.stream.s3.objects.CommitSSTObjectResponse;
import com.automq.stream.s3.objects.ObjectManager;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.stream.s3.network.request.WrapRequest;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitSSTObjectRequestData;
import org.apache.kafka.common.message.CommitSSTObjectResponseData;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.CommitStreamObjectResponse;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectResponse;
import org.apache.kafka.metadata.stream.InRangeObjects;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ControllerObjectManager implements ObjectManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(ControllerObjectManager.class);

    private final ControllerRequestSender requestSender;
    private final StreamMetadataManager metadataManager;
    private final KafkaConfig config;
    private final int nodeId;
    private final long nodeEpoch;

    public ControllerObjectManager(ControllerRequestSender requestSender, StreamMetadataManager metadataManager, KafkaConfig config) {
        this.requestSender = requestSender;
        this.metadataManager = metadataManager;
        this.config = config;
        this.nodeId = config.brokerId();
        this.nodeEpoch = config.brokerEpoch();
    }

    @Override
    public CompletableFuture<Long> prepareObject(int count, long ttl) {
        PrepareS3ObjectRequestData request = new PrepareS3ObjectRequestData()
            .setNodeId(nodeId)
            .setPreparedCount(count)
            .setTimeToLiveInMs(ttl);
        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.PREPARE_S3_OBJECT;
            }

            @Override
            public Builder toRequestBuilder() {
                return new PrepareS3ObjectRequest.Builder(request);
            }
        };
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<PrepareS3ObjectResponse, Long> task = new RequestTask<>(req, future, response -> {
            PrepareS3ObjectResponseData resp = response.data();
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.firstS3ObjectId());
                default:
                    LOGGER.error("Error while preparing {} object, code: {}, retry later", count, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<CommitSSTObjectResponse> commitSSTObject(CommitSSTObjectRequest commitSSTObjectRequest) {
        CommitSSTObjectRequestData request = new CommitSSTObjectRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setOrderId(commitSSTObjectRequest.getOrderId())
            .setObjectId(commitSSTObjectRequest.getObjectId())
            .setObjectSize(commitSSTObjectRequest.getObjectSize())
            .setObjectStreamRanges(commitSSTObjectRequest.getStreamRanges()
                .stream()
                .map(Convertor::toObjectStreamRangeInRequest).collect(Collectors.toList()))
            .setStreamObjects(commitSSTObjectRequest.getStreamObjects()
                .stream()
                .map(Convertor::toStreamObjectInRequest).collect(Collectors.toList()))
            .setCompactedObjectIds(commitSSTObjectRequest.getCompactedObjectIds());
        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.COMMIT_SST_OBJECT;
            }

            @Override
            public Builder toRequestBuilder() {
                return new org.apache.kafka.common.requests.s3.CommitSSTObjectRequest.Builder(request);
            }
        };
        CompletableFuture<CommitSSTObjectResponse> future = new CompletableFuture<>();
        RequestTask<org.apache.kafka.common.requests.s3.CommitSSTObjectResponse, CommitSSTObjectResponse> task = new RequestTask<>(req, future, response -> {
            CommitSSTObjectResponseData resp = response.data();
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(new CommitSSTObjectResponse());
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case OBJECT_NOT_EXIST:
                case COMPACTED_OBJECTS_NOT_FOUND:
                    throw code.exception();
                default:
                    LOGGER.error("Error while committing WAL object: {}, code: {}, retry later", request, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest commitStreamObjectRequest) {
        CommitStreamObjectRequestData request = new CommitStreamObjectRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setObjectId(commitStreamObjectRequest.getObjectId())
            .setObjectSize(commitStreamObjectRequest.getObjectSize())
            .setStreamId(commitStreamObjectRequest.getStreamId())
            .setStartOffset(commitStreamObjectRequest.getStartOffset())
            .setEndOffset(commitStreamObjectRequest.getEndOffset())
            .setSourceObjectIds(commitStreamObjectRequest.getSourceObjectIds());
        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.COMMIT_STREAM_OBJECT;
            }

            @Override
            public Builder toRequestBuilder() {
                return new org.apache.kafka.common.requests.s3.CommitStreamObjectRequest.Builder(request);
            }
        };
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<CommitStreamObjectResponse, Void> task = new RequestTask<>(req, future, response -> {
            CommitStreamObjectResponseData resp = response.data();
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case OBJECT_NOT_EXIST:
                case COMPACTED_OBJECTS_NOT_FOUND:
                    throw code.exception();
                default:
                    LOGGER.error("Error while committing stream object: {}, code: {}, retry later", request, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        return this.metadataManager.fetch(streamId, startOffset, endOffset, limit).thenApply(inRangeObjects -> {
            if (inRangeObjects == null || inRangeObjects == InRangeObjects.INVALID) {
                return Collections.emptyList();
            }
            return inRangeObjects.objects();
        });
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        try {
            return this.metadataManager.getWALObjects();
        } catch (Exception e) {
            LOGGER.error("Error while get server objects", e);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        return this.metadataManager.getStreamObjects(streamId, startOffset, endOffset, limit);
    }
}
