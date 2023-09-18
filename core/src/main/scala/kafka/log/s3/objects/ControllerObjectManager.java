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

package kafka.log.s3.objects;


import kafka.log.s3.metadata.StreamMetadataManager;
import kafka.log.s3.network.ControllerRequestSender;
import kafka.log.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest.Builder;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
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

    public ControllerObjectManager(ControllerRequestSender requestSender, StreamMetadataManager metadataManager, KafkaConfig config) {
        this.requestSender = requestSender;
        this.metadataManager = metadataManager;
        this.config = config;
    }

    @Override
    public CompletableFuture<Long> prepareObject(int count, long ttl) {
        PrepareS3ObjectRequest.Builder request = new Builder(
                new PrepareS3ObjectRequestData()
                        .setBrokerId(config.brokerId())
                        .setPreparedCount(count)
                        .setTimeToLiveInMs(ttl)
        );
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<PrepareS3ObjectResponseData, Long> task = new RequestTask<>(future, request, PrepareS3ObjectResponseData.class, resp -> {
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
    public CompletableFuture<CommitWALObjectResponse> commitWALObject(CommitWALObjectRequest request) {
        org.apache.kafka.common.requests.s3.CommitWALObjectRequest.Builder wrapRequestBuilder = new org.apache.kafka.common.requests.s3.CommitWALObjectRequest.Builder(
                new CommitWALObjectRequestData()
                        .setBrokerId(config.brokerId())
                        .setOrderId(request.getOrderId())
                        .setObjectId(request.getObjectId())
                        .setObjectSize(request.getObjectSize())
                        .setObjectStreamRanges(request.getStreamRanges()
                                .stream()
                                .map(ObjectStreamRange::toObjectStreamRangeInRequest).collect(Collectors.toList()))
                        .setStreamObjects(request.getStreamObjects()
                                .stream()
                                .map(StreamObject::toStreamObjectInRequest).collect(Collectors.toList()))
                        .setCompactedObjectIds(request.getCompactedObjectIds()));
        CompletableFuture<CommitWALObjectResponse> future = new CompletableFuture<>();
        RequestTask<CommitWALObjectResponseData, CommitWALObjectResponse> task = new RequestTask<>(future, wrapRequestBuilder, CommitWALObjectResponseData.class, resp -> {
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(new CommitWALObjectResponse());
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
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        org.apache.kafka.common.requests.s3.CommitStreamObjectRequest.Builder wrapRequestBuilder = new org.apache.kafka.common.requests.s3.CommitStreamObjectRequest.Builder(
                new org.apache.kafka.common.message.CommitStreamObjectRequestData()
                        .setObjectId(request.getObjectId())
                        .setObjectSize(request.getObjectSize())
                        .setStreamId(request.getStreamId())
                        .setStartOffset(request.getStartOffset())
                        .setEndOffset(request.getEndOffset())
                        .setSourceObjectIds(request.getSourceObjectIds()));
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<CommitStreamObjectResponseData, Void> task = new RequestTask<>(future, wrapRequestBuilder, CommitStreamObjectResponseData.class, resp -> {
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
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
    public List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        try {
            return this.metadataManager.fetch(streamId, startOffset, endOffset, limit).thenApply(inRangeObjects -> {
                if (inRangeObjects == null || inRangeObjects == InRangeObjects.INVALID) {
                    List<S3ObjectMetadata> objects = Collections.emptyList();
                    return objects;
                }
                return inRangeObjects.objects();
            }).get();
        } catch (Exception e) {
            LOGGER.error("Error while get objects, streamId: {}, startOffset: {}, endOffset: {}, limit: {}", streamId, startOffset, endOffset, limit,
                    e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<S3ObjectMetadata> getServerObjects() {
        try {
            return this.metadataManager.getWALObjects();
        } catch (Exception e) {
            LOGGER.error("Error while get server objects", e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<S3ObjectMetadata> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        try {
            return this.metadataManager.getStreamObjects(streamId, startOffset, endOffset, limit).get();
        } catch (Exception e) {
            LOGGER.error("Error while get stream objects, streamId: {}, startOffset: {}, endOffset: {}, limit: {}", streamId, startOffset, endOffset,
                limit,
                e);
            return Collections.emptyList();
        }
    }
}
