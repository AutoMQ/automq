/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.objects;

import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.exceptions.AutoMQException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectResponse;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.utils.FutureUtil;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.stream.s3.network.request.WrapRequest;
import org.apache.kafka.common.message.CommitStreamObjectRequestData;
import org.apache.kafka.common.message.CommitStreamObjectResponseData;
import org.apache.kafka.common.message.CommitStreamSetObjectRequestData;
import org.apache.kafka.common.message.CommitStreamSetObjectResponseData;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.CommitStreamObjectResponse;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectResponse;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

public class ControllerObjectManager implements ObjectManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(ControllerObjectManager.class);

    private final ControllerRequestSender requestSender;
    private final StreamMetadataManager metadataManager;
    private final int nodeId;
    private final long nodeEpoch;
    private final Supplier<AutoMQVersion> version;
    private final boolean failoverMode;

    public ControllerObjectManager(ControllerRequestSender requestSender, StreamMetadataManager metadataManager,
        int nodeId, long nodeEpoch, Supplier<AutoMQVersion> version, boolean failoverMode) {
        this.requestSender = requestSender;
        this.metadataManager = metadataManager;
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.version = version;
        this.failoverMode = failoverMode;
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
    public CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject(
        CommitStreamSetObjectRequest commitStreamSetObjectRequest) {
        try {
            return commitStreamSetObject0(commitStreamSetObjectRequest);
        } catch (Throwable e) {
            return FutureUtil.failedFuture(e);
        }
    }

    public CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject0(
        CommitStreamSetObjectRequest commitStreamSetObjectRequest) {
        CommitStreamSetObjectRequestData request = new CommitStreamSetObjectRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setOrderId(commitStreamSetObjectRequest.getOrderId())
            .setObjectId(commitStreamSetObjectRequest.getObjectId())
            .setObjectSize(commitStreamSetObjectRequest.getObjectSize())
            .setObjectStreamRanges(commitStreamSetObjectRequest.getStreamRanges()
                .stream()
                .map(Convertor::toObjectStreamRangeInRequest).collect(Collectors.toList()))
            .setStreamObjects(commitStreamSetObjectRequest.getStreamObjects()
                .stream()
                .map(s -> Convertor.toStreamObjectInRequest(s, version.get())).collect(Collectors.toList()))
            .setCompactedObjectIds(commitStreamSetObjectRequest.getCompactedObjectIds())
            .setFailoverMode(failoverMode);
        if (commitStreamSetObjectRequest.getObjectId() != NOOP_OBJECT_ID && commitStreamSetObjectRequest.getAttributes() == ObjectAttributes.UNSET.attributes()) {
            throw new IllegalArgumentException("[BUG]attributes must be set");
        }
        if (version.get().isObjectAttributesSupported()) {
            request.setAttributes(commitStreamSetObjectRequest.getAttributes());
        }
        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.COMMIT_STREAM_SET_OBJECT;
            }

            @Override
            public Builder toRequestBuilder() {
                return new org.apache.kafka.common.requests.s3.CommitStreamSetObjectRequest.Builder(request);
            }
        };
        CompletableFuture<CommitStreamSetObjectResponse> future = new CompletableFuture<>();
        RequestTask<org.apache.kafka.common.requests.s3.CommitStreamSetObjectResponse, CommitStreamSetObjectResponse> task = new RequestTask<>(req, future, response -> {
            CommitStreamSetObjectResponseData resp = response.data();
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(new CommitStreamSetObjectResponse());
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case OBJECT_NOT_EXIST:
                case COMPACTED_OBJECTS_NOT_FOUND:
                    throw code.exception();
                default:
                    LOGGER.error("Error while committing stream set object: {}, code: {}, retry later", request, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    public CompletableFuture<Void> compactStreamObject(CompactStreamObjectRequest compactStreamObjectRequest) {
        CommitStreamObjectRequestData request = new CommitStreamObjectRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setObjectId(compactStreamObjectRequest.getObjectId())
            .setObjectSize(compactStreamObjectRequest.getObjectSize())
            .setStreamId(compactStreamObjectRequest.getStreamId())
            .setStreamEpoch(compactStreamObjectRequest.getStreamEpoch())
            .setStartOffset(compactStreamObjectRequest.getStartOffset())
            .setEndOffset(compactStreamObjectRequest.getEndOffset())
            .setSourceObjectIds(compactStreamObjectRequest.getSourceObjectIds());
        if (version.get().isObjectAttributesSupported()) {
            request.setAttributes(compactStreamObjectRequest.getAttributes());
        }
        if (version.get().isCompositeObjectSupported()) {
            request.setOperations(compactStreamObjectRequest.getOperations().stream().map(CompactOperations::value).collect(Collectors.toList()));
        }
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
                case STREAM_NOT_EXIST:
                case STREAM_FENCED:
                    LOGGER.warn("Stream fenced or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                default:
                    LOGGER.error("Error while committing stream object: {}, code: {}, retry later", request, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return this.metadataManager.fetch(streamId, startOffset, endOffset, limit).thenApply(inRangeObjects -> {
            if (inRangeObjects == null || inRangeObjects == InRangeObjects.INVALID) {
                LOGGER.error("Unexpected getObjects result={} from streamId={} [{}, {}) limit={}", inRangeObjects, streamId, startOffset, endOffset, limit);
                throw new AutoMQException("Unexpected getObjects result");
            }
            return inRangeObjects.objects();
        });
    }

    @Override
    public boolean isObjectExist(long objectId) {
        return this.metadataManager.isObjectExist(objectId);
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        try {
            return this.metadataManager.getStreamSetObjects();
        } catch (Exception e) {
            LOGGER.error("Error while get server objects", e);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return this.metadataManager.getStreamObjects(streamId, startOffset, endOffset, limit);
    }

    @Override
    public CompletableFuture<Integer> getObjectsCount() {
        return CompletableFuture.completedFuture(this.metadataManager.getObjectsCount());
    }
}
