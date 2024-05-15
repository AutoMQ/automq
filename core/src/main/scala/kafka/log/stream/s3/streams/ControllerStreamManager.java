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

package kafka.log.stream.s3.streams;

import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.streams.StreamManager;
import java.util.Map;
import java.util.function.Supplier;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.stream.s3.network.request.BatchRequest;
import kafka.log.stream.s3.network.request.WrapRequest;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.message.CloseStreamsRequestData;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.message.CloseStreamsResponseData.CloseStreamResponse;
import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.message.DeleteStreamsRequestData;
import org.apache.kafka.common.message.DeleteStreamsRequestData.DeleteStreamRequest;
import org.apache.kafka.common.message.DeleteStreamsResponseData.DeleteStreamResponse;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.OpenStreamsRequestData;
import org.apache.kafka.common.message.OpenStreamsRequestData.OpenStreamRequest;
import org.apache.kafka.common.message.OpenStreamsResponseData.OpenStreamResponse;
import org.apache.kafka.common.message.TrimStreamsRequestData;
import org.apache.kafka.common.message.TrimStreamsRequestData.TrimStreamRequest;
import org.apache.kafka.common.message.TrimStreamsResponseData.TrimStreamResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.CloseStreamsRequest;
import org.apache.kafka.common.requests.s3.CreateStreamsRequest;
import org.apache.kafka.common.requests.s3.DeleteStreamsRequest;
import org.apache.kafka.common.requests.s3.GetOpeningStreamsRequest;
import org.apache.kafka.common.requests.s3.GetOpeningStreamsResponse;
import org.apache.kafka.common.requests.s3.OpenStreamsRequest;
import org.apache.kafka.common.requests.s3.TrimStreamsRequest;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ControllerStreamManager implements StreamManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStreamManager.class);
    private final StreamMetadataManager streamMetadataManager;
    private final int nodeId;
    private final long nodeEpoch;

    private final KafkaConfig config;
    private final ControllerRequestSender requestSender;
    private final Supplier<AutoMQVersion> version;

    public ControllerStreamManager(StreamMetadataManager streamMetadataManager, ControllerRequestSender requestSender,
        KafkaConfig config, Supplier<AutoMQVersion> version) {
        this.streamMetadataManager = streamMetadataManager;
        this.config = config;
        this.nodeId = config.brokerId();
        this.nodeEpoch = config.nodeEpoch();
        this.requestSender = requestSender;
        this.version = version;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        return getOpeningStreams(nodeId, nodeEpoch, false);
    }

    public CompletableFuture<List<StreamMetadata>> getOpeningStreams(int nodeId, long nodeEpoch, boolean failoverMode) {
        GetOpeningStreamsRequestData request = new GetOpeningStreamsRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setFailoverMode(failoverMode);
        WrapRequest req = new WrapRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.GET_OPENING_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new GetOpeningStreamsRequest.Builder(request);
            }
        };

        CompletableFuture<List<StreamMetadata>> future = new CompletableFuture<>();
        RequestTask<GetOpeningStreamsResponse, List<StreamMetadata>> task = new RequestTask<GetOpeningStreamsResponse, List<StreamMetadata>>(req, future,
            response -> {
                GetOpeningStreamsResponseData resp = response.data();
                Errors code = Errors.forCode(resp.errorCode());
                switch (code) {
                    case NONE:
                        return ResponseHandleResult.withSuccess(resp.streamMetadataList().stream()
                            .map(m -> new StreamMetadata(m.streamId(), m.epoch(), m.startOffset(), m.endOffset(), StreamState.OPENED,
                                m.tags().stream().collect(Collectors.toMap(GetOpeningStreamsResponseData.Tag::key, GetOpeningStreamsResponseData.Tag::value))))
                            .collect(Collectors.toList()));
                    case NODE_EPOCH_EXPIRED:
                        LOGGER.error("Node epoch expired: {}, code: {}", req, code);
                        throw code.exception();
                    default:
                        LOGGER.error("Error while getting streams offset: {}, code: {}, retry later", req, code);
                        return ResponseHandleResult.withRetry();
                }
            });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        return CompletableFuture.completedFuture(this.streamMetadataManager.getStreamMetadataList(streamIds));
    }

    @Override
    public CompletableFuture<Long> createStream(Map<String, String> tags) {
        CreateStreamRequest request = new CreateStreamRequest().setNodeId(nodeId);
        if (version.get().isStreamTagsSupported() && tags != null && !tags.isEmpty()) {
            CreateStreamsRequestData.TagCollection tagCollection = new CreateStreamsRequestData.TagCollection();
            tags.forEach((k, v) -> tagCollection.add(new CreateStreamsRequestData.Tag().setKey(k).setValue(v)));
            request.setTags(tagCollection);
        }
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                CreateStreamsRequest.Builder realBuilder = (CreateStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(request);
                return realBuilder;
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.CREATE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new CreateStreamsRequest.Builder(
                    new CreateStreamsRequestData()
                        .setNodeId(nodeId)
                        .setNodeEpoch(nodeEpoch)).addSubRequest(request);
            }
        };
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponse, Long> task = new RequestTask<CreateStreamResponse, Long>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", req, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                default:
                    LOGGER.error("Error while creating stream: {}, code: {}, retry later", req, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        OpenStreamRequest request = new OpenStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch);
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                OpenStreamsRequest.Builder realBuilder = (OpenStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(request);
                return realBuilder;
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.OPEN_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new OpenStreamsRequest.Builder(
                    new OpenStreamsRequestData()
                        .setNodeId(nodeId)
                        .setNodeEpoch(nodeEpoch)).addSubRequest(request);
            }

            @Override
            public String toString() {
                return request.toString();
            }
        };
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        RequestTask task = new RequestTask<OpenStreamResponse, StreamMetadata>(req, future, resp -> {
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return ResponseHandleResult.withSuccess(
                        new StreamMetadata(streamId, epoch, resp.startOffset(), resp.nextOffset(), StreamState.OPENED));
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist, stream {}, epoch {}, code: {}", streamId, epoch, code);
                    throw code.exception();
                case STREAM_FENCED:
                    LOGGER.warn("[STREAM_FENCED] open stream failed streamId={}, epoch {}, code: {}", streamId, epoch, code);
                    throw code.exception();
                case STREAM_NOT_EXIST:
                case STREAM_INNER_ERROR:
                    LOGGER.error("Unexpected error while opening stream: {}, epoch {}, code: {}", streamId, epoch, code);
                    throw code.exception();
                case STREAM_NOT_CLOSED:
                    LOGGER.warn("open stream fail: {}, epoch {}, code: STREAM_NOT_CLOSED, retry later", streamId, epoch);
                    return ResponseHandleResult.withRetry();
                default:
                    LOGGER.error("Error while opening stream: {}, epoch {}, code: {}, retry later", streamId, epoch, code);
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        TrimStreamRequest request = new TrimStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch)
            .setNewStartOffset(newStartOffset);
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                TrimStreamsRequest.Builder realBuilder = (TrimStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(request);
                return realBuilder;
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.TRIM_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new TrimStreamsRequest.Builder(
                    new TrimStreamsRequestData()
                        .setNodeId(nodeId)
                        .setNodeEpoch(nodeEpoch)).addSubRequest(request);
            }
        };
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<TrimStreamResponse, Void> task = new RequestTask<>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case STREAM_NOT_EXIST:
                case STREAM_FENCED:
                case STREAM_NOT_OPENED:
                case OFFSET_NOT_MATCHED:
                case STREAM_INNER_ERROR:
                    LOGGER.error("Unexpected error while trimming stream: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                default:
                    LOGGER.warn("Error while trimming stream: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long epoch) {
        return closeStream(streamId, epoch, nodeId, nodeEpoch);
    }

    public CompletableFuture<Void> closeStream(long streamId, long epoch, int nodeId, long nodeEpoch) {
        CloseStreamRequest request = new CloseStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch);
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                CloseStreamsRequest.Builder realBuilder = (CloseStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(request);
                return realBuilder;
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.CLOSE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new CloseStreamsRequest.Builder(
                    new CloseStreamsRequestData()
                        .setNodeId(nodeId)
                        .setNodeEpoch(nodeEpoch)).addSubRequest(request);
            }
        };
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<CloseStreamResponse, Void> task = new RequestTask<CloseStreamResponse, Void>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case STREAM_NOT_EXIST:
                case STREAM_FENCED:
                case STREAM_INNER_ERROR:
                    LOGGER.error("Unexpected error while closing stream: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                default:
                    LOGGER.warn("Error while closing stream: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        DeleteStreamRequest request = new DeleteStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch);
        WrapRequest req = new BatchRequest() {
            @Override
            public Builder addSubRequest(Builder builder) {
                DeleteStreamsRequest.Builder realBuilder = (DeleteStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(request);
                return realBuilder;
            }

            @Override
            public ApiKeys apiKey() {
                return ApiKeys.DELETE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new DeleteStreamsRequest.Builder(
                    new DeleteStreamsRequestData()
                        .setNodeId(nodeId)
                        .setNodeEpoch(nodeEpoch)).addSubRequest(request);
            }
        };
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<DeleteStreamResponse, Void> task = new RequestTask<>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
                case NODE_EPOCH_EXPIRED:
                case NODE_EPOCH_NOT_EXIST:
                    LOGGER.error("Node epoch expired or not exist: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case STREAM_NOT_EXIST:
                case STREAM_FENCED:
                case STREAM_INNER_ERROR:
                    LOGGER.error("Unexpected error while deleting stream: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                default:
                    LOGGER.warn("Error while deleting stream: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }
}
