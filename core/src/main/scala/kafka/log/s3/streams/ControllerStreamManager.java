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

package kafka.log.s3.streams;

import kafka.log.s3.network.ControllerRequestSender;
import kafka.log.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.message.CloseStreamRequestData;
import org.apache.kafka.common.message.CloseStreamResponseData;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.message.OpenStreamRequestData;
import org.apache.kafka.common.message.OpenStreamResponseData;
import org.apache.kafka.common.message.TrimStreamRequestData;
import org.apache.kafka.common.message.TrimStreamResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.CloseStreamRequest;
import org.apache.kafka.common.requests.s3.CreateStreamRequest;
import org.apache.kafka.common.requests.s3.GetOpeningStreamsRequest;
import org.apache.kafka.common.requests.s3.OpenStreamRequest;
import org.apache.kafka.metadata.stream.StreamMetadata;
import org.apache.kafka.metadata.stream.StreamState;
import org.apache.kafka.common.requests.s3.TrimStreamRequest;
import org.apache.kafka.common.requests.s3.TrimStreamRequest.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ControllerStreamManager implements StreamManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStreamManager.class);
    private final KafkaConfig config;
    private final ControllerRequestSender requestSender;

    public ControllerStreamManager(ControllerRequestSender requestSender, KafkaConfig config) {
        this.config = config;
        this.requestSender = requestSender;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        GetOpeningStreamsRequest.Builder request = new GetOpeningStreamsRequest.Builder(
                new GetOpeningStreamsRequestData()
                    .setBrokerId(config.brokerId())
                    .setBrokerEpoch(config.brokerEpoch()));
        CompletableFuture<List<StreamMetadata>> future = new CompletableFuture<>();
        RequestTask<GetOpeningStreamsResponseData, List<StreamMetadata>> task = new RequestTask<>(future, request, GetOpeningStreamsResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamMetadataList().stream()
                            .map(m -> new StreamMetadata(m.streamId(), m.epoch(), m.startOffset(), m.endOffset(), StreamState.OPENED))
                            .collect(Collectors.toList()));
                default:
                    LOGGER.error("Error while getting streams offset: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Long> createStream() {
        CreateStreamRequest.Builder request = new CreateStreamRequest.Builder(
                new CreateStreamRequestData()
        );
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponseData, Long> task = new RequestTask<>(future, request, CreateStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                default:
                    LOGGER.error("Error while creating stream: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        OpenStreamRequest.Builder request = new OpenStreamRequest.Builder(
                new OpenStreamRequestData()
                        .setStreamId(streamId)
                        .setStreamEpoch(epoch)
                        .setBrokerId(config.brokerId())
        );
        CompletableFuture<StreamMetadata> future = new CompletableFuture<>();
        RequestTask<OpenStreamResponseData, StreamMetadata> task = new RequestTask<>(future, request, OpenStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(new StreamMetadata(streamId, epoch, resp.startOffset(), resp.nextOffset(), StreamState.OPENED));
                case STREAM_NOT_EXIST:
                case STREAM_FENCED:
                case STREAM_INNER_ERROR:
                    LOGGER.error("Unexpected error while opening stream: {}, code: {}", request, Errors.forCode(resp.errorCode()));
                    throw Errors.forCode(resp.errorCode()).exception();
                case STREAM_NOT_CLOSED:
                default:
                    LOGGER.error("Error while opening stream: {}, code: {}, retry later", request, Errors.forCode(resp.errorCode()));
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        return future;
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        TrimStreamRequest.Builder request = new Builder(
            new TrimStreamRequestData()
                .setStreamId(streamId)
                .setStreamEpoch(epoch)
                .setBrokerId(config.brokerId())
                .setNewStartOffset(newStartOffset)
        );
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<TrimStreamResponseData, Void> task = new RequestTask<>(future, request, TrimStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
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
        CloseStreamRequest.Builder request = new CloseStreamRequest.Builder(
                new CloseStreamRequestData()
                        .setStreamId(streamId)
                        .setStreamEpoch(epoch)
                        .setBrokerId(config.brokerId())
        );
        CompletableFuture<Void> future = new CompletableFuture<>();
        RequestTask<CloseStreamResponseData, Void> task = new RequestTask<>(future, request, CloseStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(null);
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
        // TODO: implement
        return CompletableFuture.completedFuture(null);
    }
}
