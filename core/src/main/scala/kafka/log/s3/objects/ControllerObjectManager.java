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


import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import kafka.log.s3.StreamMetadataManager;
import kafka.log.s3.StreamMetadataManager.InflightWalObject;
import kafka.log.s3.network.ControllerRequestSender;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.message.CommitWALObjectRequestData;
import org.apache.kafka.common.message.CommitWALObjectResponseData;
import org.apache.kafka.common.message.PrepareS3ObjectRequestData;
import org.apache.kafka.common.message.PrepareS3ObjectResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest;
import org.apache.kafka.common.requests.s3.PrepareS3ObjectRequest.Builder;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectStreamIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return requestSender.send(request, PrepareS3ObjectResponseData.class).thenApply(resp -> {
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    // TODO: simply response's data structure, only return first object id is enough
                    return resp.s3ObjectIds().stream().findFirst().get();
                default:
                    LOGGER.error("Error while preparing {} object, code: {}", count, code);
                    throw code.exception();
            }
        });
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
                    .map(StreamObject::toStreamObjectInRequest).collect(Collectors.toList())));
        return requestSender.send(wrapRequestBuilder, CommitWALObjectResponseData.class).thenApply(resp -> {
            Errors code = Errors.forCode(resp.errorCode());
            switch (code) {
                case NONE:
                    return new CommitWALObjectResponse();
                default:
                    LOGGER.error("Error while committing WAL object: {}, code: {}", request, code);
                    throw code.exception();
            }
        }).thenApply(resp -> {
            long objectId = request.getObjectId();
            long orderId = request.getOrderId();
            int brokerId = config.brokerId();
            long objectSize = request.getObjectSize();
            Map<Long, List<S3ObjectStreamIndex>> rangeList = request.getStreamRanges().stream()
                .map(range -> new S3ObjectStreamIndex(range.getStreamId(), range.getStartOffset(), range.getEndOffset()))
                .collect(Collectors.groupingBy(S3ObjectStreamIndex::getStreamId));
            this.metadataManager.append(new InflightWalObject(objectId, brokerId, rangeList, orderId, objectSize));
            return resp;
        });
    }

    @Override
    public CompletableFuture<Void> commitMajorCompactObject(CommitCompactObjectRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitStreamObject(CommitStreamObjectRequest request) {
        return null;
    }

    @Override
    public List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        try {
            return this.metadataManager.getObjects(streamId, startOffset, endOffset, limit);
        } catch (Exception e) {
            LOGGER.error("Error while get objects, streamId: {}, startOffset: {}, endOffset: {}, limit: {}", streamId, startOffset, endOffset, limit,
                e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<S3ObjectMetadata> getServerObjects() {
        return null;
    }
}
