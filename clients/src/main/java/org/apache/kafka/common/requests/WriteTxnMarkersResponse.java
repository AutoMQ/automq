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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerResult;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Possible error codes:
 *
 *   - {@link Errors#CORRUPT_MESSAGE}
 *   - {@link Errors#INVALID_PRODUCER_EPOCH}
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *   - {@link Errors#NOT_LEADER_FOR_PARTITION}
 *   - {@link Errors#MESSAGE_TOO_LARGE}
 *   - {@link Errors#RECORD_LIST_TOO_LARGE}
 *   - {@link Errors#NOT_ENOUGH_REPLICAS}
 *   - {@link Errors#NOT_ENOUGH_REPLICAS_AFTER_APPEND}
 *   - {@link Errors#INVALID_REQUIRED_ACKS}
 *   - {@link Errors#TRANSACTION_COORDINATOR_FENCED}
 *   - {@link Errors#REQUEST_TIMED_OUT}
 *   - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 */
public class WriteTxnMarkersResponse extends AbstractResponse {

    private final Map<Long, Map<TopicPartition, Errors>> errors;
    public final WriteTxnMarkersResponseData data;

    public WriteTxnMarkersResponse(Map<Long, Map<TopicPartition, Errors>> errors) {
        List<WritableTxnMarkerResult> markers = new ArrayList<>();
        for (Map.Entry<Long, Map<TopicPartition, Errors>> markerEntry : errors.entrySet()) {
            Map<String, WritableTxnMarkerTopicResult> responseTopicDataMap = new HashMap<>();
            for (Map.Entry<TopicPartition, Errors> topicEntry : markerEntry.getValue().entrySet()) {
                TopicPartition topicPartition = topicEntry.getKey();
                String topicName = topicPartition.topic();

                WritableTxnMarkerTopicResult topic =
                    responseTopicDataMap.getOrDefault(topicName, new WritableTxnMarkerTopicResult().setName(topicName));
                topic.partitions().add(new WritableTxnMarkerPartitionResult()
                                           .setErrorCode(topicEntry.getValue().code())
                                           .setPartitionIndex(topicPartition.partition())
                );
                responseTopicDataMap.put(topicName, topic);
            }

            markers.add(new WritableTxnMarkerResult()
                            .setProducerId(markerEntry.getKey())
                            .setTopics(new ArrayList<>(responseTopicDataMap.values()))
            );
        }

        this.errors = errors;
        this.data = new WriteTxnMarkersResponseData()
                        .setMarkers(markers);
    }

    public WriteTxnMarkersResponse(Struct struct, short version) {
        this.data = new WriteTxnMarkersResponseData(struct, version);
        this.errors = new HashMap<>();
        for (WritableTxnMarkerResult marker : data.markers()) {
            Map<TopicPartition, Errors> topicPartitionErrorsMap = new HashMap<>();
            for (WritableTxnMarkerTopicResult topic: marker.topics()) {
                for (WritableTxnMarkerPartitionResult partitionResult: topic.partitions()) {
                    topicPartitionErrorsMap.put(new TopicPartition(topic.name(), partitionResult.partitionIndex()),
                                                Errors.forCode(partitionResult.errorCode()));
                }
            }
            errors.put(marker.producerId(), topicPartitionErrorsMap);
        }
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public Map<TopicPartition, Errors> errors(long producerId) {
        return errors.get(producerId);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        errors.values().forEach(allErrors ->
            allErrors.values().forEach(error ->
                updateErrorCounts(errorCounts, error)
            )
        );
        return errorCounts;
    }

    public static WriteTxnMarkersResponse parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkersResponse(ApiKeys.WRITE_TXN_MARKERS.parseResponse(version, buffer), version);
    }
}
