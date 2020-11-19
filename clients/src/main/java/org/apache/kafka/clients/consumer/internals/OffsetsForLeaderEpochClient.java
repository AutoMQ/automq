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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderPartitionResult;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Convenience class for making asynchronous requests to the OffsetsForLeaderEpoch API
 */
public class OffsetsForLeaderEpochClient extends AsyncClient<
        Map<TopicPartition, SubscriptionState.FetchPosition>,
        OffsetsForLeaderEpochRequest,
        OffsetsForLeaderEpochResponse,
        OffsetsForLeaderEpochClient.OffsetForEpochResult> {

    OffsetsForLeaderEpochClient(ConsumerNetworkClient client, LogContext logContext) {
        super(client, logContext);
    }

    @Override
    protected AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Node node, Map<TopicPartition, SubscriptionState.FetchPosition> requestData) {
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection(requestData.size());
        requestData.forEach((topicPartition, fetchPosition) ->
            fetchPosition.offsetEpoch.ifPresent(fetchEpoch -> {
                OffsetForLeaderTopic topic = topics.find(topicPartition.topic());
                if (topic == null) {
                    topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic());
                    topics.add(topic);
                }
                topic.partitions().add(new OffsetForLeaderPartition()
                    .setPartition(topicPartition.partition())
                    .setLeaderEpoch(fetchEpoch)
                    .setCurrentLeaderEpoch(fetchPosition.currentLeader.epoch
                        .orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                );
            })
        );
        return OffsetsForLeaderEpochRequest.Builder.forConsumer(topics);
    }

    @Override
    protected OffsetForEpochResult handleResponse(
            Node node,
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {

        Set<TopicPartition> partitionsToRetry = new HashSet<>(requestData.keySet());
        Set<String> unauthorizedTopics = new HashSet<>();
        Map<TopicPartition, OffsetForLeaderPartitionResult> endOffsets = new HashMap<>();

        for (OffsetForLeaderTopicResult topic : response.data().topics()) {
            for (OffsetForLeaderPartitionResult partition : topic.partitions()) {
                TopicPartition topicPartition = new TopicPartition(topic.topic(), partition.partition());

                if (!requestData.containsKey(topicPartition)) {
                    logger().warn("Received unrequested topic or partition {} from response, ignoring.", topicPartition);
                    continue;
                }

                Errors error = Errors.forCode(partition.errorCode());
                switch (error) {
                    case NONE:
                        logger().debug("Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}.",
                            topicPartition, partition.endOffset(), partition.leaderEpoch());
                        endOffsets.put(topicPartition, partition);
                        partitionsToRetry.remove(topicPartition);
                        break;
                    case NOT_LEADER_OR_FOLLOWER:
                    case REPLICA_NOT_AVAILABLE:
                    case KAFKA_STORAGE_ERROR:
                    case OFFSET_NOT_AVAILABLE:
                    case LEADER_NOT_AVAILABLE:
                    case FENCED_LEADER_EPOCH:
                    case UNKNOWN_LEADER_EPOCH:
                        logger().debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                            topicPartition, error);
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        logger().warn("Received unknown topic or partition error in OffsetsForLeaderEpoch request for partition {}.",
                            topicPartition);
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        unauthorizedTopics.add(topicPartition.topic());
                        partitionsToRetry.remove(topicPartition);
                        break;
                    default:
                        logger().warn("Attempt to fetch offsets for partition {} failed due to: {}, retrying.",
                            topicPartition, error.message());
                }
            }
        }

        if (!unauthorizedTopics.isEmpty())
            throw new TopicAuthorizationException(unauthorizedTopics);
        else
            return new OffsetForEpochResult(endOffsets, partitionsToRetry);
    }

    public static class OffsetForEpochResult {
        private final Map<TopicPartition, OffsetForLeaderPartitionResult> endOffsets;
        private final Set<TopicPartition> partitionsToRetry;

        OffsetForEpochResult(Map<TopicPartition, OffsetForLeaderPartitionResult> endOffsets, Set<TopicPartition> partitionsNeedingRetry) {
            this.endOffsets = endOffsets;
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        public Map<TopicPartition, OffsetForLeaderPartitionResult> endOffsets() {
            return endOffsets;
        }

        public Set<TopicPartition> partitionsToRetry() {
            return partitionsToRetry;
        }
    }
}
