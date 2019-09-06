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
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TxnOffsetCommitRequest extends AbstractRequest {

    public final TxnOffsetCommitRequestData data;

    public static class Builder extends AbstractRequest.Builder<TxnOffsetCommitRequest> {

        public final TxnOffsetCommitRequestData data;

        public Builder(TxnOffsetCommitRequestData data) {
            super(ApiKeys.TXN_OFFSET_COMMIT);
            this.data = data;
        }

        @Override
        public TxnOffsetCommitRequest build(short version) {
            return new TxnOffsetCommitRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public TxnOffsetCommitRequest(TxnOffsetCommitRequestData data, short version) {
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.data = data;
    }

    public TxnOffsetCommitRequest(Struct struct, short version) {
        super(ApiKeys.TXN_OFFSET_COMMIT, version);
        this.data = new TxnOffsetCommitRequestData(struct, version);
    }

    public Map<TopicPartition, CommittedOffset> offsets() {
        List<TxnOffsetCommitRequestTopic> topics = data.topics();
        Map<TopicPartition, CommittedOffset> offsetMap = new HashMap<>();
        for (TxnOffsetCommitRequestTopic topic : topics) {
            for (TxnOffsetCommitRequestPartition partition : topic.partitions()) {
                offsetMap.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                              new CommittedOffset(partition.committedOffset(),
                                                  partition.committedMetadata(),
                                                  RequestUtils.getLeaderEpoch(partition.committedLeaderEpoch()))
                );
            }
        }
        return offsetMap;
    }

    public static List<TxnOffsetCommitRequestTopic> getTopics(Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits) {
        Map<String, List<TxnOffsetCommitRequestPartition>> topicPartitionMap = new HashMap<>();
        for (Map.Entry<TopicPartition, CommittedOffset> entry : pendingTxnOffsetCommits.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            CommittedOffset offset = entry.getValue();

            List<TxnOffsetCommitRequestPartition> partitions =
                topicPartitionMap.getOrDefault(topicPartition.topic(), new ArrayList<>());
            partitions.add(new TxnOffsetCommitRequestPartition()
                               .setPartitionIndex(topicPartition.partition())
                               .setCommittedOffset(offset.offset)
                               .setCommittedLeaderEpoch(offset.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                               .setCommittedMetadata(offset.metadata)
            );
            topicPartitionMap.put(topicPartition.topic(), partitions);
        }
        return topicPartitionMap.entrySet().stream()
                   .map(entry -> new TxnOffsetCommitRequestTopic()
                                     .setName(entry.getKey())
                                     .setPartitions(entry.getValue()))
                   .collect(Collectors.toList());
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    static List<TxnOffsetCommitResponseTopic> getErrorResponseTopics(List<TxnOffsetCommitRequestTopic> requestTopics,
                                                                     Errors e) {
        List<TxnOffsetCommitResponseTopic> responseTopicData = new ArrayList<>();
        for (TxnOffsetCommitRequestTopic entry : requestTopics) {
            List<TxnOffsetCommitResponsePartition> responsePartitions = new ArrayList<>();
            for (TxnOffsetCommitRequestPartition requestPartition : entry.partitions()) {
                responsePartitions.add(new TxnOffsetCommitResponsePartition()
                                           .setPartitionIndex(requestPartition.partitionIndex())
                                           .setErrorCode(e.code()));
            }
            responseTopicData.add(new TxnOffsetCommitResponseTopic()
                                      .setName(entry.name())
                                      .setPartitions(responsePartitions)
            );
        }
        return responseTopicData;
    }

    @Override
    public TxnOffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<TxnOffsetCommitResponseTopic> responseTopicData =
            getErrorResponseTopics(data.topics(), Errors.forException(e));

        return new TxnOffsetCommitResponse(new TxnOffsetCommitResponseData()
                                               .setThrottleTimeMs(throttleTimeMs)
                                               .setTopics(responseTopicData));
    }

    public static TxnOffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitRequest(ApiKeys.TXN_OFFSET_COMMIT.parseRequest(version, buffer), version);
    }

    public static class CommittedOffset {
        public final long offset;
        public final String metadata;
        public final Optional<Integer> leaderEpoch;

        public CommittedOffset(long offset, String metadata, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.metadata = metadata;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public String toString() {
            return "CommittedOffset(" +
                    "offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ", metadata='" + metadata + "')";
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CommittedOffset)) {
                return false;
            }
            CommittedOffset otherOffset = (CommittedOffset) other;

            return this.offset == otherOffset.offset
                       && this.leaderEpoch.equals(otherOffset.leaderEpoch)
                       && Objects.equals(this.metadata, otherOffset.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, leaderEpoch, metadata);
        }
    }
}
