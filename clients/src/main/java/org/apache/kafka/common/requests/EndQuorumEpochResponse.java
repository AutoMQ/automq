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
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes.
 *
 * Top level errors:
 * - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * - {@link Errors#BROKER_NOT_AVAILABLE}
 *
 * Partition level errors:
 * - {@link Errors#FENCED_LEADER_EPOCH}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#INCONSISTENT_VOTER_SET}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 */
public class EndQuorumEpochResponse extends AbstractResponse {
    public final EndQuorumEpochResponseData data;

    public EndQuorumEpochResponse(EndQuorumEpochResponseData data) {
        this.data = data;
    }

    public EndQuorumEpochResponse(Struct struct, short version) {
        this.data = new EndQuorumEpochResponseData(struct, version);
    }

    public EndQuorumEpochResponse(Struct struct) {
        short latestVersion = (short) (EndQuorumEpochResponseData.SCHEMAS.length - 1);
        this.data = new EndQuorumEpochResponseData(struct, latestVersion);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();

        errors.put(Errors.forCode(data.errorCode()), 1);

        for (EndQuorumEpochResponseData.TopicData topicResponse : data.topics()) {
            for (EndQuorumEpochResponseData.PartitionData partitionResponse : topicResponse.partitions()) {
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode()));
            }
        }
        return errors;
    }

    public static EndQuorumEpochResponseData singletonResponse(
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId
    ) {
        return new EndQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(Collections.singletonList(
                       new EndQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new EndQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );
    }

    public static EndQuorumEpochResponse parse(ByteBuffer buffer, short version) {
        return new EndQuorumEpochResponse(ApiKeys.END_QUORUM_EPOCH.responseSchema(version).read(buffer), version);
    }
}
