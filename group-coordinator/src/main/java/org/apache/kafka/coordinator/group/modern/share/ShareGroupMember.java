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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroupMember;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * ShareGroupMember contains all the information related to a member
 * within a share group. This class is immutable.
 */
public class ShareGroupMember extends ModernGroupMember {

    /**
     * A builder that facilitates the creation of a new member or the update of
     * an existing one.
     * <p>
     * Please refer to the javadoc of {{@link ShareGroupMember}} for the
     * definition of the fields.
     */
    public static class Builder {
        private final String memberId;
        private int memberEpoch = 0;
        private int previousMemberEpoch = -1;
        private MemberState state = MemberState.STABLE;
        private String rackId = null;
        private String clientId = "";
        private String clientHost = "";
        private Set<String> subscribedTopicNames = Collections.emptySet();
        private Map<Uuid, Set<Integer>> assignedPartitions = Collections.emptyMap();

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Builder(ShareGroupMember member) {
            this(
                Objects.requireNonNull(member),
                member.memberId
            );
        }

        public Builder(ShareGroupMember member, String newMemberId) {
            Objects.requireNonNull(member);

            this.memberId = Objects.requireNonNull(newMemberId);
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.rackId = member.rackId;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.subscribedTopicNames = member.subscribedTopicNames;
            this.assignedPartitions = member.assignedPartitions;
        }

        public Builder setMemberEpoch(int memberEpoch) {
            this.memberEpoch = memberEpoch;
            return this;
        }

        public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
            this.previousMemberEpoch = previousMemberEpoch;
            return this;
        }

        public Builder setRackId(String rackId) {
            this.rackId = rackId;
            return this;
        }

        public Builder maybeUpdateRackId(Optional<String> rackId) {
            this.rackId = rackId.orElse(this.rackId);
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setClientHost(String clientHost) {
            this.clientHost = clientHost;
            return this;
        }

        public Builder setSubscribedTopicNames(List<String> subscribedTopicList) {
            if (subscribedTopicNames != null) this.subscribedTopicNames = new HashSet<>(subscribedTopicList);
            return this;
        }

        public Builder maybeUpdateSubscribedTopicNames(Optional<List<String>> subscribedTopicList) {
            subscribedTopicList.ifPresent(list -> this.subscribedTopicNames = new HashSet<>(list));
            return this;
        }

        public Builder setState(MemberState state) {
            this.state = state;
            return this;
        }

        public Builder setAssignedPartitions(Map<Uuid, Set<Integer>> assignedPartitions) {
            this.assignedPartitions = assignedPartitions;
            return this;
        }

        public Builder updateWith(ShareGroupMemberMetadataValue record) {
            setRackId(record.rackId());
            setClientId(record.clientId());
            setClientHost(record.clientHost());
            setSubscribedTopicNames(record.subscribedTopicNames());
            return this;
        }

        public ShareGroupMember build() {
            return new ShareGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                rackId,
                clientId,
                clientHost,
                subscribedTopicNames,
                state,
                assignedPartitions
            );
        }
    }

    private ShareGroupMember(
          String memberId,
          int memberEpoch,
          int previousMemberEpoch,
          String rackId,
          String clientId,
          String clientHost,
          Set<String> subscribedTopicNames,
          MemberState state,
          Map<Uuid, Set<Integer>> assignedPartitions
    ) {
        super(
            memberId,
            memberEpoch,
            previousMemberEpoch,
            null,
            rackId,
            clientId,
            clientHost,
            subscribedTopicNames,
            state,
            assignedPartitions
        );
    }

    /**
     * Converts this ShareGroupMember to a ShareGroupDescribeResponseData.Member.
     *
     * @param topicsImage: Topics image object to search for a specific topic id
     *
     * @return The ShareGroupMember mapped as ShareGroupDescribeResponseData.Member.
     */
    public ShareGroupDescribeResponseData.Member asShareGroupDescribeMember(
        TopicsImage topicsImage
    ) {
        return new ShareGroupDescribeResponseData.Member()
            .setMemberEpoch(memberEpoch)
            .setMemberId(memberId)
            .setAssignment(new ShareGroupDescribeResponseData.Assignment()
                .setTopicPartitions(topicPartitionsFromMap(assignedPartitions, topicsImage)))
            .setClientHost(clientHost)
            .setClientId(clientId)
            .setRackId(rackId)
            .setSubscribedTopicNames(subscribedTopicNames == null ? null : new ArrayList<>(subscribedTopicNames));
    }

    private static List<ShareGroupDescribeResponseData.TopicPartitions> topicPartitionsFromMap(
        Map<Uuid, Set<Integer>> partitions,
        TopicsImage topicsImage
    ) {
        List<ShareGroupDescribeResponseData.TopicPartitions> topicPartitions = new ArrayList<>();
        partitions.forEach((topicId, partitionSet) -> {
            TopicImage topicImage = topicsImage.getTopic(topicId);
            if (topicImage != null) {
                topicPartitions.add(new ShareGroupDescribeResponseData.TopicPartitions()
                    .setTopicId(topicId)
                    .setTopicName(topicImage.name())
                    .setPartitions(new ArrayList<>(partitionSet)));
            }
        });
        return topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareGroupMember that = (ShareGroupMember) o;
        return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && state == that.state
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(subscribedTopicNames, that.subscribedTopicNames)
            && Objects.equals(assignedPartitions, that.assignedPartitions);
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + memberEpoch;
        result = 31 * result + previousMemberEpoch;
        result = 31 * result + Objects.hashCode(state);
        result = 31 * result + Objects.hashCode(rackId);
        result = 31 * result + Objects.hashCode(clientId);
        result = 31 * result + Objects.hashCode(clientHost);
        result = 31 * result + Objects.hashCode(subscribedTopicNames);
        result = 31 * result + Objects.hashCode(assignedPartitions);
        return result;
    }

    @Override
    public String toString() {
        return "ShareGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch + '\'' +
            ", previousMemberEpoch=" + previousMemberEpoch + '\'' +
            ", state='" + state + '\'' +
            ", rackId='" + rackId + '\'' +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", subscribedTopicNames=" + subscribedTopicNames + '\'' +
            ", assignedPartitions=" + assignedPartitions +
            ')';
    }
}
