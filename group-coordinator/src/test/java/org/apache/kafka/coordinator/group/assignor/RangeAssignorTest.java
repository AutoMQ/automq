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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.GroupSpecImpl;
import org.apache.kafka.coordinator.group.consumer.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.consumer.SubscribedTopicDescriberImpl;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangeAssignorTest {
    private final RangeAssignor assignor = new RangeAssignor();
    private final Uuid topic1Uuid = Uuid.randomUuid();
    private final String topic1Name = "topic1";
    private final Uuid topic2Uuid = Uuid.randomUuid();
    private final String topic2Name = "topic2";
    private final Uuid topic3Uuid = Uuid.randomUuid();
    private final String topic3Name = "topic3";
    private final String memberA = "A";
    private final String memberB = "B";
    private final String memberC = "C";

    @Test
    public void testOneConsumerNoTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3,
                    Collections.emptyMap()
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            memberA,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                Collections.emptySet(),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Collections.emptyMap()
        );

        GroupAssignment groupAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        assertEquals(Collections.emptyMap(), groupAssignment.members());
    }

    @Test
    public void testOneConsumerSubscribedToNonExistentTopic() {
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(
            Collections.singletonMap(
                topic1Uuid,
                new TopicMetadata(
                    topic1Uuid,
                    topic1Name,
                    3,
                    Collections.emptyMap()
                )
            )
        );

        Map<String, MemberSubscriptionAndAssignmentImpl> members = Collections.singletonMap(
            memberA,
            new MemberSubscriptionAndAssignmentImpl(
                Optional.empty(),
                mkSet(topic2Uuid),
                Assignment.EMPTY
            )
        );

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            Collections.emptyMap()
        );

        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(groupSpec, subscribedTopicMetadata));
    }

    @Test
    public void testFirstAssignmentTwoConsumersTwoTopicsSameSubscriptions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentThreeConsumersThreeTopicsDifferentSubscriptions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic2Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2),
            mkTopicAssignment(topic3Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testFirstAssignmentNumConsumersGreaterThanNumPartitions() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic3Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        // Topic 3 has 2 partitions but three consumers subscribed to it - one of them will not get a partition.
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic3Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic3Uuid, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentNumConsumersGreaterThanNumPartitionsWhenOneConsumerAdded() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            2,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            2,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 1),
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));

        // Consumer C shouldn't get any assignment, due to stickiness A, B retain their assignments
        assertNull(computedAssignment.members().get(memberC));
        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOnePartitionAddedForTwoConsumersTwoTopics() {
        // Simulating adding a partition - originally T1 -> 3 Partitions and T2 -> 3 Partitions
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            4,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            4,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2, 3),
            mkTopicAssignment(topic2Uuid, 2, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0),
            mkTopicAssignment(topic2Uuid, 0)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 1),
            mkTopicAssignment(topic2Uuid, 1)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerAddedAndOnePartitionAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        // Add a new partition to topic 1, initially T1 -> 3 partitions
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            4,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1),
                mkTopicAssignment(topic2Uuid, 0, 1)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        // Add a new consumer to trigger a re-assignment
        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid),
            Assignment.EMPTY
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1),
            mkTopicAssignment(topic2Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 2)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic1Uuid, 3)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenOneConsumerRemovedAfterInitialAssignmentWithTwoConsumersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            Collections.emptyMap()
        ));

        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        // Consumer A was removed

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 2),
                mkTopicAssignment(topic2Uuid, 2)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HOMOGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1, 2),
            mkTopicAssignment(topic2Uuid, 0, 1, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    @Test
    public void testReassignmentWhenMultipleSubscriptionsRemovedAfterInitialAssignmentWithThreeConsumersTwoTopics() {
        Map<Uuid, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put(topic1Uuid, new TopicMetadata(
            topic1Uuid,
            topic1Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic2Uuid, new TopicMetadata(
            topic2Uuid,
            topic2Name,
            3,
            Collections.emptyMap()
        ));
        topicMetadata.put(topic3Uuid, new TopicMetadata(
            topic3Uuid,
            topic3Name,
            2,
            Collections.emptyMap()
        ));

        // Let initial subscriptions be A -> T1, T2 // B -> T2 // C -> T2, T3
        // Change the subscriptions to A -> T1 // B -> T1, T2, T3 // C -> T2
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new TreeMap<>();

        members.put(memberA, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic1Uuid, 0, 1, 2),
                mkTopicAssignment(topic2Uuid, 0)
            ))
        ));

        members.put(memberB, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic1Uuid, topic2Uuid, topic3Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 1)
            ))
        ));

        members.put(memberC, new MemberSubscriptionAndAssignmentImpl(
            Optional.empty(),
            mkSet(topic2Uuid),
            new Assignment(mkAssignment(
                mkTopicAssignment(topic2Uuid, 2),
                mkTopicAssignment(topic3Uuid, 0, 1)
            ))
        ));

        GroupSpec groupSpec = new GroupSpecImpl(
            members,
            HETEROGENEOUS,
            invertedTargetAssignment(members)
        );
        SubscribedTopicDescriberImpl subscribedTopicMetadata = new SubscribedTopicDescriberImpl(topicMetadata);

        GroupAssignment computedAssignment = assignor.assign(
            groupSpec,
            subscribedTopicMetadata
        );

        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment = new HashMap<>();
        expectedAssignment.put(memberA, mkAssignment(
            mkTopicAssignment(topic1Uuid, 0, 1)
        ));
        expectedAssignment.put(memberB, mkAssignment(
            mkTopicAssignment(topic1Uuid, 2),
            mkTopicAssignment(topic2Uuid, 0, 1),
            mkTopicAssignment(topic3Uuid, 0, 1)
        ));
        expectedAssignment.put(memberC, mkAssignment(
            mkTopicAssignment(topic2Uuid, 2)
        ));

        assertAssignment(expectedAssignment, computedAssignment);
    }

    private void assertAssignment(
        Map<String, Map<Uuid, Set<Integer>>> expectedAssignment,
        GroupAssignment computedGroupAssignment
    ) {
        assertEquals(expectedAssignment.size(), computedGroupAssignment.members().size());
        for (String memberId : computedGroupAssignment.members().keySet()) {
            Map<Uuid, Set<Integer>> computedAssignmentForMember = computedGroupAssignment.members().get(memberId).partitions();
            assertEquals(expectedAssignment.get(memberId), computedAssignmentForMember);
        }
    }
}
