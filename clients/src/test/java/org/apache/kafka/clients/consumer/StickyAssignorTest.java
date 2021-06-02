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
package org.apache.kafka.clients.consumer;

import static org.apache.kafka.clients.consumer.StickyAssignor.serializeTopicPartitionAssignment;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.MemberData;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class StickyAssignorTest extends AbstractStickyAssignorTest {

    @Override
    public AbstractStickyAssignor createAssignor() {
        return new StickyAssignor();
    }

    @Override
    public Subscription buildSubscription(List<String> topics, List<TopicPartition> partitions) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(DEFAULT_GENERATION))));
    }

    @ParameterizedTest(name = "testAssignmentWithMultipleGenerations1 with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = {true, false})
    public void testAssignmentWithMultipleGenerations1(boolean isAllSubscriptionsEqual) {
        List<String> allTopics = topics(topic, topic2);
        List<String> consumer2SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 6);
        partitionsPerTopic.put(topic2, 6);
        subscriptions.put(consumer1, new Subscription(allTopics));
        subscriptions.put(consumer2, new Subscription(consumer2SubscribedTopics));
        subscriptions.put(consumer3, new Subscription(allTopics));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 4 && r1partitions2.size() == 4 && r1partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscription(allTopics, r1partitions1));
        subscriptions.put(consumer2, buildSubscription(consumer2SubscribedTopics, r1partitions2));
        subscriptions.remove(consumer3);

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions1 = assignment.get(consumer1);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertTrue(r2partitions1.size() == 6 && r2partitions2.size() == 6);
        if (isAllSubscriptionsEqual) {
            // only true in all subscription equal case
            assertTrue(r2partitions1.containsAll(r1partitions1));
        }
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
        assertFalse(Collections.disjoint(r2partitions2, r1partitions3));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(consumer2SubscribedTopics, r2partitions2, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(allTopics, r1partitions3, 1));

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions2.size() == 6 && r3partitions3.size() == 6);
        assertTrue(Collections.disjoint(r3partitions2, r3partitions3));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = "testAssignmentWithMultipleGenerations2 with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = {true, false})
    public void testAssignmentWithMultipleGenerations2(boolean isAllSubscriptionsEqual) {
        List<String> allTopics = topics(topic, topic2, topic3);
        List<String> consumer1SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);
        List<String> consumer3SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic, topic2);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 4);
        partitionsPerTopic.put(topic2, 4);
        partitionsPerTopic.put(topic3, 4);
        subscriptions.put(consumer1, new Subscription(consumer1SubscribedTopics));
        subscriptions.put(consumer2, new Subscription(allTopics));
        subscriptions.put(consumer3, new Subscription(consumer3SubscribedTopics));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r1partitions1 = assignment.get(consumer1);
        List<TopicPartition> r1partitions2 = assignment.get(consumer2);
        List<TopicPartition> r1partitions3 = assignment.get(consumer3);
        assertTrue(r1partitions1.size() == 4 && r1partitions2.size() == 4 && r1partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.remove(consumer1);
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(allTopics, r1partitions2, 1));
        subscriptions.remove(consumer3);

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r2partitions2 = assignment.get(consumer2);
        assertEquals(12, r2partitions2.size());
        assertTrue(r2partitions2.containsAll(r1partitions2));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));

        subscriptions.put(consumer1, buildSubscriptionWithGeneration(consumer1SubscribedTopics, r1partitions1, 1));
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(allTopics, r2partitions2, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(consumer3SubscribedTopics, r1partitions3, 1));

        assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> r3partitions1 = assignment.get(consumer1);
        List<TopicPartition> r3partitions2 = assignment.get(consumer2);
        List<TopicPartition> r3partitions3 = assignment.get(consumer3);
        assertTrue(r3partitions1.size() == 4 && r3partitions2.size() == 4 && r3partitions3.size() == 4);
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @ParameterizedTest(name = "testAssignmentWithConflictingPreviousGenerations with isAllSubscriptionsEqual: {0}")
    @ValueSource(booleans = {true, false})
    public void testAssignmentWithConflictingPreviousGenerations(boolean isAllSubscriptionsEqual) {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 4);
        partitionsPerTopic.put(topic2, 4);
        partitionsPerTopic.put(topic3, 4);

        List<String> allTopics = topics(topic, topic2, topic3);
        List<String> consumer1SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic);
        List<String> consumer2SubscribedTopics = isAllSubscriptionsEqual ? allTopics : topics(topic, topic2);

        subscriptions.put(consumer1, new Subscription(consumer1SubscribedTopics));
        subscriptions.put(consumer2, new Subscription(consumer2SubscribedTopics));
        subscriptions.put(consumer3, new Subscription(allTopics));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);
        TopicPartition tp3 = new TopicPartition(topic, 3);
        TopicPartition t2p0 = new TopicPartition(topic2, 0);
        TopicPartition t2p1 = new TopicPartition(topic2, 1);
        TopicPartition t2p2 = new TopicPartition(topic2, 2);
        TopicPartition t2p3 = new TopicPartition(topic2, 3);
        TopicPartition t3p0 = new TopicPartition(topic3, 0);
        TopicPartition t3p1 = new TopicPartition(topic3, 1);
        TopicPartition t3p2 = new TopicPartition(topic3, 2);
        TopicPartition t3p3 = new TopicPartition(topic3, 3);

        List<TopicPartition> c1partitions0 = isAllSubscriptionsEqual ? partitions(tp0, tp1, tp2, t2p2, t2p3, t3p0) :
            partitions(tp0, tp1, tp2, tp3);
        List<TopicPartition> c2partitions0 = partitions(tp0, tp1, t2p0, t2p1, t2p2, t2p3);
        List<TopicPartition> c3partitions0 = partitions(tp2, tp3, t3p0, t3p1, t3p2, t3p3);
        subscriptions.put(consumer1, buildSubscriptionWithGeneration(consumer1SubscribedTopics, c1partitions0, 1));
        subscriptions.put(consumer2, buildSubscriptionWithGeneration(consumer2SubscribedTopics, c2partitions0, 2));
        subscriptions.put(consumer3, buildSubscriptionWithGeneration(allTopics, c3partitions0, 2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 4 && c2partitions.size() == 4 && c3partitions.size() == 4);
        assertTrue(c2partitions0.containsAll(c2partitions));
        assertTrue(c3partitions0.containsAll(c3partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    @Test
    public void testSchemaBackwardCompatibility() {
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        subscriptions.put(consumer1, new Subscription(topics(topic)));
        subscriptions.put(consumer2, new Subscription(topics(topic)));
        subscriptions.put(consumer3, new Subscription(topics(topic)));

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        TopicPartition tp2 = new TopicPartition(topic, 2);

        List<TopicPartition> c1partitions0 = partitions(tp0, tp2);
        List<TopicPartition> c2partitions0 = partitions(tp1);
        subscriptions.put(consumer1, buildSubscriptionWithGeneration(topics(topic), c1partitions0, 1));
        subscriptions.put(consumer2, buildSubscriptionWithOldSchema(topics(topic), c2partitions0));
        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, subscriptions);
        List<TopicPartition> c1partitions = assignment.get(consumer1);
        List<TopicPartition> c2partitions = assignment.get(consumer2);
        List<TopicPartition> c3partitions = assignment.get(consumer3);

        assertTrue(c1partitions.size() == 1 && c2partitions.size() == 1 && c3partitions.size() == 1);
        assertTrue(c1partitions0.containsAll(c1partitions));
        assertTrue(c2partitions0.containsAll(c2partitions));
        verifyValidityAndBalance(subscriptions, assignment, partitionsPerTopic);
        assertTrue(isFullyBalanced(assignment));
    }

    private Subscription buildSubscriptionWithGeneration(List<String> topics, List<TopicPartition> partitions, int generation) {
        return new Subscription(topics,
            serializeTopicPartitionAssignment(new MemberData(partitions, Optional.of(generation))));
    }

    private static Subscription buildSubscriptionWithOldSchema(List<String> topics, List<TopicPartition> partitions) {
        Struct struct = new Struct(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0);
        List<Struct> topicAssignments = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> topicEntry : CollectionUtils.groupPartitionsByTopic(partitions).entrySet()) {
            Struct topicAssignment = new Struct(StickyAssignor.TOPIC_ASSIGNMENT);
            topicAssignment.set(StickyAssignor.TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(StickyAssignor.PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(StickyAssignor.TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
        ByteBuffer buffer = ByteBuffer.allocate(StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.sizeOf(struct));
        StickyAssignor.STICKY_ASSIGNOR_USER_DATA_V0.write(buffer, struct);
        buffer.flip();

        return new Subscription(topics, buffer);
    }
}
