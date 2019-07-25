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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.OWNED_PARTITIONS_KEY_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.TOPICS_KEY_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.TOPIC_ASSIGNMENT_V0;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.TOPIC_PARTITIONS_KEY_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.USER_DATA_KEY_NAME;
import static org.apache.kafka.clients.consumer.internals.ConsumerProtocol.VERSION_KEY_NAME;
import static org.apache.kafka.test.TestUtils.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsumerProtocolTest {

    private final TopicPartition tp1 = new TopicPartition("foo", 1);
    private final TopicPartition tp2 = new TopicPartition("bar", 2);
    private final Optional<String> groupInstanceId = Optional.of("instance.id");

    @Test
    public void serializeDeserializeMetadata() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), ByteBuffer.wrap(new byte[0]));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(subscription.topics(), parsedSubscription.topics());
        assertEquals(0, parsedSubscription.userData().limit());
        assertFalse(parsedSubscription.groupInstanceId().isPresent());
    }

    @Test
    public void serializeDeserializeMetadataAndGroupInstanceId() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), ByteBuffer.wrap(new byte[0]));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);

        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        parsedSubscription.setGroupInstanceId(groupInstanceId);
        assertEquals(subscription.topics(), parsedSubscription.topics());
        assertEquals(0, parsedSubscription.userData().limit());
        assertEquals(groupInstanceId, parsedSubscription.groupInstanceId());
    }

    @Test
    public void serializeDeserializeNullSubscriptionUserData() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null);
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(subscription.topics(), parsedSubscription.topics());
        assertNull(parsedSubscription.userData());
    }

    @Test
    public void deserializeOldSubscriptionVersion() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null);
        ByteBuffer buffer = ConsumerProtocol.serializeSubscriptionV0(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(parsedSubscription.topics(), parsedSubscription.topics());
        assertNull(parsedSubscription.userData());
        assertTrue(parsedSubscription.ownedPartitions().isEmpty());
    }

    @Test
    public void deserializeNewSubscriptionWithOldVersion() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null, Collections.singletonList(tp2));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        // ignore the version assuming it is the old byte code, as it will blindly deserialize as V0
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        header.getShort(VERSION_KEY_NAME);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscriptionV0(buffer);
        assertEquals(subscription.topics(), parsedSubscription.topics());
        assertNull(parsedSubscription.userData());
        assertTrue(parsedSubscription.ownedPartitions().isEmpty());
        assertFalse(parsedSubscription.groupInstanceId().isPresent());
    }

    @Test
    public void deserializeFutureSubscriptionVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema subscriptionSchemaV100 = new Schema(
                new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
                new Field(USER_DATA_KEY_NAME, Type.BYTES),
                new Field(OWNED_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
                new Field("foo", Type.STRING));

        Struct subscriptionV100 = new Struct(subscriptionSchemaV100);
        subscriptionV100.set(TOPICS_KEY_NAME, new Object[]{"topic"});
        subscriptionV100.set(USER_DATA_KEY_NAME, ByteBuffer.wrap(new byte[0]));
        subscriptionV100.set(OWNED_PARTITIONS_KEY_NAME, new Object[]{new Struct(TOPIC_ASSIGNMENT_V0)
            .set(ConsumerProtocol.TOPIC_KEY_NAME, tp2.topic())
            .set(ConsumerProtocol.PARTITIONS_KEY_NAME, new Object[]{tp2.partition()})});
        subscriptionV100.set("foo", "bar");

        Struct headerV100 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        subscriptionV100.writeTo(buffer);

        buffer.flip();

        Subscription subscription = ConsumerProtocol.deserializeSubscription(buffer);
        subscription.setGroupInstanceId(groupInstanceId);
        assertEquals(Collections.singletonList("topic"), subscription.topics());
        assertEquals(Collections.singletonList(tp2), subscription.ownedPartitions());
        assertEquals(groupInstanceId, subscription.groupInstanceId());
    }

    @Test
    public void serializeDeserializeAssignment() {
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new Assignment(partitions, ByteBuffer.wrap(new byte[0])));
        Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
        assertEquals(0, parsedAssignment.userData().limit());
    }

    @Test
    public void deserializeNullAssignmentUserData() {
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new Assignment(partitions, null));
        Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
        assertNull(parsedAssignment.userData());
    }

    @Test
    public void deserializeFutureAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema assignmentSchemaV100 = new Schema(
                new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
                new Field(USER_DATA_KEY_NAME, Type.BYTES),
                new Field("foo", Type.STRING));

        Struct assignmentV100 = new Struct(assignmentSchemaV100);
        assignmentV100.set(TOPIC_PARTITIONS_KEY_NAME,
                new Object[]{new Struct(TOPIC_ASSIGNMENT_V0)
                        .set(ConsumerProtocol.TOPIC_KEY_NAME, tp1.topic())
                        .set(ConsumerProtocol.PARTITIONS_KEY_NAME, new Object[]{tp1.partition()})});
        assignmentV100.set(USER_DATA_KEY_NAME, ByteBuffer.wrap(new byte[0]));
        assignmentV100.set("foo", "bar");

        Struct headerV100 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        assignmentV100.writeTo(buffer);

        buffer.flip();

        Assignment assignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(Collections.singletonList(tp1)), toSet(assignment.partitions()));
    }
}
