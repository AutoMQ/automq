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

package org.apache.kafka.clients.admin;

import com.automq.events.FailoverEvent;
import com.automq.events.OffsetCommitFrequencyEvent;
import com.automq.events.RebalancePartitionEvent;
import com.automq.events.RebalanceSummaryEvent;
import com.automq.events.RequestErrorEvent;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterEventTypeRegistryTest {

    @Test
    void decodeRebalanceSummaryEvent() {
        RebalanceSummaryEvent payload = RebalanceSummaryEvent.newBuilder()
            .setRebalanceId("r1")
            .setTriggerReason("load imbalance")
            .setPartitionCount(3)
            .build();

        CloudEvent event = buildEvent(RebalanceSummaryEventData.DATA_SCHEMA, payload.toByteArray());
        Optional<Object> result = ClusterEventTypeRegistry.decode(event);

        assertTrue(result.isPresent());
        RebalanceSummaryEventData decoded = assertInstanceOf(RebalanceSummaryEventData.class, result.get());
        assertEquals("r1", decoded.rebalanceId());
        assertEquals("load imbalance", decoded.triggerReason());
        assertEquals(3, decoded.partitionCount());
    }

    @Test
    void decodeRebalancePartitionEvent() {
        RebalancePartitionEvent payload = RebalancePartitionEvent.newBuilder()
            .setRebalanceId("r1")
            .setTopicPartition("my-topic-0")
            .setFromBroker(1)
            .setToBroker(2)
            .build();

        CloudEvent event = buildEvent(RebalancePartitionEventData.DATA_SCHEMA, payload.toByteArray());
        Optional<Object> result = ClusterEventTypeRegistry.decode(event);

        assertTrue(result.isPresent());
        RebalancePartitionEventData decoded = assertInstanceOf(RebalancePartitionEventData.class, result.get());
        assertEquals("my-topic-0", decoded.topicPartition());
        assertEquals(1, decoded.fromBroker());
        assertEquals(2, decoded.toBroker());
    }

    @Test
    void decodeFailoverEvent() {
        FailoverEvent payload = FailoverEvent.newBuilder()
            .setFailedNodeId(5)
            .setDetectedTimestamp(1000L)
            .setCompletedTimestamp(2000L)
            .setDetail("node unreachable")
            .build();

        CloudEvent event = buildEvent(FailoverEventData.DATA_SCHEMA, payload.toByteArray());
        Optional<Object> result = ClusterEventTypeRegistry.decode(event);

        assertTrue(result.isPresent());
        FailoverEventData decoded = assertInstanceOf(FailoverEventData.class, result.get());
        assertEquals(5, decoded.failedNodeId());
        assertEquals("node unreachable", decoded.detail());
    }

    @Test
    void decodeRequestErrorEvent() {
        RequestErrorEvent payload = RequestErrorEvent.newBuilder()
            .setApiKey(0)
            .setErrorCode(29)
            .setResource("my-topic")
            .setRps(42.0)
            .build();

        CloudEvent event = buildEvent(RequestErrorEventData.DATA_SCHEMA, payload.toByteArray());
        Optional<Object> result = ClusterEventTypeRegistry.decode(event);

        assertTrue(result.isPresent());
        RequestErrorEventData decoded = assertInstanceOf(RequestErrorEventData.class, result.get());
        assertEquals(0, decoded.apiKey());
        assertEquals(29, decoded.errorCode());
        assertEquals(42.0, decoded.rps(), 0.001);
    }

    @Test
    void decodeOffsetCommitFrequencyEvent() {
        OffsetCommitFrequencyEvent payload = OffsetCommitFrequencyEvent.newBuilder()
            .setGroupId("my-group")
            .setTopic("my-topic")
            .setRps(5000.0)
            .build();

        CloudEvent event = buildEvent(OffsetCommitFrequencyEventData.DATA_SCHEMA, payload.toByteArray());
        Optional<Object> result = ClusterEventTypeRegistry.decode(event);

        assertTrue(result.isPresent());
        OffsetCommitFrequencyEventData decoded = assertInstanceOf(OffsetCommitFrequencyEventData.class, result.get());
        assertEquals("my-group", decoded.groupId());
        assertEquals(5000.0, decoded.rps(), 0.001);
    }

    @Test
    void returnsEmptyForUnknownSchema() {
        CloudEvent event = buildEvent("com.example.UnknownEvent", new byte[]{1, 2, 3});
        assertFalse(ClusterEventTypeRegistry.decode(event).isPresent());
    }

    @Test
    void returnsEmptyForNullDataSchema() {
        CloudEvent event = CloudEventBuilder.v1()
            .withId("test-id")
            .withType("com.automq.ops.failover")
            .withSource(URI.create("/automq/broker/0"))
            .withData("application/protobuf", new byte[]{1})
            .build();
        assertFalse(ClusterEventTypeRegistry.decode(event).isPresent());
    }

    @Test
    void returnsEmptyForCorruptPayload() {
        CloudEvent event = buildEvent(FailoverEventData.DATA_SCHEMA, new byte[]{(byte) 0xFF, (byte) 0xFF});
        assertFalse(ClusterEventTypeRegistry.decode(event).isPresent());
    }

    private static CloudEvent buildEvent(String dataSchema, byte[] payload) {
        return CloudEventBuilder.v1()
            .withId("test-id")
            .withType("com.automq.test")
            .withSource(URI.create("/automq/broker/0"))
            .withDataSchema(URI.create(dataSchema))
            .withData("application/protobuf", payload)
            .build();
    }
}
