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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.kafka.coordinator.group.TestUtil.requestContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicatedGroupCoordinatorTest {

    @Test
    public void testConsumerGroupHeartbeat() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        RequestContext context = requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData();
        CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> result = new CoordinatorResult<>(
            Collections.emptyList(),
            new ConsumerGroupHeartbeatResponseData()
        );

        when(coordinator.consumerGroupHeartbeat(
            context,
            request
        )).thenReturn(result);

        assertEquals(result, coordinator.consumerGroupHeartbeat(context, request));
    }

    @Test
    public void testReplayConsumerGroupMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupMetadataKey key = new ConsumerGroupMetadataKey();
        ConsumerGroupMetadataValue value = new ConsumerGroupMetadataValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 3),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupMetadataKey key = new ConsumerGroupMetadataKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 3),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupPartitionMetadataKey key = new ConsumerGroupPartitionMetadataKey();
        ConsumerGroupPartitionMetadataValue value = new ConsumerGroupPartitionMetadataValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 4),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupPartitionMetadataKey key = new ConsumerGroupPartitionMetadataKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 4),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupMemberMetadataKey key = new ConsumerGroupMemberMetadataKey();
        ConsumerGroupMemberMetadataValue value = new ConsumerGroupMemberMetadataValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 5),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupMemberMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupMemberMetadataKey key = new ConsumerGroupMemberMetadataKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 5),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadata() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupTargetAssignmentMetadataKey key = new ConsumerGroupTargetAssignmentMetadataKey();
        ConsumerGroupTargetAssignmentMetadataValue value = new ConsumerGroupTargetAssignmentMetadataValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 6),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupTargetAssignmentMetadataKey key = new ConsumerGroupTargetAssignmentMetadataKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 6),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMember() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupTargetAssignmentMemberKey key = new ConsumerGroupTargetAssignmentMemberKey();
        ConsumerGroupTargetAssignmentMemberValue value = new ConsumerGroupTargetAssignmentMemberValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 7),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMemberKeyWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupTargetAssignmentMemberKey key = new ConsumerGroupTargetAssignmentMemberKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 7),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignment() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupCurrentMemberAssignmentKey key = new ConsumerGroupCurrentMemberAssignmentKey();
        ConsumerGroupCurrentMemberAssignmentValue value = new ConsumerGroupCurrentMemberAssignmentValue();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 8),
            new ApiMessageAndVersion(value, (short) 0)
        ));

        verify(groupMetadataManager, times(1)).replay(key, value);
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentWithNullValue() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupCurrentMemberAssignmentKey key = new ConsumerGroupCurrentMemberAssignmentKey();

        coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 8),
            null
        ));

        verify(groupMetadataManager, times(1)).replay(key, null);
    }

    @Test
    public void testReplayKeyCannotBeNull() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        assertThrows(NullPointerException.class, () -> coordinator.replay(new Record(null, null)));
    }

    @Test
    public void testReplayWithUnsupportedVersion() {
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        ConsumerGroupCurrentMemberAssignmentKey key = new ConsumerGroupCurrentMemberAssignmentKey();
        ConsumerGroupCurrentMemberAssignmentValue value = new ConsumerGroupCurrentMemberAssignmentValue();

        assertThrows(IllegalStateException.class, () -> coordinator.replay(new Record(
            new ApiMessageAndVersion(key, (short) 255),
            new ApiMessageAndVersion(value, (short) 0)
        )));
    }

    @Test
    public void testOnLoaded() {
        MetadataImage image = MetadataImage.EMPTY;
        GroupMetadataManager groupMetadataManager = mock(GroupMetadataManager.class);
        ReplicatedGroupCoordinator coordinator = new ReplicatedGroupCoordinator(
            groupMetadataManager
        );

        coordinator.onLoaded(image);

        verify(groupMetadataManager, times(1)).onNewMetadataImage(
            eq(image),
            any()
        );

        verify(groupMetadataManager, times(1)).onLoaded();
    }
}
