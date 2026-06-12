/*
 * Copyright 2026, AutoMQ HK Limited.
 *
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

package kafka.automq.availability;

import kafka.automq.availability.action.AvailabilityActionExecutor;
import kafka.automq.availability.action.NodeExitActionAdapter;
import kafka.automq.availability.action.SegmentRollActionAdapter;
import kafka.automq.availability.action.SkipReadRangeActionAdapter;
import kafka.automq.availability.action.SkippedReadRangeRegistry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.RecoveryAction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class AvailabilityActionExecutorTest {
    private final MockTime time = new MockTime(0L, 100L, 0L);

    @Test
    public void testDryRunCreatesDryRunResponseWithoutAdapter() {
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(), time);

        ActionResponse response = executor.execute(action(AvailabilityActionType.PARTITION_RECREATE, true, 200L));

        assertEquals(ActionExecutionStatus.DRY_RUN, response.getStatus());
    }

    @Test
    public void testExecutableActionDispatchesAdapterAndDeduplicatesByUuid() {
        AtomicInteger executions = new AtomicInteger();
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(AvailabilityActionType.NODE_EXIT,
            (action, nowMs) -> {
                executions.incrementAndGet();
                return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
                    ActionExecutionStatus.SUCCEEDED, nowMs, nowMs + 1, null);
            }), time);
        RecoveryAction action = action(AvailabilityActionType.NODE_EXIT, false, 200L);

        ActionResponse first = executor.execute(action);
        ActionResponse second = executor.execute(action);

        assertEquals(ActionExecutionStatus.SUCCEEDED, first.getStatus());
        assertEquals(first, second);
        assertEquals(1, executions.get());
    }

    @Test
    public void testResponseCacheExpiresAfterActionDeadline() {
        AtomicInteger executions = new AtomicInteger();
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(AvailabilityActionType.NODE_EXIT,
            (action, nowMs) -> {
                executions.incrementAndGet();
                return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
                    ActionExecutionStatus.SUCCEEDED, nowMs, nowMs + 1, null);
            }), time);
        RecoveryAction action = action(AvailabilityActionType.NODE_EXIT, false, 200L);
        RecoveryAction retriedAction = new RecoveryAction(action.getActionUuid(), action.getActionType(),
            action.getTarget(), action.getExecutorBrokerId(), 200L, 400L, action.getReason(), action.isDryRun());

        executor.execute(action);
        executor.execute(action);
        time.sleep(101L);
        executor.execute(retriedAction);

        assertEquals(2, executions.get());
    }

    @Test
    public void testMissingAdapterFailsClosed() {
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(), time);

        ActionResponse response = executor.execute(action(AvailabilityActionType.NODE_EXIT, false, 200L));

        assertEquals(ActionExecutionStatus.FAILED, response.getStatus());
        assertTrue(response.getFailureReason().contains("unsupported action type"));
    }

    @Test
    public void testSupportsOnlyRegisteredAdapterActions() {
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(AvailabilityActionType.NODE_EXIT,
            (action, nowMs) -> null), time);

        assertTrue(executor.supports(AvailabilityActionType.NODE_EXIT));
        assertFalse(executor.supports(AvailabilityActionType.CLEAN_SHUTDOWN_RECOVERY));
    }

    @Test
    public void testExpiredActionFailsClosedBeforeAdapter() {
        AtomicInteger executions = new AtomicInteger();
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(AvailabilityActionType.NODE_EXIT,
            (action, nowMs) -> {
                executions.incrementAndGet();
                return null;
            }), time);

        ActionResponse response = executor.execute(action(AvailabilityActionType.NODE_EXIT, false, 99L));

        assertEquals(ActionExecutionStatus.FAILED, response.getStatus());
        assertTrue(response.getFailureReason().contains("deadline"));
        assertEquals(0, executions.get());
    }

    @Test
    public void testAdapterExceptionReturnsFailedResponse() {
        AvailabilityActionExecutor executor = new AvailabilityActionExecutor(Map.of(AvailabilityActionType.NODE_EXIT,
            (action, nowMs) -> {
                throw new IllegalStateException("boom");
            }), time);

        ActionResponse response = executor.execute(action(AvailabilityActionType.NODE_EXIT, false, 200L));

        assertEquals(ActionExecutionStatus.FAILED, response.getStatus());
        assertTrue(response.getFailureReason().contains("IllegalStateException"));
    }

    @Test
    public void testNodeExitAdapterAlwaysRequestsProcessExit() {
        AtomicInteger exits = new AtomicInteger();
        AtomicInteger exitStatus = new AtomicInteger();
        NodeExitActionAdapter adapter = new NodeExitActionAdapter(status -> {
            exits.incrementAndGet();
            exitStatus.set(status);
        });

        assertEquals(ActionExecutionStatus.INCOMPLETE,
            adapter.execute(action(AvailabilityActionType.NODE_EXIT, false, 200L), 100L).getStatus());

        assertEquals(1, exits.get());
        assertEquals(1, exitStatus.get());
    }

    @Test
    public void testSegmentRollAdapterUsesPartitionLayerOwner() throws Exception {
        AtomicInteger rolls = new AtomicInteger();
        SegmentRollActionAdapter adapter = new SegmentRollActionAdapter(action -> rolls.incrementAndGet());

        ActionResponse response = adapter.execute(action(AvailabilityActionType.SEGMENT_ROLL, false, 200L), 100L);

        assertEquals(ActionExecutionStatus.SUCCEEDED, response.getStatus());
        assertEquals(1, rolls.get());
    }

    @Test
    public void testSkipReadRangeAdapterSkipsExactStartOffsetToContainingSegmentExclusiveEndOffset() {
        SkippedReadRangeRegistry registry = new SkippedReadRangeRegistry();
        SkipReadRangeActionAdapter adapter = new SkipReadRangeActionAdapter(registry, (topicPartition, offset) -> {
            assertEquals(new TopicPartition("topic", 0), topicPartition);
            assertEquals(42L, offset);
            return 50L;
        });
        RecoveryAction action = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.SKIP_READ_RANGE,
            AvailabilityTarget.topicPartitionOffset("topic", 0, 42L), -1, 100L, 200L, "read", false);

        ActionResponse response = adapter.execute(action, 100L);

        assertEquals(ActionExecutionStatus.SUCCEEDED, response.getStatus());
        assertEquals(50L, registry.adjustStartOffset(new TopicPartition("topic", 0), 42L));
        assertEquals(49L, registry.adjustStartOffset(new TopicPartition("topic", 0), 49L));
        assertEquals(50L, registry.adjustStartOffset(new TopicPartition("topic", 0), 50L));
    }

    @Test
    public void testSkippedReadRangeRegistryEvictsOldestEntriesWhenBounded() {
        SkippedReadRangeRegistry registry = new SkippedReadRangeRegistry(2);
        TopicPartition topicPartition = new TopicPartition("topic", 0);

        registry.skip(topicPartition, 1L, 10L);
        registry.skip(topicPartition, 2L, 20L);
        registry.skip(topicPartition, 3L, 30L);

        assertEquals(1L, registry.adjustStartOffset(topicPartition, 1L));
        assertEquals(20L, registry.adjustStartOffset(topicPartition, 2L));
        assertEquals(30L, registry.adjustStartOffset(topicPartition, 3L));
    }

    @Test
    public void testSkipReadRangeAdapterFailsClosedForWrongTargetKind() {
        SkipReadRangeActionAdapter adapter = new SkipReadRangeActionAdapter(new SkippedReadRangeRegistry(),
            (topicPartition, offset) -> 100L);

        ActionResponse response = adapter.execute(action(AvailabilityActionType.SKIP_READ_RANGE, false, 200L), 100L);

        assertEquals(ActionExecutionStatus.FAILED, response.getStatus());
        assertTrue(response.getFailureReason().contains("topic-partition-offset target"));
    }

    private RecoveryAction action(AvailabilityActionType actionType, boolean dryRun, long deadlineMs) {
        return new RecoveryAction(Uuid.randomUuid(), actionType, AvailabilityTarget.broker(1, 10L), 1,
            100L, deadlineMs, "test", dryRun);
    }
}
