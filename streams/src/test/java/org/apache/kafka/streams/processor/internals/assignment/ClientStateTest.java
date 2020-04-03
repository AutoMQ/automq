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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.junit.Test;

import java.util.Collections;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_1;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.TASK_0_2;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ClientStateTest {

    private final ClientState client = new ClientState(1);
    private final ClientState zeroCapacityClient = new ClientState(0);

    @Test
    public void shouldHaveNotReachedCapacityWhenAssignedTasksLessThanCapacity() {
        assertFalse(client.reachedCapacity());
    }

    @Test
    public void shouldHaveReachedCapacityWhenAssignedTasksGreaterThanOrEqualToCapacity() {
        client.assignActive(TASK_0_1);
        assertTrue(client.reachedCapacity());
    }

    @Test
    public void shouldAddActiveTasksToBothAssignedAndActive() {
        client.assignActive(TASK_0_1);
        assertThat(client.activeTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.standbyTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddStandbyTasksToBothStandbyAndAssigned() {
        client.assignStandby(TASK_0_1);
        assertThat(client.assignedTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertThat(client.standbyTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertThat(client.assignedTaskCount(), equalTo(1));
        assertThat(client.activeTasks().size(), equalTo(0));
    }

    @Test
    public void shouldAddPreviousActiveTasksToPreviousAssignedAndPreviousActive() {
        client.addPreviousActiveTasks(Utils.mkSet(TASK_0_1, TASK_0_2));
        assertThat(client.prevActiveTasks(), equalTo(Utils.mkSet(TASK_0_1, TASK_0_2)));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldAddPreviousStandbyTasksToPreviousAssignedAndPreviousStandby() {
        client.addPreviousStandbyTasks(Utils.mkSet(TASK_0_1, TASK_0_2));
        assertThat(client.prevActiveTasks().size(), equalTo(0));
        assertThat(client.previousAssignedTasks(), equalTo(Utils.mkSet(TASK_0_1, TASK_0_2)));
    }

    @Test
    public void shouldHaveAssignedTaskIfActiveTaskAssigned() {
        client.assignActive(TASK_0_1);
        assertTrue(client.hasAssignedTask(TASK_0_1));
    }

    @Test
    public void shouldHaveAssignedTaskIfStandbyTaskAssigned() {
        client.assignStandby(TASK_0_1);
        assertTrue(client.hasAssignedTask(TASK_0_1));
    }

    @Test
    public void shouldNotHaveAssignedTaskIfTaskNotAssigned() {
        client.assignActive(TASK_0_1);
        assertFalse(client.hasAssignedTask(TASK_0_2));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityTheSameButFewerAssignedTasks() {
        final ClientState otherClient = new ClientState(1);
        client.assignActive(TASK_0_1);
        assertTrue(otherClient.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(otherClient));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityHigherAndSameAssignedTaskCount() {
        final ClientState otherClient = new ClientState(2);
        assertTrue(otherClient.hasMoreAvailableCapacityThan(client));
        assertFalse(client.hasMoreAvailableCapacityThan(otherClient));
    }

    @Test
    public void shouldUseMultiplesOfCapacityToDetermineClientWithMoreAvailableCapacity() {
        final ClientState otherClient = new ClientState(2);

        for (int i = 0; i < 7; i++) {
            otherClient.assignActive(new TaskId(0, i));
        }

        for (int i = 7; i < 11; i++) {
            client.assignActive(new TaskId(0, i));
        }

        assertTrue(otherClient.hasMoreAvailableCapacityThan(client));
    }

    @Test
    public void shouldHaveMoreAvailableCapacityWhenCapacityIsTheSameButAssignedTasksIsLess() {
        final ClientState client = new ClientState(3);
        final ClientState otherClient = new ClientState(3);
        for (int i = 0; i < 4; i++) {
            client.assignActive(new TaskId(0, i));
            otherClient.assignActive(new TaskId(0, i));
        }
        otherClient.assignActive(new TaskId(0, 5));
        assertTrue(client.hasMoreAvailableCapacityThan(otherClient));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCapacityOfThisClientStateIsZero() {
        assertThrows(IllegalStateException.class, () -> zeroCapacityClient.hasMoreAvailableCapacityThan(client));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfCapacityOfOtherClientStateIsZero() {
        assertThrows(IllegalStateException.class, () -> client.hasMoreAvailableCapacityThan(zeroCapacityClient));
    }

    @Test
    public void shouldHaveUnfulfilledQuotaWhenActiveTaskSizeLessThanCapacityTimesTasksPerThread() {
        client.assignActive(new TaskId(0, 1));
        assertTrue(client.hasUnfulfilledQuota(2));
    }

    @Test
    public void shouldNotHaveUnfulfilledQuotaWhenActiveTaskSizeGreaterEqualThanCapacityTimesTasksPerThread() {
        client.assignActive(new TaskId(0, 1));
        assertFalse(client.hasUnfulfilledQuota(1));
    }

    @Test
    public void shouldAddTasksWithLatestOffsetToPrevActiveTasks() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, Task.LATEST_OFFSET);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.initializePrevTasks(Collections.emptyMap());
        assertThat(client.prevActiveTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertThat(client.previousAssignedTasks(), equalTo(Collections.singleton(TASK_0_1)));
        assertTrue(client.prevStandbyTasks().isEmpty());
    }

    @Test
    public void shouldAddTasksInOffsetSumsMapToPrevStandbyTasks() {
        final Map<TaskId, Long> taskOffsetSums = mkMap(
            mkEntry(TASK_0_1, 0L),
            mkEntry(TASK_0_2, 100L)
        );
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.initializePrevTasks(Collections.emptyMap());
        assertThat(client.prevStandbyTasks(), equalTo(mkSet(TASK_0_1, TASK_0_2)));
        assertThat(client.previousAssignedTasks(), equalTo(mkSet(TASK_0_1, TASK_0_2)));
        assertTrue(client.prevActiveTasks().isEmpty());
    }

    @Test
    public void shouldComputeTaskLags() {
        final Map<TaskId, Long> taskOffsetSums = mkMap(
            mkEntry(TASK_0_1, 0L),
            mkEntry(TASK_0_2, 100L)
        );
        final Map<TaskId, Long> allTaskEndOffsetSums = mkMap(
            mkEntry(TASK_0_1, 500L),
            mkEntry(TASK_0_2, 100L)
        );
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);

        assertThat(client.lagFor(TASK_0_1), equalTo(500L));
        assertThat(client.lagFor(TASK_0_2), equalTo(0L));
    }

    @Test
    public void shouldReturnEndOffsetSumForLagOfTaskWeDidNotPreviouslyOwn() {
        final Map<TaskId, Long> taskOffsetSums = Collections.emptyMap();
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 500L);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);
        assertThat(client.lagFor(TASK_0_1), equalTo(500L));
    }

    @Test
    public void shouldReturnLatestOffsetForLagOfPreviousActiveRunningTask() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, Task.LATEST_OFFSET);
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 500L);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);
        assertThat(client.lagFor(TASK_0_1), equalTo(Task.LATEST_OFFSET));
    }

    @Test
    public void shouldReturnUnknownOffsetSumForLagOfTaskWithUnknownOffset() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, UNKNOWN_OFFSET_SUM);
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 500L);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);
        assertThat(client.lagFor(TASK_0_1), equalTo(UNKNOWN_OFFSET_SUM));
    }

    @Test
    public void shouldReturnEndOffsetSumIfOffsetSumIsGreaterThanEndOffsetSum() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, 5L);
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 1L);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);
        assertThat(client.lagFor(TASK_0_1), equalTo(1L));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfTaskLagsMapIsNotEmpty() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, 5L);
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 1L);
        client.computeTaskLags(null, taskOffsetSums);
        assertThrows(IllegalStateException.class, () -> client.computeTaskLags(null, allTaskEndOffsetSums));
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnLagForUnknownTask() {
        final Map<TaskId, Long> taskOffsetSums = Collections.singletonMap(TASK_0_1, 0L);
        final Map<TaskId, Long> allTaskEndOffsetSums = Collections.singletonMap(TASK_0_1, 500L);
        client.addPreviousTasksAndOffsetSums(taskOffsetSums);
        client.computeTaskLags(null, allTaskEndOffsetSums);
        assertThrows(IllegalStateException.class, () -> client.lagFor(TASK_0_2));
    }

    @Test
    public void shouldThrowIllegalStateExceptionIfAttemptingToInitializeNonEmptyPrevTaskSets() {
        client.addPreviousActiveTasks(Collections.singleton(TASK_0_1));
        assertThrows(IllegalStateException.class, () -> client.initializePrevTasks(Collections.emptyMap()));
    }

}
