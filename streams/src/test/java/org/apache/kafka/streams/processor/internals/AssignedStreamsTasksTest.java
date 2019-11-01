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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.MockSourceNode;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.function.ThrowingRunnable;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AssignedStreamsTasksTest {

    private final StreamTask t1 = EasyMock.createMock(StreamTask.class);
    private final StreamTask t2 = EasyMock.createMock(StreamTask.class);
    private final TopicPartition tp1 = new TopicPartition("t1", 0);
    private final TopicPartition tp2 = new TopicPartition("t2", 0);
    private final TopicPartition changeLog1 = new TopicPartition("cl1", 0);
    private final TopicPartition changeLog2 = new TopicPartition("cl2", 0);
    private final TaskId taskId1 = new TaskId(0, 0);
    private final TaskId taskId2 = new TaskId(1, 0);
    private AssignedStreamsTasks assignedTasks;

    private final List<TopicPartition> revokedChangelogs = new ArrayList<>();

    @Before
    public void before() {
        assignedTasks = new AssignedStreamsTasks(new LogContext("log "));
        EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
        EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();

        revokedChangelogs.clear();
    }

    @Test
    public void shouldInitializeNewTasks() {
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.replay(t1);

        addAndInitTask();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldMoveInitializedTasksNeedingRestoreToRestoring() {
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        t2.initializeMetadata();
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        final Set<TopicPartition> t2partitions = Collections.singleton(tp2);
        EasyMock.expect(t2.partitions()).andReturn(t2partitions);
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        assignedTasks.initializeNewTasks();

        final Collection<StreamTask> restoring = assignedTasks.restoringTasks();
        assertThat(restoring.size(), equalTo(1));
        assertSame(restoring.iterator().next(), t1);
    }

    @Test
    public void shouldMoveInitializedTasksThatDontNeedRestoringToRunning() {
        t2.initializeMetadata();
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t2);

        assignedTasks.addNewTask(t2);
        assignedTasks.initializeNewTasks();

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId2)));
    }

    @Test
    public void shouldTransitionFullyRestoredTasksToRunning() {
        final Set<TopicPartition> task1Partitions = Utils.mkSet(tp1);
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
        EasyMock.expect(t1.hasStateStores()).andReturn(true).anyTimes();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.updateRestored(Utils.mkSet(changeLog1));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.<TaskId>emptySet()));
        assignedTasks.updateRestored(Utils.mkSet(changeLog2));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
    }

    @Test
    public void shouldSuspendRunningTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        assertThat(assignedTasks.suspendedTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseRestoringTasks() {
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).times(2);
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet()).times(3);
        t1.closeStateManager(true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();
        assertThat(assignedTasks.closeRestoringTasks(assignedTasks.restoringTaskIds(), revokedChangelogs), nullValue());

        EasyMock.verify(t1);
    }

    @Test
    public void shouldClosedUnInitializedTasksOnSuspend() {
        EasyMock.expect(t1.changelogPartitions()).andAnswer(Collections::emptyList);

        t1.close(false, false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        assertThat(assignedTasks.suspendOrCloseTasks(assignedTasks.allAssignedTaskIds(), revokedChangelogs), nullValue());

        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotSuspendSuspendedTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());
        assertThat(assignedTasks.suspendOrCloseTasks(assignedTasks.allAssignedTaskIds(), revokedChangelogs), nullValue());
        EasyMock.verify(t1);
    }


    @Test
    public void shouldCloseTaskOnSuspendWhenRuntimeException() {
        mockTaskInitialization();
        EasyMock.expect(t1.partitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();

        t1.suspend();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        t1.close(false, false);
        EasyMock.expectLastCall();

        EasyMock.replay(t1);

        assertThat(suspendTask(), not(nullValue()));
        assertTrue(assignedTasks.runningTaskIds().isEmpty());
        assertTrue(assignedTasks.suspendedTaskIds().isEmpty());
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnSuspendIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.partitions()).andReturn(Collections.emptySet()).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet()).anyTimes();

        t1.suspend();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());
        assertTrue(assignedTasks.runningTaskIds().isEmpty());
        EasyMock.verify(t1);
    }

    @Test
    public void shouldResumeMatchingSuspendedTasks() {
        mockRunningTaskSuspension();
        t1.resume();
        EasyMock.expectLastCall();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        assertTrue(assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotCloseTaskWithinResumeSuspendedIfTaskMigratedException() {
        mockRunningTaskSuspension();
        t1.resume();
        t1.initializeTopology();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);

        assertThat(suspendTask(), nullValue());

        verifyTaskMigratedExceptionDoesNotCloseTask(
            () -> assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
    }

    private void mockTaskInitialization() {
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList());
    }

    @Test
    public void shouldCommitRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.commit();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotCloseTaskWithinCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);
        addAndInitTask();

        verifyTaskMigratedExceptionDoesNotCloseTask(
            () -> assignedTasks.commit());
    }

    @Test
    public void shouldThrowExceptionOnCommitWhenNotCommitFailedOrProducerFenced() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            fail("Should have thrown exception");
        } catch (final Exception e) {
            // ok
        }
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCommitRunningTasksIfNeeded() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.maybeCommitPerUserRequested(), equalTo(1));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotCloseTaskWithinMaybeCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);
        addAndInitTask();

        verifyTaskMigratedExceptionDoesNotCloseTask(
            () -> assignedTasks.maybeCommitPerUserRequested());
    }

    @Test
    public void shouldNotCloseTaskWithinProcessIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        t1.process();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);
        addAndInitTask();

        verifyTaskMigratedExceptionDoesNotCloseTask(
            () -> assignedTasks.process(0L));
    }

    @Test
    public void shouldNotProcessUnprocessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(false);
        EasyMock.replay(t1);
        addAndInitTask();

        assertThat(assignedTasks.process(0L), equalTo(0));

        EasyMock.verify(t1);
    }

    @Test
    public void shouldAlwaysProcessProcessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        EasyMock.expect(t1.process()).andReturn(true).once();

        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.process(0L), equalTo(1));

        EasyMock.verify(t1);
    }

    @Test
    public void shouldPunctuateRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(true);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(2));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotCloseTaskWithinMaybePunctuateStreamTimeIfTaskMigratedException() {
        mockTaskInitialization();
        t1.maybePunctuateStreamTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);
        addAndInitTask();


        verifyTaskMigratedExceptionDoesNotCloseTask(
            () -> assignedTasks.punctuate());
    }

    @Test
    public void shouldNotloseTaskWithinMaybePunctuateSystemTimeIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        t1.maybePunctuateSystemTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.punctuate();
            fail("Should have thrown TaskMigratedException.");
        } catch (final TaskMigratedException expected) {
            assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        }
        EasyMock.verify(t1);
    }

    @Test
    public void shouldReturnNumberOfPunctuations() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(false);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(1));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseCleanlyWithSuspendedTaskAndEOS() {
        final String topic = "topic";

        final Deserializer<byte[]> deserializer = Serdes.ByteArray().deserializer();
        final Serializer<byte[]> serializer = Serdes.ByteArray().serializer();

        final MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(false, serializer, serializer);

        final MockSourceNode<byte[], byte[]> source = new MockSourceNode<>(
            new String[] {"topic"},
            deserializer,
            deserializer);

        final ChangelogReader changelogReader = new MockChangelogReader();

        final ProcessorTopology topology = new ProcessorTopology(
            Collections.singletonList(source),
            Collections.singletonMap(topic, source),
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptySet());

        final Set<TopicPartition> partitions = Collections.singleton(
            new TopicPartition(topic, 1));

        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(RecordingLevel.DEBUG));

        final StreamsMetricsImpl streamsMetrics = new MockStreamsMetrics(metrics);

        final MockTime time = new MockTime();

        final StateDirectory stateDirectory = new StateDirectory(
            StreamTaskTest.createConfig(true),
            time,
            true);

        final StreamTask task = new StreamTask(
            new TaskId(0, 0),
            partitions,
            topology,
            consumer,
            changelogReader,
            StreamTaskTest.createConfig(true),
            streamsMetrics,
            stateDirectory,
            null,
            time,
            () -> producer);

        assignedTasks.addNewTask(task);
        assignedTasks.initializeNewTasks();
        assertNull(assignedTasks.suspendOrCloseTasks(assignedTasks.allAssignedTaskIds(), revokedChangelogs));

        assignedTasks.shutdown(true);
    }

    @Test
    public void shouldClearZombieCreatedTasks() {
        new TaskTestSuite() {
            @Override
            public void additionalSetup(final StreamTask task) {
                task.close(false, true);
            }

            @Override
            public void action(final StreamTask task) {
                assignedTasks.addNewTask(task);
            }

            @Override
            public Set<TaskId> taskIds() {
                return assignedTasks.created.keySet();
            }

            @Override
            public List<TopicPartition> expectedLostChangelogs() {
                return clearingPartitions;
            }
        }.createTaskAndClear();
    }

    @Test
    public void shouldClearZombieRestoringTasks() {
        new TaskTestSuite() {
            @Override
            public void additionalSetup(final StreamTask task) {
                EasyMock.expect(task.partitions()).andReturn(Collections.emptySet()).anyTimes();
                task.closeStateManager(false);
            }

            @Override
            public void action(final StreamTask task) {
                assignedTasks.addTaskToRestoring(task);
            }

            @Override
            public Set<TaskId> taskIds() {
                return assignedTasks.restoringTaskIds();
            }

            @Override
            public List<TopicPartition> expectedLostChangelogs() {
                return clearingPartitions;
            }
        }.createTaskAndClear();
    }

    @Test
    public void shouldClearZombieRunningTasks() {
        new TaskTestSuite() {
            @Override
            public void additionalSetup(final StreamTask task) {
                task.initializeTopology();
                EasyMock.expect(task.partitions()).andReturn(Collections.emptySet()).anyTimes();
                task.close(false, true);
            }

            @Override
            public void action(final StreamTask task) {
                assignedTasks.transitionToRunning(task);
            }

            @Override
            public Set<TaskId> taskIds() {
                return assignedTasks.runningTaskIds();
            }

            @Override
            public List<TopicPartition> expectedLostChangelogs() {
                return clearingPartitions;
            }
        }.createTaskAndClear();
    }

    @Test
    public void shouldClearZombieSuspendedTasks() {
        new TaskTestSuite() {
            @Override
            public void additionalSetup(final StreamTask task) {
                task.initializeTopology();
                EasyMock.expect(task.partitions()).andReturn(Collections.emptySet()).anyTimes();
                task.suspend();
                task.closeSuspended(false, null);
            }

            @Override
            public void action(final StreamTask task) {
                assignedTasks.transitionToRunning(task);
                final List<TopicPartition> revokedChangelogs = new ArrayList<>();
                final List<TaskId> ids = Collections.singletonList(task.id());
                assignedTasks.suspendOrCloseTasks(new HashSet<>(ids), revokedChangelogs);
                assertEquals(clearingPartitions, revokedChangelogs);
            }

            @Override
            public Set<TaskId> taskIds() {
                return assignedTasks.suspendedTaskIds();
            }

            @Override
            public List<TopicPartition> expectedLostChangelogs() {
                return Collections.emptyList();
            }
        }.createTaskAndClear();
    }

    abstract class TaskTestSuite {

        TaskId clearingTaskId = new TaskId(0, 0);
        List<TopicPartition> clearingPartitions = Collections.singletonList(new TopicPartition("topic", 0));

        abstract void additionalSetup(final StreamTask task);

        abstract void action(final StreamTask task);

        abstract Set<TaskId> taskIds();

        abstract List<TopicPartition> expectedLostChangelogs();

        void createTaskAndClear() {
            final StreamTask task = EasyMock.createMock(StreamTask.class);
            EasyMock.expect(task.id()).andReturn(clearingTaskId).anyTimes();
            EasyMock.expect(task.changelogPartitions()).andReturn(clearingPartitions).anyTimes();
            additionalSetup(task);
            EasyMock.replay(task);

            action(task);
            final List<TopicPartition> changelogs = new ArrayList<>();
            final Set<TaskId> ids = new HashSet<>(Collections.singleton(task.id()));
            assertEquals(ids, taskIds());

            assignedTasks.closeZombieTasks(ids, changelogs);
            assertEquals(Collections.emptySet(), taskIds());
            assertEquals(expectedLostChangelogs(), changelogs);
        }
    }

    private void addAndInitTask() {
        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();
    }

    private RuntimeException suspendTask() {
        addAndInitTask();
        return assignedTasks.suspendOrCloseTasks(assignedTasks.allAssignedTaskIds(), revokedChangelogs);
    }

    private void mockRunningTaskSuspension() {
        t1.initializeMetadata();
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.hasStateStores()).andReturn(false).anyTimes();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList()).anyTimes();
        t1.suspend();
        EasyMock.expectLastCall();
    }

    private void verifyTaskMigratedExceptionDoesNotCloseTask(final ThrowingRunnable action) {
        final Set<TaskId> expectedRunningTaskIds = Collections.singleton(taskId1);

        // This action is expected to throw a TaskMigratedException
        assertThrows(TaskMigratedException.class, action);

        // This task should be closed as a zombie with all the other tasks during onPartitionsLost
        assertThat(assignedTasks.runningTaskIds(), equalTo(expectedRunningTaskIds));

        EasyMock.verify(t1);
    }

}