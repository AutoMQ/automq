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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.Task.State.CLOSED;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;

public abstract class AbstractTask implements Task {
    private Task.State state = CREATED;
    protected Set<TopicPartition> inputPartitions;

    /**
     * If the checkpoint has not been loaded from the file yet (null), then we should not overwrite the checkpoint;
     * If the checkpoint has been loaded from the file but has not been updated since, then we do not need to checkpoint;
     * If the checkpoint has been loaded from the file and has been updated since, then we could overwrite the checkpoint;
     */
    protected Map<TopicPartition, Long> offsetSnapshotSinceLastFlush = null;

    protected final TaskId id;
    protected final ProcessorTopology topology;
    protected final StateDirectory stateDirectory;
    protected final ProcessorStateManager stateMgr;

    AbstractTask(final TaskId id,
                 final ProcessorTopology topology,
                 final StateDirectory stateDirectory,
                 final ProcessorStateManager stateMgr,
                 final Set<TopicPartition> inputPartitions) {
        this.id = id;
        this.stateMgr = stateMgr;
        this.topology = topology;
        this.inputPartitions = inputPartitions;
        this.stateDirectory = stateDirectory;
    }

    /**
     * The following exceptions maybe thrown from the state manager flushing call
     *
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
     *                          or flushing state store get IO errors; such error should cause the thread to die
     */
    protected void maybeWriteCheckpoint(final boolean enforceCheckpoint) {
        final Map<TopicPartition, Long> offsetSnapshot = stateMgr.changelogOffsets();
        if (StateManagerUtil.checkpointNeeded(enforceCheckpoint, offsetSnapshotSinceLastFlush, offsetSnapshot)) {
            // the state's current offset would be used to checkpoint
            stateMgr.flush();
            stateMgr.checkpoint();
            offsetSnapshotSinceLastFlush = new HashMap<>(offsetSnapshot);
        }
    }


    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public Set<TopicPartition> inputPartitions() {
        return inputPartitions;
    }

    @Override
    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }

    @Override
    public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
        stateMgr.markChangelogAsCorrupted(partitions);
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
    }

    @Override
    public boolean isClosed() {
        return state() == State.CLOSED;
    }

    @Override
    public final Task.State state() {
        return state;
    }

    @Override
    public void revive() {
        if (state == CLOSED) {
            transitionTo(CREATED);
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while reviving task " + id);
        }
    }

    final void transitionTo(final Task.State newState) {
        final State oldState = state();

        if (oldState.isValidTransition(newState)) {
            state = newState;
        } else {
            throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
        }
    }

    @Override
    public void update(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> nodeToSourceTopics) {
        this.inputPartitions = topicPartitions;
        topology.updateSourceTopics(nodeToSourceTopics);
    }
}
