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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractTask implements Task {

    final TaskId id;
    final String applicationId;
    final ProcessorTopology topology;
    final ProcessorStateManager stateMgr;
    final Set<TopicPartition> partitions;
    final Consumer<byte[], byte[]> consumer;
    final String logPrefix;
    final boolean eosEnabled;
    final Logger log;
    final LogContext logContext;
    final StateDirectory stateDirectory;

    boolean taskInitialized;
    boolean taskClosed;
    boolean commitNeeded;

    InternalProcessorContext processorContext;

    /**
     * @throws ProcessorStateException if the state manager cannot be created
     */
    AbstractTask(final TaskId id,
                 final Set<TopicPartition> partitions,
                 final ProcessorTopology topology,
                 final Consumer<byte[], byte[]> consumer,
                 final ChangelogReader changelogReader,
                 final boolean isStandby,
                 final StateDirectory stateDirectory,
                 final StreamsConfig config) {
        this.id = id;
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        this.stateDirectory = stateDirectory;

        final String threadIdPrefix = String.format("stream-thread [%s] ", Thread.currentThread().getName());
        this.logPrefix = threadIdPrefix + String.format("%s [%s] ", isStandby ? "standby-task" : "task", id);
        this.logContext = new LogContext(logPrefix);
        this.log = logContext.logger(getClass());

        // create the processor state manager
        try {
            stateMgr = new ProcessorStateManager(
                id,
                partitions,
                isStandby,
                stateDirectory,
                topology.storeToChangelogTopic(),
                changelogReader,
                eosEnabled,
                logContext);
        } catch (final IOException e) {
            throw new ProcessorStateException(String.format("%sError while creating the state manager", logPrefix), e);
        }
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public String applicationId() {
        return applicationId;
    }

    @Override
    public Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public ProcessorTopology topology() {
        return topology;
    }

    @Override
    public ProcessorContext context() {
        return processorContext;
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
    }

    /**
     * Produces a string representation containing useful information about a Task.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamTask instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    public boolean isEosEnabled() {
        return eosEnabled;
    }

    /**
     * Produces a string representation containing useful information about a Task starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the Task instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indent);
        sb.append("TaskId: ");
        sb.append(id);
        sb.append("\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (final TopicPartition topicPartition : partitions) {
                sb.append(topicPartition.toString()).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    /**
     * Flush all state stores owned by this task
     */
    void flushState() {
        try {
            stateMgr.flush();
        } catch (final ProcessorStateException e) {
            if (e.getCause() instanceof RecoverableClientException) {
                throw new TaskMigratedException(this, e);
            }
        }
    }

    /**
     * Package-private for testing only
     *
     * @throws StreamsException If the store's change log does not contain the partition
     */
    void registerStateStores() {
        if (topology.stateStores().isEmpty()) {
            return;
        }

        try {
            if (!stateDirectory.lock(id)) {
                throw new LockException(String.format("%sFailed to lock the state directory for task %s", logPrefix, id));
            }
        } catch (final IOException e) {
            throw new StreamsException(
                String.format("%sFatal error while trying to lock the state directory for task %s", logPrefix, id),
                e
            );
        }
        log.trace("Initializing state stores");

        for (final StateStore store : topology.stateStores()) {
            log.debug("Initializing store {}", store.name());
            processorContext.uninitialize();
            store.init(processorContext, store);
        }
    }

    void reinitializeStateStoresForPartitions(final Collection<TopicPartition> partitions) {
        stateMgr.reinitializeStateStoresForPartitions(partitions, processorContext);
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     */
    void closeStateManager(final boolean clean) throws ProcessorStateException {
        ProcessorStateException exception = null;
        log.trace("Closing state manager");
        try {
            stateMgr.close(clean);
        } catch (final ProcessorStateException e) {
            exception = e;
        } finally {
            try {
                stateDirectory.unlock(id);
            } catch (final IOException e) {
                if (exception == null) {
                    exception = new ProcessorStateException(String.format("%sFailed to release state dir lock", logPrefix), e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    public boolean isClosed() {
        return taskClosed;
    }

    public boolean commitNeeded() {
        return commitNeeded;
    }

    public boolean hasStateStores() {
        return !topology.stateStores().isEmpty();
    }

    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }

}
