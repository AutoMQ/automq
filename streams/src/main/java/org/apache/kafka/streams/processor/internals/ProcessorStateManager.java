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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;

/**
 * ProcessorStateManager is the source of truth for the current offset for each state store,
 * which is either the read offset during restoring, or the written offset during normal processing.
 *
 * The offset is initialized as null when the state store is registered, and then it can be updated by
 * loading checkpoint file, restore state stores, or passing from the record collector's written offsets.
 *
 * When checkpointing, if the offset is not null it would be written to the file.
 *
 * The manager is also responsible for restoring state stores via their registered restore callback,
 * which is used for both updating standby tasks as well as restoring active tasks.
 */
public class ProcessorStateManager implements StateManager {

    public static class StateStoreMetadata {
        private final StateStore stateStore;

        // corresponding changelog partition of the store, this and the following two fields
        // will only be not-null if the state store is logged (i.e. changelog partition and restorer provided)
        private final TopicPartition changelogPartition;

        // could be used for both active restoration and standby
        private final StateRestoreCallback restoreCallback;

        // record converters used for restoration and standby
        private final RecordConverter recordConverter;

        // indicating the current snapshot of the store as the offset of last changelog record that has been
        // applied to the store used for both restoration (active and standby tasks restored offset) and
        // normal processing that update stores (written offset); could be null (when initialized)
        //
        // the offset is updated in three ways:
        //   1. when loading from the checkpoint file, when the corresponding task has acquired the state
        //      directory lock and have registered all the state store; it is only one-time
        //   2. when updating with restore records (by both restoring active and standby),
        //      update to the last restore record's offset
        //   3. when checkpointing with the given written offsets from record collector,
        //      update blindly with the given offset
        private Long offset;

        private StateStoreMetadata(final StateStore stateStore) {
            this.stateStore = stateStore;
            this.restoreCallback = null;
            this.recordConverter = null;
            this.changelogPartition = null;
            this.offset = null;
        }

        private StateStoreMetadata(final StateStore stateStore,
                                   final TopicPartition changelogPartition,
                                   final StateRestoreCallback restoreCallback,
                                   final RecordConverter recordConverter) {
            if (restoreCallback == null) {
                throw new IllegalStateException("Log enabled store should always provide a restore callback upon registration");
            }

            this.stateStore = stateStore;
            this.changelogPartition = changelogPartition;
            this.restoreCallback = restoreCallback;
            this.recordConverter = recordConverter;
            this.offset = null;
        }

        private void setOffset(final Long offset) {
            this.offset = offset;
        }

        // the offset is exposed to the changelog reader to determine if restoration is completed
        Long offset() {
            return this.offset;
        }

        TopicPartition changelogPartition() {
            return this.changelogPartition;
        }

        StateStore store() {
            return this.stateStore;
        }

        @Override
        public String toString() {
            return "StateStoreMetadata (" + stateStore.name() + " : " + changelogPartition + " @ " + offset;
        }
    }

    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private final Logger log;
    private final TaskId taskId;
    private final String logPrefix;
    private final TaskType taskType;
    private final ChangelogRegister changelogReader;
    private final Map<String, String> storeToChangelogTopic;
    private final Collection<TopicPartition> sourcePartitions;

    // must be maintained in topological order
    private final FixedOrderMap<String, StateStoreMetadata> stores = new FixedOrderMap<>();
    private final FixedOrderMap<String, StateStore> globalStores = new FixedOrderMap<>();

    private final File baseDir;
    private final OffsetCheckpoint checkpointFile;

    public static String storeChangelogTopic(final String applicationId,
                                             final String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final TaskType taskType,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic,
                                 final ChangelogRegister changelogReader,
                                 final LogContext logContext) throws ProcessorStateException {
        this.logPrefix = format("task [%s] ", taskId);
        this.log = logContext.logger(ProcessorStateManager.class);

        this.taskId = taskId;
        this.taskType = taskType;
        this.sourcePartitions = sources;
        this.changelogReader = changelogReader;
        this.storeToChangelogTopic = storeToChangelogTopic;

        this.baseDir = stateDirectory.directoryForTask(taskId);
        this.checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));

        log.debug("Created state store manager for task {}", taskId);
    }

    void registerGlobalStateStores(final List<StateStore> stateStores) {
        log.debug("Register global stores {}", stateStores);
        for (final StateStore stateStore : stateStores) {
            globalStores.put(stateStore.name(), stateStore);
        }
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return globalStores.get(name);
    }

    // package-private for test only
    void initializeStoreOffsetsFromCheckpoint() {
        try {
            final Map<TopicPartition, Long> loadedCheckpoints = checkpointFile.read();

            log.trace("Loaded offsets from the checkpoint file: {}", loadedCheckpoints);

            for (final StateStoreMetadata store : stores.values()) {
                if (store.changelogPartition == null) {
                    log.info("State store {} is not logged and hence would not be restored", store.stateStore.name());
                } else {
                    if (loadedCheckpoints.containsKey(store.changelogPartition)) {
                        store.setOffset(loadedCheckpoints.remove(store.changelogPartition));

                        log.debug("State store {} initialized from checkpoint with offset {} at changelog {}",
                            store.stateStore.name(), store.offset, store.changelogPartition);
                    } else {
                        // TODO K9113: for EOS when there's no checkpointed offset, we should treat it as TaskCorrupted

                        log.info("State store {} did not find checkpoint offset, hence would " +
                                "default to the starting offset at changelog {}",
                            store.stateStore.name(), store.changelogPartition);
                    }
                }
            }

            if (!loadedCheckpoints.isEmpty()) {
                log.warn("Some loaded checkpoint offsets cannot find their corresponding state stores: {}", loadedCheckpoints);
            }

            checkpointFile.delete();
        } catch (final IOException | RuntimeException e) {
            // both IOException or runtime exception like number parsing can throw
            throw new ProcessorStateException(format("%sError loading and deleting checkpoint file when creating the state manager",
                logPrefix), e);
        }
    }

    @Override
    public File baseDir() {
        return baseDir;
    }

    @Override
    public void registerStore(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        final String storeName = store.name();

        if (CHECKPOINT_FILE_NAME.equals(storeName)) {
            throw new IllegalArgumentException(format("%sIllegal store name: %s, which collides with the pre-defined " +
                "checkpoint file name", logPrefix, storeName));
        }

        if (stores.containsKey(storeName)) {
            throw new IllegalArgumentException(format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        final String topic = storeToChangelogTopic.get(storeName);

        // if the store name does not exist in the changelog map, it means the underlying store
        // is not log enabled (including global stores), and hence it does not need to be restored
        if (topic != null) {
            // NOTE we assume the partition of the topic can always be inferred from the task id;
            // if user ever use a default partition grouper (deprecated in KIP-528) this would break and
            // it is not a regression (it would always break anyways)
            final TopicPartition storePartition = new TopicPartition(topic, taskId.partition);
            final StateStoreMetadata storeMetadata = new StateStoreMetadata(
                store,
                storePartition,
                stateRestoreCallback,
                converterForStore(store));
            stores.put(storeName, storeMetadata);

            changelogReader.register(storePartition, this);
        } else {
            stores.put(storeName, new StateStoreMetadata(store));
        }

        log.debug("Registered state store {} to its state manager", storeName);
    }

    @Override
    public StateStore getStore(final String name) {
        if (stores.containsKey(name)) {
            return stores.get(name).stateStore;
        } else {
            return null;
        }
    }

    Collection<TopicPartition> changelogPartitions() {
        return changelogOffsets().keySet();
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        // return the current offsets for those logged stores
        final Map<TopicPartition, Long> changelogOffsets = new HashMap<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (storeMetadata.changelogPartition != null) {
                // for changelog whose offset is unknown, use 0L indicating earliest offset
                // otherwise return the current offset + 1 as the next offset to fetch
                changelogOffsets.put(
                    storeMetadata.changelogPartition,
                    storeMetadata.offset == null ? 0L : storeMetadata.offset + 1L);
            }
        }
        return changelogOffsets;
    }

    TaskId taskId() {
        return taskId;
    }

    // used by the changelog reader only
    boolean changelogAsSource(final TopicPartition partition) {
        return sourcePartitions.contains(partition);
    }

    // used by the changelog reader only
    TaskType taskType() {
        return taskType;
    }

    // used by the changelog reader only
    StateStoreMetadata storeMetadata(final TopicPartition partition) {
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (partition.equals(storeMetadata.changelogPartition)) {
                return storeMetadata;
            }
        }
        return null;
    }

    // used by the changelog reader only
    void restore(final StateStoreMetadata storeMetadata, final List<ConsumerRecord<byte[], byte[]>> restoreRecords) {
        if (!stores.values().contains(storeMetadata)) {
            throw new IllegalStateException("Restoring " + storeMetadata + " which is not registered in this state manager, " +
                "this should not happen.");
        }

        if (!restoreRecords.isEmpty()) {
            // restore states from changelog records and update the snapshot offset as the batch end record's offset
            final Long batchEndOffset = restoreRecords.get(restoreRecords.size() - 1).offset();
            final RecordBatchingStateRestoreCallback restoreCallback = adapt(storeMetadata.restoreCallback);
            final List<ConsumerRecord<byte[], byte[]>> convertedRecords = restoreRecords.stream()
                .map(storeMetadata.recordConverter::convert)
                .collect(Collectors.toList());

            try {
                restoreCallback.restoreBatch(convertedRecords);
            } catch (final RuntimeException e) {
                throw new ProcessorStateException(
                    format("%sException caught while trying to restore state from %s", logPrefix, storeMetadata.changelogPartition),
                    e
                );
            }

            storeMetadata.setOffset(batchEndOffset);
        }
    }

    /**
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
     *                          or flushing state store get IO errors; such error should cause the thread to die
     */
    @Override
    public void flush() {
        RuntimeException firstException = null;
        // attempting to flush the stores
        if (!stores.isEmpty()) {
            log.debug("Flushing all stores registered in the state manager: {}", stores);
            for (final Map.Entry<String, StateStoreMetadata> entry : stores.entrySet()) {
                final StateStore store = entry.getValue().stateStore;
                log.trace("Flushing store {}", store.name());
                try {
                    store.flush();
                } catch (final RuntimeException exception) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (exception instanceof StreamsException)
                            firstException = exception;
                        else
                            firstException = new ProcessorStateException(
                                format("%sFailed to flush state store %s", logPrefix, store.name()), exception);
                    }
                    log.error("Failed to flush state store {}: ", store.name(), exception);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * {@link StateStore#close() Close} all stores (even in case of failure).
     * Log all exception and re-throw the first exception that did occur at the end.
     *
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    @Override
    public void close() throws ProcessorStateException {
        RuntimeException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!stores.isEmpty()) {
            log.debug("Closing its state manager and all the registered state stores: {}", stores);
            for (final Map.Entry<String, StateStoreMetadata> entry : stores.entrySet()) {
                final StateStore store = entry.getValue().stateStore;
                log.trace("Closing store {}", store.name());
                try {
                    store.close();
                } catch (final RuntimeException exception) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (exception instanceof StreamsException)
                            firstException = exception;
                        else
                            firstException = new ProcessorStateException(
                                format("%sFailed to close state store %s", logPrefix, store.name()), exception);
                    }
                    log.error("Failed to close state store {}: ", store.name(), exception);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public void checkpoint(final Map<TopicPartition, Long> writtenOffsets) {
        // first update each state store's current offset, then checkpoint
        // those stores that are only logged and persistent to the checkpoint file
        for (final Map.Entry<TopicPartition, Long> entry : writtenOffsets.entrySet()) {
            final StateStoreMetadata store = findStore(entry.getKey());

            if (store != null) {
                store.setOffset(entry.getValue());

                log.debug("State store {} updated to written offset {} at changelog {}",
                    store.stateStore.name(), store.offset, store.changelogPartition);
            }
        }

        final Map<TopicPartition, Long> checkpointingOffsets = new HashMap<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            // store is logged, persistent, and has a valid current offset
            if (storeMetadata.changelogPartition != null &&
                storeMetadata.stateStore.persistent() &&
                storeMetadata.offset != null) {
                checkpointingOffsets.put(storeMetadata.changelogPartition, storeMetadata.offset);
            }
        }

        log.debug("Writing checkpoint: {}", checkpointingOffsets);
        try {
            checkpointFile.write(checkpointingOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to [{}]", checkpointFile, e);
        }
    }

    private StateStoreMetadata findStore(final TopicPartition changelogPartition) {
        final List<StateStoreMetadata> found = stores.values().stream()
            .filter(metadata -> changelogPartition.equals(metadata.changelogPartition))
            .collect(Collectors.toList());

        if (found.size() > 1) {
            throw new IllegalStateException("Multiple state stores are found for changelog partition " + changelogPartition +
                ", this should never happen: " + found);
        }

        return found.isEmpty() ? null : found.get(0);
    }
}
