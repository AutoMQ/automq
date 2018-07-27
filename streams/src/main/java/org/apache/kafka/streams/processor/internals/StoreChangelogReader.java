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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StoreChangelogReader implements ChangelogReader {

    private final Logger log;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StateRestoreListener userStateRestoreListener;
    private final Map<TopicPartition, Long> endOffsets = new HashMap<>();
    private final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> needsRestoring = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> needsInitializing = new HashMap<>();
    private final Duration pollTime;

    public StoreChangelogReader(final Consumer<byte[], byte[]> restoreConsumer,
                                final Duration pollTime,
                                final StateRestoreListener userStateRestoreListener,
                                final LogContext logContext) {
        this.restoreConsumer = restoreConsumer;
        this.pollTime = pollTime;
        this.log = logContext.logger(getClass());
        this.userStateRestoreListener = userStateRestoreListener;
    }

    @Override
    public void register(final StateRestorer restorer) {
        restorer.setUserRestoreListener(userStateRestoreListener);
        stateRestorers.put(restorer.partition(), restorer);
        needsInitializing.put(restorer.partition(), restorer);
    }

    public Collection<TopicPartition> restore(final RestoringTasks active) {
        if (!needsInitializing.isEmpty()) {
            initialize(active);
        }

        if (needsRestoring.isEmpty()) {
            restoreConsumer.unsubscribe();
            return completed();
        }

        try {
            final ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(pollTime);
            final Iterator<TopicPartition> iterator = needsRestoring.keySet().iterator();
            while (iterator.hasNext()) {
                final TopicPartition partition = iterator.next();
                final StateRestorer restorer = stateRestorers.get(partition);
                final long pos = processNext(records.records(partition), restorer, endOffsets.get(partition));
                restorer.setRestoredOffset(pos);
                if (restorer.hasCompleted(pos, endOffsets.get(partition))) {
                    restorer.restoreDone();
                    endOffsets.remove(partition);
                    iterator.remove();
                }
            }
        } catch (final InvalidOffsetException recoverableException) {
            log.warn("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
            final Set<TopicPartition> partitions = recoverableException.partitions();
            for (final TopicPartition partition : partitions) {
                final StreamTask task = active.restoringTaskFor(partition);
                log.info("Reinitializing StreamTask {}", task);
                task.reinitializeStateStoresForPartitions(recoverableException.partitions());
            }
            restoreConsumer.seekToBeginning(partitions);
        }

        if (needsRestoring.isEmpty()) {
            restoreConsumer.unsubscribe();
        }

        return completed();
    }

    private void initialize(final RestoringTasks active) {
        if (!restoreConsumer.subscription().isEmpty()) {
            throw new StreamsException("Restore consumer should not be subscribed to any topics (" + restoreConsumer.subscription() + ")");
        }

        // first refresh the changelog partition information from brokers, since initialize is only called when
        // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
        refreshChangelogInfo();

        final Map<TopicPartition, StateRestorer> initializable = new HashMap<>();
        for (final Map.Entry<TopicPartition, StateRestorer> entry : needsInitializing.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            if (hasPartition(topicPartition)) {
                initializable.put(entry.getKey(), entry.getValue());
            }
        }

        // try to fetch end offsets for the initializable restorers and remove any partitions
        // where we already have all of the data
        try {
            endOffsets.putAll(restoreConsumer.endOffsets(initializable.keySet()));
        } catch (final TimeoutException e) {
            // if timeout exception gets thrown we just give up this time and retry in the next run loop
            log.debug("Could not fetch end offset for {}; will fall back to partition by partition fetching", initializable);
            return;
        }

        final Iterator<TopicPartition> iter = initializable.keySet().iterator();
        while (iter.hasNext()) {
            final TopicPartition topicPartition = iter.next();
            final Long endOffset = endOffsets.get(topicPartition);

            // offset should not be null; but since the consumer API does not guarantee it
            // we add this check just in case
            if (endOffset != null) {
                final StateRestorer restorer = needsInitializing.get(topicPartition);
                if (restorer.checkpoint() >= endOffset) {
                    restorer.setRestoredOffset(restorer.checkpoint());
                    iter.remove();
                } else if (restorer.offsetLimit() == 0 || endOffset == 0) {
                    restorer.setRestoredOffset(0);
                    iter.remove();
                } else {
                    restorer.setEndingOffset(endOffset);
                }
                needsInitializing.remove(topicPartition);
            } else {
                log.info("End offset cannot be found form the returned metadata; removing this partition from the current run loop");
                iter.remove();
            }
        }

        // set up restorer for those initializable
        if (!initializable.isEmpty()) {
            startRestoration(initializable, active);
        }
    }

    private void startRestoration(final Map<TopicPartition, StateRestorer> initialized,
                                  final RestoringTasks active) {
        log.debug("Start restoring state stores from changelog topics {}", initialized.keySet());

        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());
        assignment.addAll(initialized.keySet());
        restoreConsumer.assign(assignment);

        final List<StateRestorer> needsPositionUpdate = new ArrayList<>();
        for (final StateRestorer restorer : initialized.values()) {
            if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT) {
                restoreConsumer.seek(restorer.partition(), restorer.checkpoint());
                logRestoreOffsets(restorer.partition(),
                                  restorer.checkpoint(),
                                  endOffsets.get(restorer.partition()));
                restorer.setStartingOffset(restoreConsumer.position(restorer.partition()));
                restorer.restoreStarted();
            } else {
                final StreamTask task = active.restoringTaskFor(restorer.partition());

                // If checkpoint does not exist it means the task was not shutdown gracefully before;
                // and in this case if EOS is turned on we should wipe out the state and re-initialize the task
                if (task.isEosEnabled()) {
                    log.info("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                            "Reinitializing the task and restore its state from the beginning.", task.id, restorer.storeName(), restorer.partition());
                    task.reinitializeStateStoresForPartitions(Collections.singleton(restorer.partition()));
                } else {
                    log.info("Restoring task {}'s state store {} from beginning of the changelog {} ", task.id, restorer.storeName(), restorer.partition());
                }

                restoreConsumer.seekToBeginning(Collections.singletonList(restorer.partition()));
                needsPositionUpdate.add(restorer);
            }
        }

        for (final StateRestorer restorer : needsPositionUpdate) {
            final long position = restoreConsumer.position(restorer.partition());
            logRestoreOffsets(restorer.partition(),
                              position,
                              endOffsets.get(restorer.partition()));
            restorer.setStartingOffset(position);
            restorer.restoreStarted();
        }

        needsRestoring.putAll(initialized);
    }

    private void logRestoreOffsets(final TopicPartition partition,
                                   final long startingOffset,
                                   final Long endOffset) {
        log.debug("Restoring partition {} from offset {} to endOffset {}",
                  partition,
                  startingOffset,
                  endOffset);
    }

    private Collection<TopicPartition> completed() {
        final Set<TopicPartition> completed = new HashSet<>(stateRestorers.keySet());
        completed.removeAll(needsRestoring.keySet());
        log.trace("The set of restoration completed partitions so far: {}", completed);
        return completed;
    }

    private void refreshChangelogInfo() {
        try {
            partitionInfo.putAll(restoreConsumer.listTopics());
        } catch (final TimeoutException e) {
            log.debug("Could not fetch topic metadata within the timeout, will retry in the next run loop");
        }
    }

    @Override
    public Map<TopicPartition, Long> restoredOffsets() {
        final Map<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, StateRestorer> entry : stateRestorers.entrySet()) {
            final StateRestorer restorer = entry.getValue();
            if (restorer.isPersistent()) {
                restoredOffsets.put(entry.getKey(), restorer.restoredOffset());
            }
        }
        return restoredOffsets;
    }

    @Override
    public void reset() {
        partitionInfo.clear();
        stateRestorers.clear();
        needsRestoring.clear();
        endOffsets.clear();
        needsInitializing.clear();
    }

    private long processNext(final List<ConsumerRecord<byte[], byte[]>> records,
                             final StateRestorer restorer,
                             final Long endOffset) {
        final List<KeyValue<byte[], byte[]>> restoreRecords = new ArrayList<>();
        long nextPosition = -1;
        final int numberRecords = records.size();
        int numberRestored = 0;
        long lastRestoredOffset = -1;
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset)) {
                nextPosition = record.offset();
                break;
            }
            lastRestoredOffset = offset;
            numberRestored++;
            if (record.key() != null) {
                restoreRecords.add(KeyValue.pair(record.key(), record.value()));
            }
        }


        // if we have changelog topic then we should have restored all records in the list
        // otherwise if we did not fully restore to that point we need to set nextPosition
        // to the position of the restoreConsumer and we'll cause a TaskMigratedException exception
        if (nextPosition == -1 || (restorer.offsetLimit() == Long.MAX_VALUE && numberRecords != numberRestored)) {
            nextPosition = restoreConsumer.position(restorer.partition());
        }

        if (!restoreRecords.isEmpty()) {
            restorer.restore(restoreRecords);
            restorer.restoreBatchCompleted(lastRestoredOffset, records.size());

            log.trace("Restored from {} to {} with {} records, ending offset is {}, next starting position is {}",
                    restorer.partition(), restorer.storeName(), records.size(), lastRestoredOffset, nextPosition);
        }

        return nextPosition;
    }



    private boolean hasPartition(final TopicPartition topicPartition) {
        final List<PartitionInfo> partitions = partitionInfo.get(topicPartition.topic());

        if (partitions == null) {
            return false;
        }

        for (final PartitionInfo partition : partitions) {
            if (partition.partition() == topicPartition.partition()) {
                return true;
            }
        }

        return false;
    }
}
