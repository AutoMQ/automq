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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

abstract class AbstractStateManager implements StateManager {
    static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    final File baseDir;
    private final boolean eosEnabled;
    OffsetCheckpoint checkpoint;

    final Map<TopicPartition, Long> checkpointableOffsets = new HashMap<>();
    final Map<String, StateStore> stores = new LinkedHashMap<>();
    final Map<String, StateStore> globalStores = new LinkedHashMap<>();

    AbstractStateManager(final File baseDir,
                         final boolean eosEnabled) {
        this.baseDir = baseDir;
        this.eosEnabled = eosEnabled;
        this.checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
    }

    public void reinitializeStateStoresForPartitions(final Logger log,
                                                     final Map<String, StateStore> stateStores,
                                                     final Map<String, String> storeToChangelogTopic,
                                                     final Collection<TopicPartition> partitions,
                                                     final InternalProcessorContext processorContext) {
        final Map<String, String> changelogTopicToStore = inverseOneToOneMap(storeToChangelogTopic);
        final Set<String> storeToBeReinitialized = new HashSet<>();
        final Map<String, StateStore> storesCopy = new HashMap<>(stateStores);

        for (final TopicPartition topicPartition : partitions) {
            checkpointableOffsets.remove(topicPartition);
            storeToBeReinitialized.add(changelogTopicToStore.get(topicPartition.topic()));
        }

        if (!eosEnabled) {
            try {
                checkpoint.write(checkpointableOffsets);
            } catch (final IOException fatalException) {
                log.error("Failed to write offset checkpoint file to {} while re-initializing {}: {}", checkpoint, stateStores, fatalException);
                throw new StreamsException("Failed to reinitialize global store.", fatalException);
            }
        }

        for (final Map.Entry<String, StateStore> entry : storesCopy.entrySet()) {
            final StateStore stateStore = entry.getValue();
            final String storeName = stateStore.name();
            if (storeToBeReinitialized.contains(storeName)) {
                try {
                    stateStore.close();
                } catch (final RuntimeException ignoreAndSwallow) { /* ignore */ }
                processorContext.uninitialize();
                stateStores.remove(entry.getKey());

                // TODO remove this eventually
                // -> (only after we are sure, we don't need it for backward compatibility reasons anymore; maybe 2.0 release?)
                // this is an ugly "hack" that is required because RocksDBStore does not follow the pattern to put the
                // store directory as <taskDir>/<storeName> but nests it with an intermediate <taskDir>/rocksdb/<storeName>
                try {
                    Utils.delete(new File(baseDir + File.separator + "rocksdb" + File.separator + storeName));
                } catch (final IOException fatalException) {
                    log.error("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(String.format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                try {
                    Utils.delete(new File(baseDir + File.separator + storeName));
                } catch (final IOException fatalException) {
                    log.error("Failed to reinitialize store {}.", storeName, fatalException);
                    throw new StreamsException(String.format("Failed to reinitialize store %s.", storeName), fatalException);
                }

                stateStore.init(processorContext, stateStore);
            }
        }
    }

    private Map<String, String> inverseOneToOneMap(final Map<String, String> origin) {
        final Map<String, String> reversedMap = new HashMap<>();
        for (final Map.Entry<String, String> entry : origin.entrySet()) {
            reversedMap.put(entry.getValue(), entry.getKey());
        }
        return reversedMap;
    }

}
