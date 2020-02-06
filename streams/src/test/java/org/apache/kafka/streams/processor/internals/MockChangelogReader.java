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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockChangelogReader implements ChangelogReader {
    private final Set<TopicPartition> restoringPartitions = new HashSet<>();
    private Map<TopicPartition, Long> restoredOffsets = Collections.emptyMap();

    public boolean isPartitionRegistered(final TopicPartition partition) {
        return restoringPartitions.contains(partition);
    }

    @Override
    public void register(final TopicPartition partition, final ProcessorStateManager stateManager) {
        restoringPartitions.add(partition);
    }

    @Override
    public void restore() {
        // do nothing
    }

    @Override
    public void transitToRestoreActive() {
        // do nothing
    }

    @Override
    public void transitToUpdateStandby() {
        // do nothing
    }

    @Override
    public Set<TopicPartition> completedChangelogs() {
        // assuming all restoring partitions are completed
        return restoringPartitions;
    }

    @Override
    public void clear() {
        restoringPartitions.clear();
    }

    @Override
    public void remove(final Collection<TopicPartition> partitions) {
        restoringPartitions.removeAll(partitions);

        for (final TopicPartition partition : partitions) {
            restoredOffsets.remove(partition);
        }
    }

    @Override
    public boolean isEmpty() {
        return restoredOffsets.isEmpty() && restoringPartitions.isEmpty();
    }
}
