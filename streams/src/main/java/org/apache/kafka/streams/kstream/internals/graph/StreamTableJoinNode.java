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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

/**
 * Represents a join between a KStream and a KTable or GlobalKTable
 */

public class StreamTableJoinNode<K, V> extends GraphNode {

    private final String[] storeNames;
    private final ProcessorParameters<K, V, ?, ?> processorParameters;
    private final String otherJoinSideNodeName;
    private final Duration gracePeriod;
    private final Optional<String> bufferName;


    public StreamTableJoinNode(final String nodeName,
                               final ProcessorParameters<K, V, ?, ?> processorParameters,
                               final String[] storeNames,
                               final String otherJoinSideNodeName,
                               final Duration gracePeriod,
                               final Optional<String> bufferName) {
        super(nodeName);

        // in the case of Stream-Table join the state stores associated with the KTable
        this.storeNames = storeNames;
        this.processorParameters = processorParameters;
        this.otherJoinSideNodeName = otherJoinSideNodeName;
        this.gracePeriod = gracePeriod;
        this.bufferName = bufferName;
    }

    @Override
    public String toString() {
        return "StreamTableJoinNode{" +
               "storeNames=" + Arrays.toString(storeNames) +
               ", processorParameters=" + processorParameters +
               ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String processorName = processorParameters.processorName();
        final ProcessorSupplier<K, V, ?, ?> processorSupplier = processorParameters.processorSupplier();

        // Stream - Table join (Global or KTable)
        topologyBuilder.addProcessor(processorName, processorSupplier, parentNodeNames());

        // Steam - KTable join only
        if (otherJoinSideNodeName != null) {
            topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
            bufferName.ifPresent(s -> topologyBuilder.connectProcessorAndStateStores(processorName, s));
            if (gracePeriod != null) {
                for (final String storeName : storeNames) {
                    if (!topologyBuilder.isStoreVersioned(storeName)) {
                        throw new IllegalArgumentException("KTable must be versioned to use a grace period in a stream table join.");
                    }
                    if (gracePeriod.toMillis() > topologyBuilder.getHistoryRetention(storeName)) {
                        throw new IllegalArgumentException("History retention must be at least grace period.");
                    }
                }
            }
        }

    }
}
