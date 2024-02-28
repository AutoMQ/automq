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

import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorAdapter;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Class used to represent a {@link ProcessorSupplier} or {@link FixedKeyProcessorSupplier} and the name
 * used to register it with the {@link org.apache.kafka.streams.processor.internals.InternalTopologyBuilder}
 *
 * Used by the Join nodes as there are several parameters, this abstraction helps
 * keep the number of arguments more reasonable.
 *
 * @see ProcessorSupplier
 * @see FixedKeyProcessorSupplier
 */
public class ProcessorParameters<KIn, VIn, KOut, VOut> {

    // During the transition to KIP-478, we capture arguments passed from the old API to simplify
    // the performance of casts that we still need to perform. This will eventually be removed.
    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    private final org.apache.kafka.streams.processor.ProcessorSupplier<KIn, VIn> oldProcessorSupplier;
    private final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier;
    private final FixedKeyProcessorSupplier<KIn, VIn, VOut> fixedKeyProcessorSupplier;
    private final String processorName;

    @SuppressWarnings("deprecation") // Old PAPI compatibility.
    public ProcessorParameters(final org.apache.kafka.streams.processor.ProcessorSupplier<KIn, VIn> processorSupplier,
                               final String processorName) {
        oldProcessorSupplier = processorSupplier;
        this.processorSupplier = () -> ProcessorAdapter.adapt(processorSupplier.get());
        fixedKeyProcessorSupplier = null;
        this.processorName = processorName;
    }

    public ProcessorParameters(final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
                               final String processorName) {
        oldProcessorSupplier = null;
        this.processorSupplier = processorSupplier;
        fixedKeyProcessorSupplier = null;
        this.processorName = processorName;
    }

    public ProcessorParameters(final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier,
                               final String processorName) {
        oldProcessorSupplier = null;
        this.processorSupplier = null;
        fixedKeyProcessorSupplier = processorSupplier;
        this.processorName = processorName;
    }

    public ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier() {
        return processorSupplier;
    }

    public FixedKeyProcessorSupplier<KIn, VIn, VOut> fixedKeyProcessorSupplier() {
        return fixedKeyProcessorSupplier;
    }

    public void addProcessorTo(final InternalTopologyBuilder topologyBuilder, final String[] parentNodeNames) {
        if (processorSupplier != null) {
            topologyBuilder.addProcessor(processorName, processorSupplier, parentNodeNames);
            if (processorSupplier.stores() != null) {
                for (final StoreBuilder<?> storeBuilder : processorSupplier.stores()) {
                    topologyBuilder.addStateStore(storeBuilder, processorName);
                }
            }
        }

        if (fixedKeyProcessorSupplier != null) {
            topologyBuilder.addProcessor(processorName, fixedKeyProcessorSupplier, parentNodeNames);
            if (fixedKeyProcessorSupplier.stores() != null) {
                for (final StoreBuilder<?> storeBuilder : fixedKeyProcessorSupplier.stores()) {
                    topologyBuilder.addStateStore(storeBuilder, processorName);
                }
            }
        }

        // temporary hack until KIP-478 is fully implemented
        // Old PAPI. Needs to be migrated.
        if (oldProcessorSupplier != null && oldProcessorSupplier.stores() != null) {
            for (final StoreBuilder<?> storeBuilder : oldProcessorSupplier.stores()) {
                topologyBuilder.addStateStore(storeBuilder, processorName);
            }
        }
    }

    public String processorName() {
        return processorName;
    }

    @Override
    public String toString() {
        return "ProcessorParameters{" +
            "processor supplier class=" + (processorSupplier != null ? processorSupplier.getClass() : "null") +
            ", fixed key processor supplier class=" + (fixedKeyProcessorSupplier != null ? fixedKeyProcessorSupplier.getClass() : "null") +
            ", processor name='" + processorName + '\'' +
            '}';
    }
}
