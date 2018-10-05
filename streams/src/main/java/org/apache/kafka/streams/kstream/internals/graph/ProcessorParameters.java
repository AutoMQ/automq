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

import org.apache.kafka.streams.processor.ProcessorSupplier;

/**
 * Class used to represent a {@link ProcessorSupplier} and the name
 * used to register it with the {@link org.apache.kafka.streams.processor.internals.InternalTopologyBuilder}
 *
 * Used by the Join nodes as there are several parameters, this abstraction helps
 * keep the number of arguments more reasonable.
 */
public class ProcessorParameters<K, V> {

    private final ProcessorSupplier<K, V> processorSupplier;
    private final String processorName;

    public ProcessorParameters(final ProcessorSupplier<K, V> processorSupplier,
                               final String processorName) {

        this.processorSupplier = processorSupplier;
        this.processorName = processorName;
    }

    public ProcessorSupplier<K, V> processorSupplier() {
        return processorSupplier;
    }

    public String processorName() {
        return processorName;
    }

    @Override
    public String toString() {
        return "ProcessorParameters{" +
            "processor class=" + processorSupplier.get().getClass() +
            ", processor name='" + processorName + '\'' +
            '}';
    }
}
