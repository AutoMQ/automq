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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamFilter<K, V> implements ProcessorSupplier<K, V> {

    private final Predicate<K, V> predicate;
    private final boolean filterNot;

    public KStreamFilter(final Predicate<K, V> predicate, final boolean filterNot) {
        this.predicate = predicate;
        this.filterNot = filterNot;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamFilterProcessor();
    }

    private class KStreamFilterProcessor extends AbstractProcessor<K, V> {
        @Override
        public void process(final K key, final V value) {
            if (filterNot ^ predicate.test(key, value)) {
                context().forward(key, value);
            }
        }
    }
}
