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
package org.apache.kafka.streams.state.internals;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import org.apache.kafka.streams.state.VersionedRecord;

public class MeteredMultiVersionedKeyQueryIterator<V> implements VersionedRecordIterator<V> {

    private final VersionedRecordIterator<byte[]> iterator;
    private final Function<VersionedRecord<byte[]>, VersionedRecord<V>> deserializeValue;
    private final LongAdder numOpenIterators;
    private final Sensor sensor;
    private final Time time;
    private final long startNs;

    public MeteredMultiVersionedKeyQueryIterator(final VersionedRecordIterator<byte[]> iterator,
                                                 final Sensor sensor,
                                                 final Time time,
                                                 final Function<VersionedRecord<byte[]>, VersionedRecord<V>> deserializeValue,
                                                 final LongAdder numOpenIterators) {
        this.iterator = iterator;
        this.deserializeValue = deserializeValue;
        this.numOpenIterators = numOpenIterators;
        this.sensor = sensor;
        this.time = time;
        this.startNs = time.nanoseconds();
        numOpenIterators.increment();
    }


    @Override
    public void close() {
        try {
            iterator.close();
        } finally {
            sensor.record(time.nanoseconds() - startNs);
            numOpenIterators.decrement();
        }
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public VersionedRecord<V> next() {
        return deserializeValue.apply(iterator.next());
    }
}


