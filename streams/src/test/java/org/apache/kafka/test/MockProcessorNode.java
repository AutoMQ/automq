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
package org.apache.kafka.test;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class MockProcessorNode<K, V> extends ProcessorNode<K, V> {

    private static final String NAME = "MOCK-PROCESS-";
    private static final AtomicInteger INDEX = new AtomicInteger(1);

    public final MockProcessorSupplier<K, V> supplier;
    public boolean closed;
    public long punctuatedAt;
    public boolean initialized;

    public MockProcessorNode(long scheduleInterval) {
        this(scheduleInterval, PunctuationType.STREAM_TIME);
    }

    public MockProcessorNode(long scheduleInterval, PunctuationType punctuationType) {
        this(new MockProcessorSupplier<K, V>(scheduleInterval, punctuationType));
    }

    private MockProcessorNode(MockProcessorSupplier<K, V> supplier) {
        super(NAME + INDEX.getAndIncrement(), supplier.get(), Collections.<String>emptySet());

        this.supplier = supplier;
    }

    @Override
    public void init(final InternalProcessorContext context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void process(K key, V value) {
        processor().process(key, value);
    }

    @Override
    public void punctuate(final long timestamp, final Punctuator punctuator) {
        super.punctuate(timestamp, punctuator);
        this.punctuatedAt = timestamp;
    }

    @Override
    public void close() {
        super.close();
        this.closed = true;
    }
}