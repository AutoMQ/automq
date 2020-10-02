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

import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockProcessor<K, V> extends AbstractProcessor<K, V> {
    private final MockApiProcessor<K, V, Object, Object> delegate;

    public MockProcessor(final PunctuationType punctuationType,
                         final long scheduleInterval) {
        delegate = new MockApiProcessor<>(punctuationType, scheduleInterval);
    }

    public MockProcessor() {
        delegate = new MockApiProcessor<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        super.init(context);
        delegate.init((org.apache.kafka.streams.processor.api.ProcessorContext<Object, Object>) context);
    }

    @Override
    public void process(final K key, final V value) {
        delegate.process(new Record<>(key, value, context.timestamp(), context.headers()));
    }

    public void checkAndClearProcessResult(final KeyValueTimestamp<?, ?>... expected) {
        delegate.checkAndClearProcessResult(expected);
    }

    public void requestCommit() {
        delegate.requestCommit();
    }

    public void checkEmptyAndClearProcessResult() {
        delegate.checkEmptyAndClearProcessResult();
    }

    public void checkAndClearPunctuateResult(final PunctuationType type, final long... expected) {
        delegate.checkAndClearPunctuateResult(type, expected);
    }

    public Map<K, ValueAndTimestamp<V>> lastValueAndTimestampPerKey() {
        return delegate.lastValueAndTimestampPerKey();
    }

    public List<Long> punctuatedStreamTime() {
        return delegate.punctuatedStreamTime();
    }

    public Cancellable scheduleCancellable() {
        return delegate.scheduleCancellable();
    }

    public ArrayList<KeyValueTimestamp<K, V>> processed() {
        return delegate.processed();
    }
}
