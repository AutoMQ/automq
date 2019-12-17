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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;

class StandbyContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {

    StandbyContextImpl(final TaskId id,
                       final StreamsConfig config,
                       final ProcessorStateManager stateMgr,
                       final StreamsMetricsImpl metrics) {
        super(
            id,
            config,
            metrics,
            stateMgr,
            new ThreadCache(
                new LogContext(String.format("stream-thread [%s] ", Thread.currentThread().getName())),
                0,
                metrics
            )
        );
    }


    StateManager getStateMgr() {
        return stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        // return null collector specifically since in standby task it should not be called;
        // if ever then we would throw NPE, which should never happen
        return null;
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public StateStore getStateStore(final String name) {
        throw new UnsupportedOperationException("this should not happen: getStateStore() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public String topic() {
        throw new UnsupportedOperationException("this should not happen: topic() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public int partition() {
        throw new UnsupportedOperationException("this should not happen: partition() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public long offset() {
        throw new UnsupportedOperationException("this should not happen: offset() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public long timestamp() {
        throw new UnsupportedOperationException("this should not happen: timestamp() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public <K, V> void forward(final K key, final V value) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(final K key, final V value, final String childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("this should not happen: commit() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) throws IllegalArgumentException {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ProcessorRecordContext recordContext() {
        throw new UnsupportedOperationException("this should not happen: recordContext not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        throw new UnsupportedOperationException("this should not happen: setRecordContext not supported in standby tasks.");
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        // no-op. can't throw as this is called on commit when the StateStores get flushed.
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ProcessorNode currentNode() {
        throw new UnsupportedOperationException("this should not happen: currentNode not supported in standby tasks.");
    }
}
