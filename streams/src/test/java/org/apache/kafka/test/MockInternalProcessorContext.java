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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

public class MockInternalProcessorContext extends MockProcessorContext implements InternalProcessorContext {

    private final Map<String, StateRestoreCallback> restoreCallbacks = new LinkedHashMap<>();
    private ProcessorNode currentNode;
    private RecordCollector recordCollector;
    private long currentSystemTimeMs;
    private TaskType taskType = TaskType.ACTIVE;

    public MockInternalProcessorContext() {
    }

    public MockInternalProcessorContext(final Properties config, final TaskId taskId, final File stateDir) {
        super(config, taskId, stateDir);
    }

    @Override
    public void setSystemTimeMs(long timeMs) {
        currentSystemTimeMs = timeMs;
    }

    @Override
    public long currentSystemTimeMs() {
        return currentSystemTimeMs;
    }

    @Override
    public StreamsMetricsImpl metrics() {
        return (StreamsMetricsImpl) super.metrics();
    }

    @Override
    public ProcessorRecordContext recordContext() {
        return new ProcessorRecordContext(timestamp(), offset(), partition(), topic(), headers());
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        setRecordMetadata(
            recordContext.topic(),
            recordContext.partition(),
            recordContext.offset(),
            recordContext.headers(),
            recordContext.timestamp()
        );
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode currentNode() {
        return currentNode;
    }

    @Override
    public ThreadCache cache() {
        return null;
    }

    @Override
    public void initialize() {}

    @Override
    public void uninitialize() {}

    @Override
    public RecordCollector recordCollector() {
        return recordCollector;
    }

    public void setRecordCollector(final RecordCollector recordCollector) {
        this.recordCollector = recordCollector;
    }

    @Override
    public void register(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        restoreCallbacks.put(store.name(), stateRestoreCallback);
        super.register(store, stateRestoreCallback);
    }

    public StateRestoreCallback stateRestoreCallback(final String storeName) {
        return restoreCallbacks.get(storeName);
    }

    @Override
    public TaskType taskType() {
        return taskType;
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp) {
    }

    @Override
    public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
    }

    @Override
    public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
    }
}