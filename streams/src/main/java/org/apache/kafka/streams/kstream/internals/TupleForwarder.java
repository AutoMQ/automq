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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * This class is used to determine if a processor should forward values to child nodes.
 * Forwarding by this class only occurs when caching is not enabled. If caching is enabled,
 * forwarding occurs in the flush listener when the cached store flushes.
 *
 * @param <K>
 * @param <V>
 */
class TupleForwarder<K, V> {
    private final CachedStateStore cachedStateStore;
    private final ProcessorContext context;
    private final boolean sendOldValues;

    @SuppressWarnings("unchecked")
    TupleForwarder(final StateStore store,
                   final ProcessorContext context,
                   final ForwardingCacheFlushListener flushListener,
                   final boolean sendOldValues) {
        this.cachedStateStore = cachedStateStore(store);
        this.context = context;
        this.sendOldValues = sendOldValues;
        if (this.cachedStateStore != null) {
            cachedStateStore.setFlushListener(flushListener, sendOldValues);
        }
    }

    private CachedStateStore cachedStateStore(final StateStore store) {
        if (store instanceof CachedStateStore) {
            return (CachedStateStore) store;
        } else if (store instanceof WrappedStateStore
                && ((WrappedStateStore) store).wrappedStore() instanceof CachedStateStore) {
            return (CachedStateStore) ((WrappedStateStore) store).wrappedStore();
        }
        return null;
    }

    public void maybeForward(final K key,
                             final V newValue,
                             final V oldValue) {
        if (cachedStateStore == null) {
            if (sendOldValues) {
                context.forward(key, new Change<>(newValue, oldValue));
            } else {
                context.forward(key, new Change<>(newValue, null));
            }
        }
    }
}
