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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class InMemoryKeyValueLoggedStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context) {
        final StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("my-store"),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde())
                .withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

        final StateStore store = storeBuilder.build();
        store.init(context, store);

        return (KeyValueStore<K, V>) store;
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Integer, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(1, "1"));
        entries.add(new KeyValue<>(2, "2"));
        store.putAll(entries);
        assertEquals(store.get(1), "1");
        assertEquals(store.get(2), "2");
    }
}
