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


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.NoOpWindowStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.StateStoreProviderStub;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.streams.state.QueryableStoreTypes.windowStore;
import static org.junit.Assert.assertEquals;

public class WrappingStoreProviderTest {

    private WrappingStoreProvider wrappingStoreProvider;

    @Before
    public void before() {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        final StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);


        stubProviderOne.addStore("kv", Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("kv"),
                Serdes.serdeFrom(String.class),
                Serdes.serdeFrom(String.class))
                .build());
        stubProviderOne.addStore("window", new NoOpWindowStore());
        stubProviderTwo.addStore("kv", Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("kv"),
                Serdes.serdeFrom(String.class),
                Serdes.serdeFrom(String.class))
                .build());
        stubProviderTwo.addStore("window", new NoOpWindowStore());

        wrappingStoreProvider = new WrappingStoreProvider(
                Arrays.<StateStoreProvider>asList(stubProviderOne, stubProviderTwo));
    }

    @Test
    public void shouldFindKeyValueStores() {
        List<ReadOnlyKeyValueStore<String, String>> results =
                wrappingStoreProvider.stores("kv", QueryableStoreTypes.<String, String>keyValueStore());
        assertEquals(2, results.size());
    }

    @Test
    public void shouldFindWindowStores() {
        final List<ReadOnlyWindowStore<Object, Object>>
                windowStores =
                wrappingStoreProvider.stores("window", windowStore());
        assertEquals(2, windowStores.size());
    }

    @Test(expected = InvalidStateStoreException.class)
    public void shouldThrowInvalidStoreExceptionIfNoStoreOfTypeFound() {
        wrappingStoreProvider.stores("doesn't exist", QueryableStoreTypes.keyValueStore());
    }
}