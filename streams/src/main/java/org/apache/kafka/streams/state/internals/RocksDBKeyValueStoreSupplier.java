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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

/**
 * A {@link org.apache.kafka.streams.state.KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
@Deprecated
public class RocksDBKeyValueStoreSupplier<K, V> extends AbstractStoreSupplier<K, V, KeyValueStore> {

    private final KeyValueStoreBuilder<K, V> builder;

    public RocksDBKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, boolean logged, Map<String, String> logConfig, boolean cached) {
        this(name, keySerde, valueSerde, Time.SYSTEM, logged, logConfig, cached);
    }

    public RocksDBKeyValueStoreSupplier(String name, Serde<K> keySerde, Serde<V> valueSerde, Time time, boolean logged, Map<String, String> logConfig, boolean cached) {
        super(name, keySerde, valueSerde, time, logged, logConfig);
        builder = new KeyValueStoreBuilder<>(new RocksDbKeyValueBytesStoreSupplier(name),
                                             keySerde,
                                             valueSerde,
                                             time);
        if (cached) {
            builder.withCachingEnabled();
        }
        // logged by default so we only need to worry about when it is disabled.
        if (!logged) {
            builder.withLoggingDisabled();
        }
    }

    public KeyValueStore get() {
        return builder.build();
    }


}
