/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.api;

import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of NamespacedKVClient that wraps an existing KVClient.
 * This implementation maintains namespace isolation by prefixing keys with the namespace
 * and stores epoch information in a special key within each namespace.
 */
public class DefaultNamespacedKVClient implements NamespacedKVClient {
    private static final String EPOCH_KEY_PREFIX = "__epoch__:";
    private final KVClient delegate;

    public DefaultNamespacedKVClient(KVClient delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate KVClient cannot be null");
    }

    @Override
    public CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue) {
        return delegate.putKVIfAbsent(keyValue);
    }

    @Override
    public CompletableFuture<Value> putKV(KeyValue keyValue) {
        return delegate.putKV(keyValue);
    }

    @Override
    public CompletableFuture<Value> getKV(Key key) {
        return delegate.getKV(key);
    }

    @Override
    public CompletableFuture<Value> delKV(Key key) {
        return delegate.delKV(key);
    }

    @Override
    public CompletableFuture<Value> putNamespacedKVIfAbsent(NamespacedKeyValue namespacedKeyValue) {
        String namespacedKey = namespacedKeyValue.getNamespacedKey();
        KeyValue kv = KeyValue.of(namespacedKey, namespacedKeyValue.getKeyValue().value().get());
        return delegate.putKVIfAbsent(kv);
    }

    @Override
    public CompletableFuture<Value> putNamespacedKV(NamespacedKeyValue namespacedKeyValue) {
        String namespacedKey = namespacedKeyValue.getNamespacedKey();
        KeyValue kv = KeyValue.of(namespacedKey, namespacedKeyValue.getKeyValue().value().get());
        return delegate.putKV(kv);
    }

    @Override
    public CompletableFuture<Value> getNamespacedKV(String namespace, Key key) {
        String namespacedKey = namespace + ":" + key.get();
        return delegate.getKV(Key.of(namespacedKey));
    }

    @Override
    public CompletableFuture<Value> delNamespacedKV(String namespace, Key key) {
        String namespacedKey = namespace + ":" + key.get();
        return delegate.delKV(Key.of(namespacedKey));
    }

    @Override
    public CompletableFuture<Long> getNamespaceEpoch(String namespace) {
        String epochKey = EPOCH_KEY_PREFIX + namespace;
        return delegate.getKV(Key.of(epochKey))
            .thenApply(value -> {
                if (value == null || value.isNull()) {
                    return 0L;
                }
                ByteBuffer buffer = value.get();
                return buffer.getLong();
            });
    }

    @Override
    public CompletableFuture<Long> incrementNamespaceEpoch(String namespace) {
        return getNamespaceEpoch(namespace)
            .thenCompose(currentEpoch -> {
                long newEpoch = currentEpoch + 1;
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(newEpoch);
                buffer.flip();
                
                String epochKey = EPOCH_KEY_PREFIX + namespace;
                KeyValue epochKV = KeyValue.of(epochKey, buffer);
                
                return delegate.putKV(epochKV)
                    .thenApply(value -> newEpoch);
            });
    }
} 