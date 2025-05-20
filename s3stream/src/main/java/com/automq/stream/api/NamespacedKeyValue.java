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

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A wrapper around KeyValue that adds namespace and epoch support.
 * This is an optional enhancement that can be used when namespace isolation
 * and versioning is needed.
 */
public class NamespacedKeyValue {
    private final KeyValue keyValue;
    private final String namespace;
    private final long epoch;

    private NamespacedKeyValue(KeyValue keyValue, String namespace, long epoch) {
        this.keyValue = Objects.requireNonNull(keyValue, "keyValue cannot be null");
        this.namespace = Objects.requireNonNull(namespace, "namespace cannot be null");
        this.epoch = epoch;
    }

    public static NamespacedKeyValue of(String key, ByteBuffer value, String namespace, long epoch) {
        return new NamespacedKeyValue(KeyValue.of(key, value), namespace, epoch);
    }

    public static NamespacedKeyValue of(KeyValue keyValue, String namespace, long epoch) {
        return new NamespacedKeyValue(keyValue, namespace, epoch);
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }

    public String getNamespace() {
        return namespace;
    }

    public long getEpoch() {
        return epoch;
    }

    /**
     * Creates a composite key that includes the namespace.
     * This allows the underlying KV store to maintain namespace isolation.
     */
    public String getNamespacedKey() {
        return namespace + ":" + keyValue.key().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NamespacedKeyValue)) return false;
        NamespacedKeyValue that = (NamespacedKeyValue) o;
        return epoch == that.epoch &&
               Objects.equals(keyValue, that.keyValue) &&
               Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyValue, namespace, epoch);
    }

    @Override
    public String toString() {
        return "NamespacedKeyValue{" +
               "keyValue=" + keyValue +
               ", namespace='" + namespace + '\'' +
               ", epoch=" + epoch +
               '}';
    }
} 