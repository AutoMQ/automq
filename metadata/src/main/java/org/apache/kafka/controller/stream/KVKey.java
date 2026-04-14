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

package org.apache.kafka.controller.stream;

import java.util.Objects;

/**
 * Composite key for KV store entries, combining an optional namespace with a key.
 * A null namespace represents legacy (v0) records with no namespace.
 */
public class KVKey {
    private final String namespace;
    private final String key;

    private KVKey(String namespace, String key) {
        this.namespace = namespace;
        this.key = key;
    }

    public static KVKey of(String key) {
        return new KVKey(null, key);
    }

    public static KVKey of(String namespace, String key) {
        return new KVKey(namespace, key);
    }

    public String namespace() {
        return namespace;
    }

    public String key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KVKey)) return false;
        KVKey other = (KVKey) o;
        return Objects.equals(namespace, other.namespace) && Objects.equals(key, other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, key);
    }

    @Override
    public String toString() {
        return namespace == null ? key : namespace + "/" + key;
    }
}
