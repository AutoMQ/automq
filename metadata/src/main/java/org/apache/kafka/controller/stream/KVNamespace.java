/*
 * Copyright 2026, AutoMQ HK Limited.
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Namespace key for AutoMQ KV entries. A null namespace represents legacy v0 KV records.
 */
public class KVNamespace {
    private static final KVNamespace LEGACY = new KVNamespace(null);
    private static final Map<String, KVNamespace> NAMESPACES = new ConcurrentHashMap<>();

    private final String namespace;

    private KVNamespace(String namespace) {
        this.namespace = namespace;
    }

    public static KVNamespace of(String namespace) {
        if (namespace == null) {
            return LEGACY;
        }
        return NAMESPACES.computeIfAbsent(namespace, KVNamespace::new);
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KVNamespace)) return false;
        KVNamespace other = (KVNamespace) o;
        return Objects.equals(namespace, other.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace);
    }

    @Override
    public String toString() {
        return namespace == null ? "<default>" : namespace;
    }
}
