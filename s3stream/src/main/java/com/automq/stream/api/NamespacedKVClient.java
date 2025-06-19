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

import com.automq.stream.api.KeyValue.Value;

import java.util.concurrent.CompletableFuture;

/**
 * Enhanced KV client that supports namespace isolation and epoch-based versioning.
 * This is an optional interface that applications can use when they need namespace
 * and versioning support.
 */
public interface NamespacedKVClient extends KVClient {
    /**
     * Put namespaced key value if key not exist in the namespace.
     *
     * @param namespacedKeyValue The namespaced key-value pair
     * @return async put result with the current value after putting
     */
    CompletableFuture<Value> putNamespacedKVIfAbsent(NamespacedKeyValue namespacedKeyValue);

    /**
     * Put namespaced key value, overwrite if key exists in the namespace.
     *
     * @param namespacedKeyValue The namespaced key-value pair
     * @return async put result with the current value after putting
     */
    CompletableFuture<Value> putNamespacedKV(NamespacedKeyValue namespacedKeyValue);

    /**
     * Get value by namespaced key.
     *
     * @param namespace The namespace to look in
     * @param key The key to look up
     * @return async get result with the value, null if key not exist
     */
    CompletableFuture<Value> getNamespacedKV(String namespace, KeyValue.Key key);

    /**
     * Delete key value by namespaced key.
     *
     * @param namespace The namespace to delete from
     * @param key The key to delete
     * @return async delete result with the deleted value, null if key not exist
     */
    CompletableFuture<Value> delNamespacedKV(String namespace, KeyValue.Key key);

    /**
     * Get the current epoch for a namespace.
     *
     * @param namespace The namespace to get the epoch for
     * @return async result with the current epoch
     */
    CompletableFuture<Long> getNamespaceEpoch(String namespace);

    /**
     * Increment the epoch for a namespace.
     *
     * @param namespace The namespace to increment the epoch for
     * @return async result with the new epoch value
     */
    CompletableFuture<Long> incrementNamespaceEpoch(String namespace);
} 