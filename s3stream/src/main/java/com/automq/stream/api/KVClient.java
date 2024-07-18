/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.api;

import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;
import java.util.concurrent.CompletableFuture;

/**
 * Light KV client, support light & simple kv operations.
 */
public interface KVClient {
    /**
     * Put key value if key not exist, return current key value after putting.
     *
     * @param keyValue {@link KeyValue} k-v pair
     * @return async put result. {@link Value} current value after putting.
     */
    CompletableFuture<Value> putKVIfAbsent(KeyValue keyValue);

    /**
     * Put key value, overwrite if key exist, return current key value after putting.
     *
     * @param keyValue {@link KeyValue} k-v pair
     * @return async put result. {@link KeyValue} current value after putting.
     */
    CompletableFuture<Value> putKV(KeyValue keyValue);

    /**
     * Get value by key.
     *
     * @param key key.
     * @return async get result. {@link KeyValue} k-v pair, null if key not exist.
     */
    CompletableFuture<Value> getKV(Key key);

    /**
     * Delete key value by key. If key not exist, return null.
     *
     * @param key key.
     * @return async delete result. {@link Value} deleted value, null if key not exist.
     */
    CompletableFuture<Value> delKV(Key key);
}
