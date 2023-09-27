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

package com.automq.stream.api;

import com.automq.stream.api.KeyValue.Key;
import com.automq.stream.api.KeyValue.Value;
import java.util.List;
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
