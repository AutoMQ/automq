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

package kafka.log.es.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Light KV client, support light & simple kv operations.
 */
public interface KVClient {
    /**
     * Put key value.
     *
     * @param keyValues {@link KeyValue} list.
     * @return async put result.
     */
    CompletableFuture<Void> putKV(List<KeyValue> keyValues);

    /**
     * Get value by key.
     *
     * @param keys key list.
     * @return {@link KeyValue} list.
     */
    CompletableFuture<List<KeyValue>> getKV(List<String> keys);

    /**
     * Delete key value by key.
     *
     * @param keys key list.
     * @return async delete result.
     */
    CompletableFuture<Void> delKV(List<String> keys);
}
