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
package org.apache.kafka.test;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.HashMap;
import java.util.Map;

public class MockRocksDbConfigSetter implements RocksDBConfigSetter {
    public static boolean called = false;
    public static Map<String, Object> configMap = new HashMap<>();

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        called = true;

        configMap.putAll(configs);
    }

    @Override
    public void close(String storeName, Options options) {
    }
}
