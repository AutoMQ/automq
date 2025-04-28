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

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

public class MultiAttributes<K> implements ConfigListener {
    private final Map<K, Attributes> attributesMap = new ConcurrentHashMap<>();
    private final AttributeKey<K> keyName;
    private Attributes baseAttributes;

    public MultiAttributes(Attributes baseAttributes, AttributeKey<K> keyName) {
        this.baseAttributes = baseAttributes;
        this.keyName = keyName;
    }

    public Attributes get(K key) {
        return attributesMap.computeIfAbsent(key, k -> buildAttributes(baseAttributes, Attributes.of(keyName, key)));
    }

    private Attributes buildAttributes(Attributes baseAttributes, Attributes attributes) {
        return Attributes.builder().putAll(baseAttributes).putAll(attributes).build();
    }

    private void reBuildAttributes(Attributes baseAttributes) {
        for (Map.Entry<K, Attributes> entry : attributesMap.entrySet()) {
            attributesMap.replace(entry.getKey(), buildAttributes(baseAttributes, entry.getValue()));
        }
    }

    @Override
    public void onConfigChange(MetricsConfig metricsConfig) {
        this.baseAttributes = metricsConfig.getBaseAttributes();
        reBuildAttributes(metricsConfig.getBaseAttributes());
    }
}
