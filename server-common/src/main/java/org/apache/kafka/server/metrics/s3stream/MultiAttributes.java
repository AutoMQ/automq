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

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
