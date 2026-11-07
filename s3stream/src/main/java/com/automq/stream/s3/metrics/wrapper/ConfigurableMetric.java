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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;

import io.opentelemetry.api.common.Attributes;

public class ConfigurableMetric implements ConfigListener {
    private final Attributes extraAttributes;
    Attributes attributes;
    MetricsLevel metricsLevel;

    public ConfigurableMetric(MetricsConfig metricsConfig, Attributes extraAttributes) {
        this.metricsLevel = metricsConfig.getMetricsLevel();
        this.extraAttributes = extraAttributes;
        this.attributes = buildAttributes(metricsConfig.getBaseAttributes());
    }

    private Attributes buildAttributes(Attributes baseAttributes) {
        return Attributes.builder()
            .putAll(baseAttributes)
            .putAll(this.extraAttributes).build();
    }

    @Override
    public void onConfigChange(MetricsConfig metricsConfig) {
        this.metricsLevel = metricsConfig.getMetricsLevel();
        this.attributes = buildAttributes(metricsConfig.getBaseAttributes());
    }
}
