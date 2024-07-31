/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
