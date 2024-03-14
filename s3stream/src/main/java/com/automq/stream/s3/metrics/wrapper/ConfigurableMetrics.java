/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import io.opentelemetry.api.common.Attributes;

public class ConfigurableMetrics implements ConfigListener {
    private final Attributes extraAttributes;
    Attributes attributes;
    MetricsLevel metricsLevel;

    public ConfigurableMetrics(MetricsConfig metricsConfig, Attributes extraAttributes) {
        this.metricsLevel = metricsConfig.getMetricsLevel();
        this.extraAttributes = extraAttributes;
        this.attributes = buildAttributes(metricsConfig.getBaseAttributes());
    }

    Attributes buildAttributes(Attributes baseAttributes) {
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
