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

package com.automq.stream.s3.metrics;

import io.opentelemetry.api.common.Attributes;

public class MetricsConfig {
    private MetricsLevel metricsLevel;
    private Attributes baseAttributes;

    public MetricsConfig() {
        this.metricsLevel = MetricsLevel.INFO;
        this.baseAttributes = Attributes.empty();
    }

    public MetricsConfig(MetricsLevel metricsLevel, Attributes baseAttributes) {
        this.metricsLevel = metricsLevel;
        this.baseAttributes = baseAttributes;
    }

    public MetricsLevel getMetricsLevel() {
        return metricsLevel;
    }

    public Attributes getBaseAttributes() {
        return baseAttributes;
    }

    public void setMetricsLevel(MetricsLevel metricsLevel) {
        this.metricsLevel = metricsLevel;
    }

    public void setBaseAttributes(Attributes baseAttributes) {
        this.baseAttributes = baseAttributes;
    }
}
