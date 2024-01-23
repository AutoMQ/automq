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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;

public class HistogramMetric extends ConfigurableMetrics {
    private final LongHistogram longHistogram;

    public HistogramMetric(MetricsConfig metricsConfig, LongHistogram longHistogram) {
        this(metricsConfig, Attributes.empty(), longHistogram);
    }

    public HistogramMetric(MetricsConfig metricsConfig, Attributes extraAttributes, LongHistogram longHistogram) {
        super(metricsConfig, extraAttributes);
        this.longHistogram = longHistogram;
    }

    public void record(MetricsLevel metricsLevel, long value) {
        if (metricsLevel.isWithin(metricsLevel)) {
            longHistogram.record(value, attributes);
        }
    }
}
