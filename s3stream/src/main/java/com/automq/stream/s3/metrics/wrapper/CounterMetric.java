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
import io.opentelemetry.api.metrics.LongCounter;

public class CounterMetric extends ConfigurableMetrics {
    private final LongCounter longCounter;

    public CounterMetric(MetricsConfig metricsConfig, LongCounter longCounter) {
        super(metricsConfig, Attributes.empty());
        this.longCounter = longCounter;
    }

    public CounterMetric(MetricsConfig metricsConfig, Attributes extraAttributes, LongCounter longCounter) {
        super(metricsConfig, extraAttributes);
        this.longCounter = longCounter;
    }

    public void add(MetricsLevel metricsLevel, long value) {
        if (metricsLevel.isWithin(metricsLevel)) {
            longCounter.add(value, attributes);
        }
    }
}
