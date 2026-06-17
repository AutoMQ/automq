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

package com.automq.opentelemetry.exporter.s3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusUtilsTest {

    @Test
    public void testPrometheusPullCompatibleReservedSuffixAndUnitRules() {
        assertMetricName("foo", "foo_total", "", false, true);
        assertMetricName("foo", "foo_info", "", false, true);
        assertMetricName("foo_bytes", "foo_bytes_total", "bytes", false, true);
        assertMetricName("foo_bytes_current_bytes", "foo_bytes_current", "bytes", false, true);
        assertMetricName("foo_bytes_total", "foo_total", "bytes", true, false);
        assertMetricName("foo_count_sum_count", "foo_count_sum", "count", false, true);
        assertMetricName("foo_ratio", "foo", "1", false, true);
        assertMetricName("foo_ratio_total", "foo", "1", true, false);
    }

    private static void assertMetricName(String expected, String name, String unit, boolean isCounter, boolean isGauge) {
        Assertions.assertEquals(expected, PrometheusUtils.mapMetricsName(name, unit, isCounter, isGauge));
    }
}
