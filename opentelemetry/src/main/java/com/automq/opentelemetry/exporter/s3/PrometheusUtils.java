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

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for Prometheus metric and label naming.
 */
public class PrometheusUtils {
    private static final String TOTAL_SUFFIX = "_total";

    /**
     * Get the Prometheus unit from the OpenTelemetry unit.
     *
     * @param unit The OpenTelemetry unit.
     * @return The Prometheus unit.
     */
    public static String getPrometheusUnit(String unit) {
        if (unit.contains("{")) {
            return "";
        }
        switch (unit) {
            // Time
            case "d":
                return "days";
            case "h":
                return "hours";
            case "min":
                return "minutes";
            case "s":
                return "seconds";
            case "ms":
                return "milliseconds";
            case "us":
                return "microseconds";
            case "ns":
                return "nanoseconds";
            // Bytes
            case "By":
                return "bytes";
            case "KiBy":
                return "kibibytes";
            case "MiBy":
                return "mebibytes";
            case "GiBy":
                return "gibibytes";
            case "TiBy":
                return "tibibytes";
            case "KBy":
                return "kilobytes";
            case "MBy":
                return "megabytes";
            case "GBy":
                return "gigabytes";
            case "TBy":
                return "terabytes";
            // SI
            case "m":
                return "meters";
            case "V":
                return "volts";
            case "A":
                return "amperes";
            case "J":
                return "joules";
            case "W":
                return "watts";
            case "g":
                return "grams";
            // Misc
            case "Cel":
                return "celsius";
            case "Hz":
                return "hertz";
            case "1":
                return "";
            case "%":
                return "percent";
            default:
                return unit;
        }
    }

    /**
     * Map a metric name to a Prometheus-compatible name.
     *
     * @param name The original metric name.
     * @param unit The metric unit.
     * @param isCounter Whether the metric is a counter.
     * @param isGauge Whether the metric is a gauge.
     * @return The Prometheus-compatible metric name.
     */
    public static String mapMetricsName(String name, String unit, boolean isCounter, boolean isGauge) {
        // Replace "." into "_"
        name = name.replaceAll("\\.", "_");

        String prometheusUnit = getPrometheusUnit(unit);
        boolean shouldAppendUnit = StringUtils.isNotBlank(prometheusUnit) && !name.contains(prometheusUnit);

        // append prometheus unit if not null or empty.
        // unit should be appended before type suffix
        if (shouldAppendUnit) {
            name = name + "_" + prometheusUnit;
        }

        // trim counter's _total suffix so the unit is placed before it.
        if (isCounter && name.endsWith(TOTAL_SUFFIX)) {
            name = name.substring(0, name.length() - TOTAL_SUFFIX.length());
        }

        // replace _total suffix, or add if it wasn't already present.
        if (isCounter) {
            name = name + TOTAL_SUFFIX;
        }
        // special case - gauge
        if (unit.equals("1") && isGauge && !name.contains("ratio")) {
            name = name + "_ratio";
        }
        return name;
    }

    /**
     * Map a label name to a Prometheus-compatible name.
     *
     * @param name The original label name.
     * @return The Prometheus-compatible label name.
     */
    public static String mapLabelName(String name) {
        if (StringUtils.isBlank(name)) {
            return "";
        }
        return name.replaceAll("\\.", "_");
    }
}
