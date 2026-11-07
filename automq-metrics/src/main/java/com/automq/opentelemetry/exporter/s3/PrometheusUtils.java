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

/**
 * Utility class for Prometheus metric and label naming.
 */
public class PrometheusUtils {
    private static final String TOTAL_SUFFIX = "_total";
    private static final String[] RESERVED_METRIC_NAME_SUFFIXES = {
        "_total", "_created", "_bucket", "_info"
    };

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
                return "ratio";
            case "%":
                return "percent";
            // Rate units (per second)
            case "1/s":
                return "per_second";
            case "By/s":
                return "bytes_per_second";
            case "KiBy/s":
                return "kibibytes_per_second";
            case "MiBy/s":
                return "mebibytes_per_second";
            case "GiBy/s":
                return "gibibytes_per_second";
            case "KBy/s":
                return "kilobytes_per_second";
            case "MBy/s":
                return "megabytes_per_second";
            case "GBy/s":
                return "gigabytes_per_second";
            // Rate units (per minute)
            case "1/min":
                return "per_minute";
            case "By/min":
                return "bytes_per_minute";
            // Rate units (per hour)
            case "1/h":
                return "per_hour";
            case "By/h":
                return "bytes_per_hour";
            // Rate units (per day)
            case "1/d":
                return "per_day";
            case "By/d":
                return "bytes_per_day";
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
        name = sanitizeMetricName(name);

        String prometheusUnit = getPrometheusUnit(unit);
        boolean shouldAppendUnit = !prometheusUnit.isBlank() && !name.endsWith(prometheusUnit);

        // append prometheus unit if not null or empty.
        // unit should be appended before type suffix
        if (shouldAppendUnit) {
            name = name + "_" + prometheusUnit;
        }

        if (isCounter) {
            name = name + TOTAL_SUFFIX;
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
        if (name == null || name.isBlank()) {
            return "";
        }
        return name.replaceAll("\\.", "_");
    }

    private static String sanitizeMetricName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Cannot convert an empty string to a valid metric name.");
        }
        String sanitizedName = name.replaceAll("[^a-zA-Z0-9_.:]", "_");
        if (!sanitizedName.substring(0, 1).matches("[a-zA-Z_.:]")) {
            sanitizedName = "_" + sanitizedName;
        }
        sanitizedName = sanitizedName.replaceAll("\\.", "_");
        boolean modified = true;
        while (modified) {
            modified = false;
            for (String reservedSuffix : RESERVED_METRIC_NAME_SUFFIXES) {
                if (sanitizedName.equals(reservedSuffix)) {
                    return reservedSuffix.substring(1);
                }
                if (sanitizedName.endsWith(reservedSuffix)) {
                    sanitizedName = sanitizedName.substring(0, sanitizedName.length() - reservedSuffix.length());
                    modified = true;
                }
            }
        }
        return sanitizedName;
    }
}
