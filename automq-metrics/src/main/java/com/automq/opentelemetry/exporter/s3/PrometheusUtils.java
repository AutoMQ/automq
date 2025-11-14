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

import java.util.Locale;

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
        
        // special case - gauge with intelligent Connect metric handling
        if ("1".equals(unit) && isGauge && !name.contains("ratio")) {
            if (isConnectMetric(name)) {
                // For Connect metrics, use improved logic to avoid misleading _ratio suffix
                if (shouldAddRatioSuffixForConnect(name)) {
                    name = name + "_ratio";
                }
            } else {
                // For other metrics, maintain original behavior
                name = name + "_ratio";
            }
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

    /**
     * Check if a metric name is related to Kafka Connect.
     *
     * @param name The metric name to check.
     * @return true if it's a Connect metric, false otherwise.
     */
    private static boolean isConnectMetric(String name) {
        String lowerName = name.toLowerCase(Locale.ROOT);
        return lowerName.contains("kafka_connector_") || 
               lowerName.contains("kafka_task_") || 
               lowerName.contains("kafka_worker_") ||
               lowerName.contains("kafka_connect_") ||
               lowerName.contains("kafka_source_task_") ||
               lowerName.contains("kafka_sink_task_") ||
               lowerName.contains("connector_metrics") ||
               lowerName.contains("task_metrics") ||
               lowerName.contains("worker_metrics") ||
               lowerName.contains("source_task_metrics") ||
               lowerName.contains("sink_task_metrics");
    }

    /**
     * Intelligently determine if a Connect metric should have a _ratio suffix.
     * This method avoids adding misleading _ratio suffixes to count-based metrics.
     *
     * @param name The metric name to check.
     * @return true if _ratio suffix should be added, false otherwise.
     */
    private static boolean shouldAddRatioSuffixForConnect(String name) {
        String lowerName = name.toLowerCase(Locale.ROOT);
        
        if (hasRatioRelatedWords(lowerName)) {
            return false;
        }
        
        if (isCountMetric(lowerName)) {
            return false;
        }
        
        return isRatioMetric(lowerName);
    }
    
    private static boolean hasRatioRelatedWords(String lowerName) {
        return lowerName.contains("ratio") || lowerName.contains("percent") || 
               lowerName.contains("rate") || lowerName.contains("fraction");
    }
    
    private static boolean isCountMetric(String lowerName) {
        return hasBasicCountKeywords(lowerName) || hasConnectCountKeywords(lowerName) ||
               hasStatusCountKeywords(lowerName);
    }
    
    private static boolean hasBasicCountKeywords(String lowerName) {
        return lowerName.contains("count") || lowerName.contains("num") || 
               lowerName.contains("size") || lowerName.contains("total") ||
               lowerName.contains("active") || lowerName.contains("current");
    }
    
    private static boolean hasConnectCountKeywords(String lowerName) {
        return lowerName.contains("partition") || lowerName.contains("task") ||
               lowerName.contains("connector") || lowerName.contains("seq_no") ||
               lowerName.contains("seq_num") || lowerName.contains("attempts");
    }
    
    private static boolean hasStatusCountKeywords(String lowerName) {
        return lowerName.contains("success") || lowerName.contains("failure") ||
               lowerName.contains("errors") || lowerName.contains("retries") ||
               lowerName.contains("skipped") || lowerName.contains("running") ||
               lowerName.contains("paused") || lowerName.contains("failed") ||
               lowerName.contains("destroyed");
    }
    
    private static boolean isRatioMetric(String lowerName) {
        return lowerName.contains("utilization") || 
               lowerName.contains("usage") ||
               lowerName.contains("load") ||
               lowerName.contains("efficiency") ||
               lowerName.contains("hit_rate") ||
               lowerName.contains("miss_rate");
    }
}
