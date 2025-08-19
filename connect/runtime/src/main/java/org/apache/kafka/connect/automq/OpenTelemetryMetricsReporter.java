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

package org.apache.kafka.connect.automq;

import com.automq.opentelemetry.AutoMQTelemetryManager;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A MetricsReporter implementation that bridges Kafka Connect metrics to OpenTelemetry.
 * 
 * <p>This reporter integrates with the AutoMQ OpenTelemetry module to export Kafka Connect
 * metrics through various exporters (Prometheus, OTLP, etc.). It automatically converts
 * Kafka metrics to OpenTelemetry instruments based on metric types and provides proper
 * labeling and naming conventions.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Automatic metric type detection and conversion</li>
 *   <li>Support for gauges and counters</li>
 *   <li>Proper attribute mapping from Kafka metric tags</li>
 *   <li>Integration with AutoMQ telemetry infrastructure</li>
 *   <li>Configurable metric filtering</li>
 * </ul>
 * 
 * <p>Configuration options:
 * <ul>
 *   <li>{@code opentelemetry.metrics.enabled} - Enable/disable OpenTelemetry metrics (default: true)</li>
 *   <li>{@code opentelemetry.metrics.prefix} - Prefix for metric names (default: "kafka.connect")</li>
 *   <li>{@code opentelemetry.metrics.include.pattern} - Regex pattern for included metrics</li>
 *   <li>{@code opentelemetry.metrics.exclude.pattern} - Regex pattern for excluded metrics</li>
 * </ul>
 */
public class OpenTelemetryMetricsReporter implements MetricsReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryMetricsReporter.class);
    
    private static final String ENABLED_CONFIG = "opentelemetry.metrics.enabled";
    private static final String PREFIX_CONFIG = "opentelemetry.metrics.prefix";
    private static final String INCLUDE_PATTERN_CONFIG = "opentelemetry.metrics.include.pattern";
    private static final String EXCLUDE_PATTERN_CONFIG = "opentelemetry.metrics.exclude.pattern";
    
    private static final String DEFAULT_PREFIX = "kafka.connect";
    
    private boolean enabled = true;
    private String metricPrefix = DEFAULT_PREFIX;
    private String includePattern = null;
    private String excludePattern = null;
    
    private Meter meter;
    private final Map<String, DoubleGauge> gauges = new ConcurrentHashMap<>();
    private final Map<String, LongCounter> counters = new ConcurrentHashMap<>();
    private final Map<String, Double> lastValues = new ConcurrentHashMap<>();
    
    @Override
    public void configure(Map<String, ?> configs) {
        // Parse configuration
        Object enabledObj = configs.get(ENABLED_CONFIG);
        if (enabledObj != null) {
            enabled = Boolean.parseBoolean(enabledObj.toString());
        }
        
        Object prefixObj = configs.get(PREFIX_CONFIG);
        if (prefixObj != null) {
            metricPrefix = prefixObj.toString();
        }
        
        Object includeObj = configs.get(INCLUDE_PATTERN_CONFIG);
        if (includeObj != null) {
            includePattern = includeObj.toString();
        }
        
        Object excludeObj = configs.get(EXCLUDE_PATTERN_CONFIG);
        if (excludeObj != null) {
            excludePattern = excludeObj.toString();
        }
        
        LOGGER.info("OpenTelemetryMetricsReporter configured - enabled: {}, prefix: {}, include: {}, exclude: {}", 
                   enabled, metricPrefix, includePattern, excludePattern);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        if (!enabled) {
            LOGGER.info("OpenTelemetryMetricsReporter is disabled");
            return;
        }
        
        try {
            // Get the OpenTelemetry meter from AutoMQTelemetryManager
            // This assumes the telemetry manager is already initialized
            meter = AutoMQTelemetryManager.getInstance().getMeter();
            if (meter == null) {
                LOGGER.warn("AutoMQTelemetryManager is not initialized, OpenTelemetry metrics will not be available");
                enabled = false;
                return;
            }
            
            // Register initial metrics
            for (KafkaMetric metric : metrics) {
                registerMetric(metric);
            }
            
            LOGGER.info("OpenTelemetryMetricsReporter initialized with {} metrics", metrics.size());
        } catch (Exception e) {
            LOGGER.error("Failed to initialize OpenTelemetryMetricsReporter", e);
            enabled = false;
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (!enabled || meter == null) {
            return;
        }
        
        try {
            registerMetric(metric);
        } catch (Exception e) {
            LOGGER.warn("Failed to register metric change for {}", metric.metricName(), e);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        if (!enabled) {
            return;
        }
        
        try {
            String metricKey = buildMetricKey(metric.metricName());
            gauges.remove(metricKey);
            counters.remove(metricKey);
            lastValues.remove(metricKey);
            LOGGER.debug("Removed metric: {}", metricKey);
        } catch (Exception e) {
            LOGGER.warn("Failed to remove metric {}", metric.metricName(), e);
        }
    }

    @Override
    public void close() {
        LOGGER.info("OpenTelemetryMetricsReporter closed");
    }
    
    private void registerMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        String metricKey = buildMetricKey(metricName);
        
        // Apply filtering
        if (!shouldIncludeMetric(metricKey)) {
            return;
        }
        
        Object value = metric.metricValue();
        if (!(value instanceof Number)) {
            LOGGER.debug("Skipping non-numeric metric: {}", metricKey);
            return;
        }
        
        double numericValue = ((Number) value).doubleValue();
        Attributes attributes = buildAttributes(metricName);
        
        // Determine metric type and register accordingly
        if (isCounterMetric(metricName)) {
            registerCounter(metricKey, metricName, numericValue, attributes);
        } else {
            registerGauge(metricKey, metricName, numericValue, attributes);
        }
    }
    
    private void registerGauge(String metricKey, MetricName metricName, double value, Attributes attributes) {
        DoubleGauge gauge = gauges.computeIfAbsent(metricKey, k -> {
            String description = buildDescription(metricName);
            String unit = determineUnit(metricName);
            return meter.gaugeBuilder(metricKey)
                       .setDescription(description)
                       .setUnit(unit)
                       .build();
        });
        
        // Record the value
        gauge.set(value, attributes);
        lastValues.put(metricKey, value);
        LOGGER.debug("Updated gauge {} = {}", metricKey, value);
    }
    
    private void registerCounter(String metricKey, MetricName metricName, double value, Attributes attributes) {
        LongCounter counter = counters.computeIfAbsent(metricKey, k -> {
            String description = buildDescription(metricName);
            String unit = determineUnit(metricName);
            return meter.counterBuilder(metricKey)
                       .setDescription(description)
                       .setUnit(unit)
                       .build();
        });
        
        // For counters, we need to track delta values
        Double lastValue = lastValues.get(metricKey);
        if (lastValue != null) {
            double delta = value - lastValue;
            if (delta > 0) {
                counter.add((long) delta, attributes);
                LOGGER.debug("Counter {} increased by {}", metricKey, delta);
            }
        }
        lastValues.put(metricKey, value);
    }
    
    private String buildMetricKey(MetricName metricName) {
        StringBuilder sb = new StringBuilder(metricPrefix);
        sb.append(".");
        
        // Add group if present
        if (metricName.group() != null && !metricName.group().isEmpty()) {
            sb.append(metricName.group().replace("-", "_").toLowerCase());
            sb.append(".");
        }
        
        // Add name
        sb.append(metricName.name().replace("-", "_").toLowerCase());
        
        return sb.toString();
    }
    
    private Attributes buildAttributes(MetricName metricName) {
        AttributesBuilder builder = Attributes.builder();
        
        // Add metric tags as attributes
        Map<String, String> tags = metricName.tags();
        if (tags != null) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && value != null) {
                    builder.put(sanitizeAttributeKey(key), value);
                }
            }
        }
        
        // Add standard attributes
        if (metricName.group() != null) {
            builder.put("metric.group", metricName.group());
        }
        
        return builder.build();
    }
    
    private String sanitizeAttributeKey(String key) {
        // Replace invalid characters for attribute keys
        return key.replace("-", "_").replace(".", "_").toLowerCase();
    }
    
    private String buildDescription(MetricName metricName) {
        StringBuilder description = new StringBuilder();
        description.append("Kafka Connect metric: ");
        
        if (metricName.group() != null) {
            description.append(metricName.group()).append(" - ");
        }
        
        description.append(metricName.name());
        
        return description.toString();
    }
    
    private String determineUnit(MetricName metricName) {
        String name = metricName.name().toLowerCase();
        
        if (name.contains("time") || name.contains("latency") || name.contains("duration")) {
            if (name.contains("ms") || name.contains("millisecond")) {
                return "ms";
            } else if (name.contains("ns") || name.contains("nanosecond")) {
                return "ns";
            } else {
                return "s";
            }
        } else if (name.contains("byte") || name.contains("size")) {
            return "bytes";
        } else if (name.contains("rate") || name.contains("per-sec")) {
            return "1/s";
        } else if (name.contains("percent") || name.contains("ratio")) {
            return "%";
        } else if (name.contains("count") || name.contains("total")) {
            return "1";
        }
        
        return "1"; // Default unit
    }
    
    private boolean isCounterMetric(MetricName metricName) {
        String name = metricName.name().toLowerCase();
        String group = metricName.group() != null ? metricName.group().toLowerCase() : "";
        
        // Identify counter-like metrics
        return name.contains("total") || 
               name.contains("count") ||
               name.contains("error") ||
               name.contains("failure") ||
               name.endsWith("-total") ||
               group.contains("error");
    }
    
    private boolean shouldIncludeMetric(String metricKey) {
        // Apply exclude pattern first
        if (excludePattern != null && metricKey.matches(excludePattern)) {
            return false;
        }
        
        // Apply include pattern if specified
        if (includePattern != null) {
            return metricKey.matches(includePattern);
        }
        
        return true;
    }
}
