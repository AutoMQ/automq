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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import com.automq.opentelemetry.AutoMQTelemetryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.Meter;

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
 *   <li>Support for gauges and counters using async observable instruments</li>
 *   <li>Proper attribute mapping from Kafka metric tags</li>
 *   <li>Integration with AutoMQ telemetry infrastructure</li>
 *   <li>Configurable metric filtering</li>
 *   <li>Real-time metric value updates through callbacks</li>
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
    private final Map<String, AutoCloseable> observableHandles = new ConcurrentHashMap<>();
    private final Map<String, KafkaMetric> registeredMetrics = new ConcurrentHashMap<>();
    
    public static void initializeTelemetry(Properties props) {
        AutoMQTelemetryManager.initializeInstance(props);
        LOGGER.info("OpenTelemetryMetricsReporter initialized");
    }
    
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
            closeHandle(metricKey);
            registeredMetrics.remove(metricKey);
            LOGGER.debug("Removed metric: {}", metricKey);
        } catch (Exception e) {
            LOGGER.warn("Failed to remove metric {}", metric.metricName(), e);
        }
    }

    @Override
    public void close() {
        if (enabled) {
            // Close all observable handles to prevent memory leaks
            observableHandles.values().forEach(handle -> {
                try {
                    handle.close();
                } catch (Exception e) {
                    LOGGER.debug("Error closing observable handle", e);
                }
            });
            observableHandles.clear();
            registeredMetrics.clear();
        }
        LOGGER.info("OpenTelemetryMetricsReporter closed");
    }
    
    private void registerMetric(KafkaMetric metric) {
        LOGGER.debug("OpenTelemetryMetricsReporter registering metric {}", metric.metricName());
        MetricName metricName = metric.metricName();
        String metricKey = buildMetricKey(metricName);
        
        // Apply filtering
        if (!shouldIncludeMetric(metricKey)) {
            return;
        }
        
        // Check if metric value is numeric at registration time
        Object testValue = safeMetricValue(metric);
        if (!(testValue instanceof Number)) {
            LOGGER.debug("Skipping non-numeric metric: {}", metricKey);
            return;
        }
        
        Attributes attributes = buildAttributes(metricName);
        
        // Close existing handle if present (for metric updates)
        closeHandle(metricKey);
        
        // Register the metric for future access
        registeredMetrics.put(metricKey, metric);
        
        // Determine metric type and register accordingly
        if (isCounterMetric(metricName)) {
            registerAsyncCounter(metricKey, metricName, metric, attributes, (Number) testValue);
        } else {
            registerAsyncGauge(metricKey, metricName, metric, attributes);
        }
    }
    
    private void registerAsyncGauge(String metricKey, MetricName metricName, KafkaMetric metric, Attributes attributes) {
        try {
            String description = buildDescription(metricName);
            String unit = determineUnit(metricName);
            
            ObservableDoubleGauge gauge = meter.gaugeBuilder(metricKey)
                .setDescription(description)
                .setUnit(unit)
                .buildWithCallback(measurement -> {
                    Number value = (Number) safeMetricValue(metric);
                    if (value != null) {
                        measurement.record(value.doubleValue(), attributes);
                    }
                });
            
            observableHandles.put(metricKey, gauge);
            LOGGER.debug("Registered async gauge: {}", metricKey);
        } catch (Exception e) {
            LOGGER.warn("Failed to register async gauge for {}", metricKey, e);
        }
    }
    
    private void registerAsyncCounter(String metricKey, MetricName metricName, KafkaMetric metric, 
                                    Attributes attributes, Number initialValue) {
        try {
            String description = buildDescription(metricName);
            String unit = determineUnit(metricName);
            
            // Use appropriate counter type based on initial value type
            if (initialValue instanceof Long || initialValue instanceof Integer) {
                ObservableLongCounter counter = meter.counterBuilder(metricKey)
                    .setDescription(description)
                    .setUnit(unit)
                    .buildWithCallback(measurement -> {
                        Number value = (Number) safeMetricValue(metric);
                        if (value != null) {
                            long longValue = value.longValue();
                            if (longValue >= 0) {
                                measurement.record(longValue, attributes);
                            }
                        }
                    });
                observableHandles.put(metricKey, counter);
            } else {
                ObservableDoubleCounter counter = meter.counterBuilder(metricKey)
                    .ofDoubles()
                    .setDescription(description)
                    .setUnit(unit)
                    .buildWithCallback(measurement -> {
                        Number value = (Number) safeMetricValue(metric);
                        if (value != null) {
                            double doubleValue = value.doubleValue();
                            if (doubleValue >= 0) {
                                measurement.record(doubleValue, attributes);
                            }
                        }
                    });
                observableHandles.put(metricKey, counter);
            }
            
            LOGGER.debug("Registered async counter: {}", metricKey);
        } catch (Exception e) {
            LOGGER.warn("Failed to register async counter for {}", metricKey, e);
        }
    }
    
    private Object safeMetricValue(KafkaMetric metric) {
        try {
            return metric.metricValue();
        } catch (Exception e) {
            LOGGER.debug("Failed to read metric value for {}", metric.metricName(), e);
            return null;
        }
    }
    
    private void closeHandle(String metricKey) {
        AutoCloseable handle = observableHandles.remove(metricKey);
        if (handle != null) {
            try {
                handle.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing handle for {}", metricKey, e);
            }
        }
    }
    
    private String buildMetricKey(MetricName metricName) {
        StringBuilder sb = new StringBuilder(metricPrefix);
        sb.append(".");
        
        // Add group if present
        if (metricName.group() != null && !metricName.group().isEmpty()) {
            sb.append(metricName.group().replace("-", "_").toLowerCase(Locale.ROOT));
            sb.append(".");
        }
        
        // Add name
        sb.append(metricName.name().replace("-", "_").toLowerCase(Locale.ROOT));
        
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
        return key.replace("-", "_").replace(".", "_").toLowerCase(Locale.ROOT);
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
        String name = metricName.name().toLowerCase(Locale.ROOT);
        String group = metricName.group() != null ? metricName.group().toLowerCase(Locale.ROOT) : "";

        if (isKafkaConnectMetric(group)) {
            return determineConnectMetricUnit(name);
        }

        if (isTimeMetric(name)) {
            return determineTimeUnit(name);
        }

        if (isBytesMetric(name)) {
            return determineBytesUnit(name);
        }

        if (isRateMetric(name)) {
            return "1/s";
        }

        if (isRatioOrPercentageMetric(name)) {
            return "1";
        }

        if (isCountMetric(name)) {
            return "1";
        }
        
        return "1";
    }
    
    private boolean isCounterMetric(MetricName metricName) {
        String name = metricName.name().toLowerCase(Locale.ROOT);
        String group = metricName.group() != null ? metricName.group().toLowerCase(Locale.ROOT) : "";

        if (isKafkaConnectMetric(group)) {
            return isConnectCounterMetric(name);
        }

        if (name.contains("rate") || name.contains("avg") || name.contains("mean") ||
            name.contains("ratio") || name.contains("percent") || name.contains("pct") ||
            name.contains("max") || name.contains("min") || name.contains("current") ||
            name.contains("active") || name.contains("lag") || name.contains("size") ||
            name.contains("time") && !name.contains("total")) {
            return false;
        }

        String[] parts = name.split("[._-]");
        for (String part : parts) {
            if ("total".equals(part) || "count".equals(part) || "sum".equals(part) ||
                "attempts".equals(part) || "success".equals(part) || "failure".equals(part) ||
                "errors".equals(part) || "retries".equals(part) || "skipped".equals(part)) {
                return true;
            }
        }

        return false;
    }
    
    private boolean isConnectCounterMetric(String name) {
        if (name.contains("total") || name.contains("attempts") || 
            name.contains("success") && name.contains("total") ||
            name.contains("failure") && name.contains("total") ||
            name.contains("errors") || name.contains("retries") ||
            name.contains("skipped") || name.contains("requests") ||
            name.contains("completions")) {
            return true;
        }
        
        if ((name.contains("record") || name.contains("records")) && 
            (name.contains("poll-total") || name.contains("write-total") ||
             name.contains("read-total") || name.contains("send-total"))) {
            return true;
        }
        
        if (name.contains("active-count") || name.contains("partition-count") ||
            name.contains("task-count") || name.contains("connector-count") ||
            name.contains("running-count") || name.contains("paused-count") ||
            name.contains("failed-count") || name.contains("seq-no") ||
            name.contains("seq-num")) {
            return false;
        }
        
        return false;
    }
    
    private boolean isKafkaConnectMetric(String group) {
        return group.contains("connector") || group.contains("task") || 
               group.contains("connect") || group.contains("worker");
    }
    
    private String determineConnectMetricUnit(String name) {
        if (name.endsWith("-time-ms") || name.endsWith("-avg-time-ms") || 
            name.endsWith("-max-time-ms") || name.contains("commit-time") ||
            name.contains("batch-time") || name.contains("rebalance-time")) {
            return "ms";
        }
        
        if (name.contains("seq-no") || name.contains("seq-num") || 
            name.endsWith("-count") || name.contains("task-count") ||
            name.contains("partition-count")) {
            return "1";
        }
        
        if (name.contains("lag")) {
            return "1";
        }
        
        if ("status".equals(name) || name.contains("protocol") || 
            name.contains("leader-name") || name.contains("connector-type") ||
            name.contains("connector-class") || name.contains("connector-version")) {
            return "1";
        }
        
        if (name.contains("rate") && !name.contains("ratio")) {
            return "1/s";
        }
        
        if (name.contains("ratio") || name.contains("percentage")) {
            return "1";
        }
        
        if (name.contains("total") || name.contains("sum") || 
            name.contains("attempts") || name.contains("success") ||
            name.contains("failure") || name.contains("errors") ||
            name.contains("retries") || name.contains("skipped")) {
            return "1";
        }
        
        if (name.contains("timestamp") || name.contains("epoch")) {
            return "ms";
        }
        
        if (name.contains("time-since-last") || name.contains("since-last")) {
            return "ms";
        }
        
        return "1";
    }
    
    private boolean isTimeMetric(String name) {
        return (name.contains("time") || name.contains("latency") || 
                name.contains("duration")) && 
               !name.contains("ratio") && !name.contains("rate") &&
               !name.contains("count") && !name.contains("since-last");
    }
    
    private String determineTimeUnit(String name) {
        if (name.contains("ms") || name.contains("millisecond")) {
            return "ms";
        } else if (name.contains("us") || name.contains("microsecond")) {
            return "us";
        } else if (name.contains("ns") || name.contains("nanosecond")) {
            return "ns";
        } else if (name.contains("s") && !name.contains("ms")) {
            return "s";
        } else {
            return "ms";
        }
    }
    
    private boolean isBytesMetric(String name) {
        return name.contains("byte") || name.contains("bytes") || 
               name.contains("size") && !name.contains("batch-size");
    }
    
    private String determineBytesUnit(String name) {
        boolean isRate = name.contains("rate") || name.contains("per-sec") || 
                        name.contains("persec") || name.contains("/s");
        return isRate ? "By/s" : "By";
    }
    
    private boolean isRateMetric(String name) {
        return (name.contains("rate") || name.contains("per-sec") || 
                name.contains("persec") || name.contains("/s")) &&
               !name.contains("byte") && !name.contains("ratio");
    }
    
    private boolean isRatioOrPercentageMetric(String name) {
        return name.contains("percent") || name.contains("ratio") || 
               name.contains("pct");
    }
    
    private boolean isCountMetric(String name) {
        return name.contains("count") || name.contains("total") || 
               name.contains("sum") || name.endsWith("-num");
    }
    
    private boolean shouldIncludeMetric(String metricKey) {
        if (excludePattern != null && metricKey.matches(excludePattern)) {
            return false;
        }
        
        if (includePattern != null) {
            return metricKey.matches(includePattern);
        }
        
        return true;
    }
}
