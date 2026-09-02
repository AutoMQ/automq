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

package org.apache.kafka.connect.automq.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import com.automq.opentelemetry.AutoMQTelemetryManager;
import com.automq.stream.s3.operator.BucketURI;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongCounter;

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
    
    private static final String DEFAULT_PREFIX = "kafka";
    
    private boolean enabled = true;
    private String metricPrefix = DEFAULT_PREFIX;
    private String includePattern = null;
    private String excludePattern = null;
    
    private Meter meter;
    private final Map<String, MetricHandle> metricHandles = new ConcurrentHashMap<>();
    private final Map<KafkaMetric, String> metricToKey = new ConcurrentHashMap<>();
    
    public static void initializeTelemetry(Properties props) {
        String exportURIStr = props.getProperty(MetricsConfigConstants.EXPORTER_URI_KEY);
        String serviceName = props.getProperty(MetricsConfigConstants.SERVICE_NAME_KEY, "connect-default");
        String instanceId = props.getProperty(MetricsConfigConstants.SERVICE_INSTANCE_ID_KEY, "0");
        String clusterId = props.getProperty(MetricsConfigConstants.S3_CLIENT_ID_KEY, "cluster-default");
        int intervalMs = Integer.parseInt(props.getProperty(MetricsConfigConstants.EXPORTER_INTERVAL_MS_KEY, "60000"));
        BucketURI metricsBucket = getMetricsBucket(props);
        List<Pair<String, String>> baseLabels = getBaseLabels(props);
        
        AutoMQTelemetryManager.initializeInstance(exportURIStr, serviceName, instanceId, new ConnectMetricsExportConfig(clusterId, Integer.parseInt(instanceId), metricsBucket, baseLabels, intervalMs));
        LOGGER.info("OpenTelemetryMetricsReporter initialized");
    }

    private static BucketURI getMetricsBucket(Properties props) {
        String metricsBucket = props.getProperty(MetricsConfigConstants.S3_BUCKET, "");
        if (StringUtils.isNotBlank(metricsBucket)) {
            List<BucketURI> bucketList = BucketURI.parseBuckets(metricsBucket);
            if (!bucketList.isEmpty()) {
                return bucketList.get(0);
            }
        }
        return null;
    }

    private static List<Pair<String, String>> getBaseLabels(Properties props) {
        // This part is hard to abstract without a clear config pattern.
        // Assuming for now it's empty. The caller can extend this class
        // or the manager can have a method to add more labels.
        String baseLabels = props.getProperty(MetricsConfigConstants.TELEMETRY_METRICS_BASE_LABELS_CONFIG);
        if (StringUtils.isBlank(baseLabels)) {
            return Collections.emptyList();
        }
        List<Pair<String, String>> labels = new ArrayList<>();
        for (String label : baseLabels.split(",")) {
            String[] kv = label.split("=");
            if (kv.length != 2) {
                continue;
            }
            labels.add(Pair.of(kv[0], kv[1]));
        }
        return labels;
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
        
        removeMetricInternal(metric);
    }

    @Override
    public void close() {
        if (enabled) {
            metricHandles.forEach((metricKey, handle) -> closeHandle(metricKey, handle));
            metricHandles.clear();
            metricToKey.clear();
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

        // Remove any stale registration before re-adding the metric
        removeMetricInternal(metric);

        AtomicBoolean creationFailed = new AtomicBoolean(false);
        AtomicBoolean entryAdded = new AtomicBoolean(false);
        metricHandles.compute(metricKey, (key, existingHandle) -> {
            MetricHandle handle = existingHandle;
            if (handle == null) {
                MetricHandle newHandle = createMetricHandle(key, metricName, (Number) testValue, isCounterMetric(metricName));
                if (newHandle == null || newHandle.handle == null) {
                    creationFailed.set(true);
                    return null;
                }
                handle = newHandle;
            }
            handle.entries.add(new MetricEntry(metric, attributes));
            entryAdded.set(true);
            return handle;
        });

        if (creationFailed.get()) {
            LOGGER.warn("Failed to register metric handle for {}", metricKey);
            return;
        }

        if (entryAdded.get()) {
            metricToKey.put(metric, metricKey);
        }
    }
    
    private MetricHandle createMetricHandle(String metricKey, MetricName metricName, Number initialValue, boolean isCounter) {
        MetricHandle handle = new MetricHandle();
        String description = buildDescription(metricName);
        // Kafka Connect metrics have already encoded their semantic unit in the Kafka metric
        // name, for example "record-age-ms-avg", "byte-rate", and
        // "offset-commit-avg-time-ms". Keep the OpenTelemetry unit empty here so the
        // Prometheus-compatible exporter does not append another inferred suffix such as
        // "_ratio", "_milliseconds", or "_bytes_per_second".
        String unit = "";

        if (isCounter) {
            boolean useLongCounter = initialValue instanceof Long || initialValue instanceof Integer;
            AutoCloseable counterHandle = registerSharedAsyncCounter(metricKey, description, unit, handle, useLongCounter);
            if (counterHandle == null) {
                return null;
            }
            handle.handle = counterHandle;
        } else {
            AutoCloseable gaugeHandle = registerSharedAsyncGauge(metricKey, description, unit, handle);
            if (gaugeHandle == null) {
                return null;
            }
            handle.handle = gaugeHandle;
        }
        return handle;
    }

    private AutoCloseable registerSharedAsyncGauge(String metricKey, String description, String unit, MetricHandle metricHandle) {
        try {
            ObservableDoubleGauge gauge = meter.gaugeBuilder(metricKey)
                .setDescription(description)
                .setUnit(unit)
                .buildWithCallback(measurement -> {
                    for (MetricEntry entry : metricHandle.entries) {
                        Number value = (Number) safeMetricValue(entry.metric);
                        if (value != null) {
                            measurement.record(value.doubleValue(), entry.attributes);
                        }
                    }
                });
            LOGGER.debug("Registered shared async gauge: {}", metricKey);
            return gauge;
        } catch (Exception e) {
            LOGGER.warn("Failed to register shared async gauge for {}", metricKey, e);
            return null;
        }
    }

    private AutoCloseable registerSharedAsyncCounter(String metricKey, String description, String unit,
                                                     MetricHandle metricHandle, boolean useLongCounter) {
        try {
            if (useLongCounter) {
                ObservableLongCounter counter = meter.counterBuilder(metricKey)
                    .setDescription(description)
                    .setUnit(unit)
                    .buildWithCallback(measurement -> {
                        for (MetricEntry entry : metricHandle.entries) {
                            Number value = (Number) safeMetricValue(entry.metric);
                            if (value != null) {
                                long longValue = value.longValue();
                                if (longValue >= 0) {
                                    measurement.record(longValue, entry.attributes);
                                }
                            }
                        }
                    });
                LOGGER.debug("Registered shared async long counter: {}", metricKey);
                return counter;
            }

            ObservableDoubleCounter counter = meter.counterBuilder(metricKey)
                .ofDoubles()
                .setDescription(description)
                .setUnit(unit)
                .buildWithCallback(measurement -> {
                    for (MetricEntry entry : metricHandle.entries) {
                        Number value = (Number) safeMetricValue(entry.metric);
                        if (value != null) {
                            double doubleValue = value.doubleValue();
                            if (doubleValue >= 0) {
                                measurement.record(doubleValue, entry.attributes);
                            }
                        }
                    }
                });
            LOGGER.debug("Registered shared async double counter: {}", metricKey);
            return counter;
        } catch (Exception e) {
            LOGGER.warn("Failed to register shared async counter for {}", metricKey, e);
            return null;
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
    
    private void closeHandle(String metricKey, MetricHandle handle) {
        if (handle != null && handle.handle != null) {
            try {
                handle.handle.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing handle for {}", metricKey, e);
            }
        }
    }

    private void removeMetricInternal(KafkaMetric metric) {
        String metricKey = metricToKey.remove(metric);
        if (metricKey == null) {
            return;
        }

        metricHandles.compute(metricKey, (key, handle) -> {
            if (handle == null) {
                return null;
            }

            handle.entries.removeIf(entry -> entry.metric == metric);

            if (handle.entries.isEmpty()) {
                closeHandle(key, handle);
                return null;
            }
            return handle;
        });
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
    
    private boolean isCounterMetric(MetricName metricName) {
        String name = metricName.name().toLowerCase(Locale.ROOT);
        String group = metricName.group() != null ? metricName.group().toLowerCase(Locale.ROOT) : "";

        if (isKafkaConnectMetric(group)) {
            return isConnectCounterMetric(name);
        }

        if (isGaugeMetric(name)) {
            return false;
        }

        return hasCounterKeywords(name);
    }
    
    private boolean isGaugeMetric(String name) {
        return hasRateOrAvgKeywords(name) || hasRatioOrPercentKeywords(name) ||
               hasMinMaxOrCurrentKeywords(name) || hasActiveOrSizeKeywords(name) ||
               hasTimeButNotTotal(name);
    }
    
    private boolean hasRateOrAvgKeywords(String name) {
        return name.contains("rate") || name.contains("avg") || name.contains("mean");
    }
    
    private boolean hasRatioOrPercentKeywords(String name) {
        return name.contains("ratio") || name.contains("percent") || name.contains("pct");
    }
    
    private boolean hasMinMaxOrCurrentKeywords(String name) {
        return name.contains("max") || name.contains("min") || name.contains("current");
    }
    
    private boolean hasActiveOrSizeKeywords(String name) {
        return name.contains("active") || name.contains("lag") || name.contains("size");
    }
    
    private boolean hasTimeButNotTotal(String name) {
        return name.contains("time") && !name.contains("total");
    }
    
    private boolean hasCounterKeywords(String name) {
        String[] parts = name.split("[._-]");
        for (String part : parts) {
            if (isCounterKeyword(part)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isCounterKeyword(String part) {
        return isBasicCounterKeyword(part) || isAdvancedCounterKeyword(part);
    }
    
    private boolean isBasicCounterKeyword(String part) {
        return "total".equals(part) || "count".equals(part) || "sum".equals(part) ||
               "attempts".equals(part);
    }
    
    private boolean isAdvancedCounterKeyword(String part) {
        return "success".equals(part) || "failure".equals(part) ||
               "errors".equals(part) || "retries".equals(part) || "skipped".equals(part);
    }
    
    private boolean isConnectCounterMetric(String name) {
        if (hasTotalBasedCounters(name)) {
            return true;
        }
        
        if (hasRecordCounters(name)) {
            return true;
        }
        
        if (hasActiveCountMetrics(name)) {
            return false;
        }
        
        return false;
    }
    
    private boolean hasTotalBasedCounters(String name) {
        return hasBasicTotalCounters(name) || hasSuccessFailureCounters(name) ||
               hasErrorRetryCounters(name) || hasRequestCompletionCounters(name);
    }
    
    private boolean hasBasicTotalCounters(String name) {
        return name.contains("total") || name.contains("attempts");
    }
    
    private boolean hasSuccessFailureCounters(String name) {
        return (name.contains("success") && name.contains("total")) ||
               (name.contains("failure") && name.contains("total"));
    }
    
    private boolean hasErrorRetryCounters(String name) {
        return name.contains("errors") || name.contains("retries") || name.contains("skipped");
    }
    
    private boolean hasRequestCompletionCounters(String name) {
        return name.contains("requests") || name.contains("completions");
    }
    
    private boolean hasRecordCounters(String name) {
        return hasRecordKeyword(name) && hasTotalOperation(name);
    }
    
    private boolean hasRecordKeyword(String name) {
        return name.contains("record") || name.contains("records");
    }
    
    private boolean hasTotalOperation(String name) {
        return hasPollWriteTotal(name) || hasReadSendTotal(name);
    }
    
    private boolean hasPollWriteTotal(String name) {
        return name.contains("poll-total") || name.contains("write-total");
    }
    
    private boolean hasReadSendTotal(String name) {
        return name.contains("read-total") || name.contains("send-total");
    }
    
    private boolean hasActiveCountMetrics(String name) {
        return hasCountMetrics(name) || hasSequenceMetrics(name);
    }
    
    private boolean hasCountMetrics(String name) {
        return hasActiveTaskCount(name) || hasConnectorCount(name) || hasStatusCount(name);
    }
    
    private boolean hasActiveTaskCount(String name) {
        return name.contains("active-count") || name.contains("partition-count") ||
               name.contains("task-count");
    }
    
    private boolean hasConnectorCount(String name) {
        return name.contains("connector-count") || name.contains("running-count");
    }
    
    private boolean hasStatusCount(String name) {
        return name.contains("paused-count") || name.contains("failed-count");
    }
    
    private boolean hasSequenceMetrics(String name) {
        return name.contains("seq-no") || name.contains("seq-num");
    }
    
    private boolean isKafkaConnectMetric(String group) {
        return group.contains("connector") || group.contains("task") || 
               group.contains("connect") || group.contains("worker");
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

    private static final class MetricEntry {
        final KafkaMetric metric;
        final Attributes attributes;

        MetricEntry(KafkaMetric metric, Attributes attributes) {
            this.metric = metric;
            this.attributes = attributes;
        }
    }

    private static final class MetricHandle {
        final CopyOnWriteArrayList<MetricEntry> entries = new CopyOnWriteArrayList<>();
        AutoCloseable handle;
    }
}
