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
    private final Map<String, AutoCloseable> observableHandles = new ConcurrentHashMap<>();
    private final Map<String, KafkaMetric> registeredMetrics = new ConcurrentHashMap<>();
    
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
    
    private String determineConnectMetricUnit(String name) {
        String timeUnit = getTimeUnit(name);
        if (timeUnit != null) {
            return timeUnit;
        }
        
        String countUnit = getCountUnit(name);
        if (countUnit != null) {
            return countUnit;
        }
        
        String specialUnit = getSpecialUnit(name);
        if (specialUnit != null) {
            return specialUnit;
        }
        
        return "1";
    }
    
    private String getTimeUnit(String name) {
        if (isTimeBasedMetric(name)) {
            return "ms";
        }
        if (isTimestampMetric(name)) {
            return "ms";
        }
        if (isTimeSinceMetric(name)) {
            return "ms";
        }
        return null;
    }
    
    private String getCountUnit(String name) {
        if (isSequenceOrCountMetric(name)) {
            return "1";
        }
        if (isLagMetric(name)) {
            return "1";
        }
        if (isTotalOrCounterMetric(name)) {
            return "1";
        }
        return null;
    }
    
    private String getSpecialUnit(String name) {
        if (isStatusOrMetadataMetric(name)) {
            return "1";
        }
        if (isConnectRateMetric(name)) {
            return "1/s";
        }
        if (isRatioMetric(name)) {
            return "1";
        }
        return null;
    }
    
    private boolean isTimeBasedMetric(String name) {
        return hasTimeMs(name) || hasCommitBatchTime(name);
    }
    
    private boolean hasTimeMs(String name) {
        return name.endsWith("-time-ms") || name.endsWith("-avg-time-ms") || 
               name.endsWith("-max-time-ms");
    }
    
    private boolean hasCommitBatchTime(String name) {
        return name.contains("commit-time") || name.contains("batch-time") || 
               name.contains("rebalance-time");
    }
    
    private boolean isSequenceOrCountMetric(String name) {
        return hasSequenceNumbers(name) || hasCountSuffix(name);
    }
    
    private boolean hasSequenceNumbers(String name) {
        return name.contains("seq-no") || name.contains("seq-num");
    }
    
    private boolean hasCountSuffix(String name) {
        return name.endsWith("-count") || name.contains("task-count") ||
               name.contains("partition-count");
    }
    
    private boolean isLagMetric(String name) {
        return name.contains("lag");
    }
    
    private boolean isStatusOrMetadataMetric(String name) {
        return isStatusMetric(name) || hasProtocolLeaderMetrics(name) || 
               hasConnectorMetrics(name);
    }
    
    private boolean isStatusMetric(String name) {
        return "status".equals(name) || name.contains("protocol");
    }
    
    private boolean hasProtocolLeaderMetrics(String name) {
        return name.contains("leader-name");
    }
    
    private boolean hasConnectorMetrics(String name) {
        return name.contains("connector-type") || name.contains("connector-class") ||
               name.contains("connector-version");
    }
    
    private boolean isRatioMetric(String name) {
        return name.contains("ratio") || name.contains("percentage");
    }
    
    private boolean isTotalOrCounterMetric(String name) {
        return hasTotalSum(name) || hasAttempts(name) || hasSuccessFailure(name) ||
               hasErrorsRetries(name);
    }
    
    private boolean hasTotalSum(String name) {
        return name.contains("total") || name.contains("sum");
    }
    
    private boolean hasAttempts(String name) {
        return name.contains("attempts");
    }
    
    private boolean hasSuccessFailure(String name) {
        return name.contains("success") || name.contains("failure");
    }
    
    private boolean hasErrorsRetries(String name) {
        return name.contains("errors") || name.contains("retries") || name.contains("skipped");
    }
    
    private boolean isTimestampMetric(String name) {
        return name.contains("timestamp") || name.contains("epoch");
    }
    
    private boolean isConnectRateMetric(String name) {
        return name.contains("rate") && !name.contains("ratio");
    }
    
    private boolean isTimeSinceMetric(String name) {
        return name.contains("time-since-last") || name.contains("since-last");
    }
    
    private boolean isTimeMetric(String name) {
        return hasTimeKeywords(name) && !hasTimeExclusions(name);
    }
    
    private boolean hasTimeKeywords(String name) {
        return name.contains("time") || name.contains("latency") || 
               name.contains("duration");
    }
    
    private boolean hasTimeExclusions(String name) {
        return name.contains("ratio") || name.contains("rate") ||
               name.contains("count") || name.contains("since-last");
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
        return hasRateKeywords(name) && !hasExcludedKeywords(name);
    }
    
    private boolean hasRateKeywords(String name) {
        return name.contains("rate") || name.contains("per-sec") || 
               name.contains("persec") || name.contains("/s");
    }
    
    private boolean hasExcludedKeywords(String name) {
        return name.contains("byte") || name.contains("ratio");
    }
    
    private boolean isRatioOrPercentageMetric(String name) {
        return hasPercentKeywords(name) || hasRatioKeywords(name);
    }
    
    private boolean hasPercentKeywords(String name) {
        return name.contains("percent") || name.contains("pct");
    }
    
    private boolean hasRatioKeywords(String name) {
        return name.contains("ratio");
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
