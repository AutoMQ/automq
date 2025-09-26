package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Kafka MetricsReporter that bridges Kafka metrics to OpenTelemetry.
 * This reporter registers all metrics as observable gauges with OpenTelemetry,
 * which will call back to get the latest values when metrics collection occurs.
 */
public class OTelMetricsReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(OTelMetricsReporter.class);
    
    // Store all metrics for retrieval during OTel callbacks
    private final Map<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();
    
    // Group metrics by group for easier registration with OTel
    private final Map<String, Map<MetricName, KafkaMetric>> metricsByGroup = new ConcurrentHashMap<>();
    
    // Keep track of registered gauges to prevent duplicate registration
    private final Map<String, ObservableDoubleGauge> registeredGauges = new ConcurrentHashMap<>();
    
    private Meter meter;
    private boolean initialized = false;

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Configuring OTelMetricsReporter");
    }

    /**
     * Initialize OpenTelemetry meter and register metrics
     */
    public void initOpenTelemetry(OpenTelemetry openTelemetry) {
        if (initialized) {
            return;
        }
        
        this.meter = openTelemetry.getMeter("kafka-connect-metrics");
        log.info("OTelMetricsReporter initialized with OpenTelemetry meter");
        
        // Register all metrics that were already added before OpenTelemetry was initialized
        registerMetricsWithOTel();
        
        initialized = true;
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        log.info("Initializing OTelMetricsReporter with {} metrics", metrics.size());
        for (KafkaMetric metric : metrics) {
            addMetricToCollections(metric);
        }
        
        // If meter is already available, register metrics
        if (meter != null) {
            registerMetricsWithOTel();
        }
    }

    private void addMetricToCollections(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        metrics.put(metricName, metric);
        
        // Group by metric group
        metricsByGroup
            .computeIfAbsent(metricName.group(), k -> new ConcurrentHashMap<>())
            .put(metricName, metric);
    }

    private void registerMetricsWithOTel() {
        if (meter == null) {
            log.warn("Cannot register metrics with OpenTelemetry - meter not initialized");
            return;
        }
        
        // Register each group of metrics as an observable gauge collection
        for (Map.Entry<String, Map<MetricName, KafkaMetric>> entry : metricsByGroup.entrySet()) {
            String group = entry.getKey();
            Map<MetricName, KafkaMetric> groupMetrics = entry.getValue();
            
            // Register the gauge for this group if not already registered
            String gaugeKey = "kafka.connect." + group;
            if (!registeredGauges.containsKey(gaugeKey)) {
                ObservableDoubleGauge gauge = meter
                    .gaugeBuilder(gaugeKey)
                    .setDescription("Kafka Connect metrics for " + group)
                    .setUnit("1") // Default unit
                    .buildWithCallback(measurement -> {
                        // Get the latest values for all metrics in this group
                        Map<MetricName, KafkaMetric> currentGroupMetrics = metricsByGroup.get(group);
                        if (currentGroupMetrics != null) {
                            for (Map.Entry<MetricName, KafkaMetric> metricEntry : currentGroupMetrics.entrySet()) {
                                MetricName name = metricEntry.getKey();
                                KafkaMetric kafkaMetric = metricEntry.getValue();
                                
                                try {
                                    // Convert metric value to double
                                    double value = convertToDouble(kafkaMetric.metricValue());
                                    
                                    // Build attributes from metric tags
                                    AttributesBuilder attributes = Attributes.builder();
                                    attributes.put("name", name.name());
                                    
                                    // Add all tags as attributes
                                    for (Map.Entry<String, String> tag : name.tags().entrySet()) {
                                        attributes.put(tag.getKey(), tag.getValue());
                                    }
                                    
                                    // Record the measurement
                                    measurement.record(value, attributes.build());
                                } catch (Exception e) {
                                    log.warn("Error recording metric {}: {}", name, e.getMessage());
                                }
                            }
                        }
                    });
                
                registeredGauges.put(gaugeKey, gauge);
                log.info("Registered gauge for metric group: {}", group);
            }
        }
    }

    private double convertToDouble(Object value) {
        if (value == null) {
            return 0.0;
        }
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0 : 0.0;
        }
        
        return 0.0;
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        addMetricToCollections(metric);
        
        // If already initialized with OTel, register new metrics
        if (meter != null && !registeredGauges.containsKey("kafka.connect." + metric.metricName().group())) {
            registerMetricsWithOTel();
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        metrics.remove(metricName);
        
        Map<MetricName, KafkaMetric> groupMetrics = metricsByGroup.get(metricName.group());
        if (groupMetrics != null) {
            groupMetrics.remove(metricName);
            if (groupMetrics.isEmpty()) {
                metricsByGroup.remove(metricName.group());
            }
        }
        
        log.debug("Removed metric: {}", metricName);
    }

    @Override
    public void close() {
        log.info("Closing OTelMetricsReporter");
        metrics.clear();
        metricsByGroup.clear();
        registeredGauges.clear();
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        // Add context labels as attributes if needed
        log.info("Metrics context changed: {}", metricsContext.contextLabels());
    }
}
