package kafka.log.stream.s3.telemetry.exporter;

import kafka.automq.telemetry.proto.metrics.v1.ResourceMetrics;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;

/**
 * Kafka Metrics Exporter implementation to export OpenTelemetry metrics to a Kafka topic.
 */
public class KafkaExporter implements MetricExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaExporter.class);

    private static final int MAX_SAMPLES_PER_RECORD = 1000;
    // 1MB
    private static final int MAX_RECORD_SIZE = 1024 * 1024;

    // region Configuration constants
    private static final Map<String, Object> DEFAULT_CONFIG = Map.of(
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.RETRIES_CONFIG, 3,
            ProducerConfig.LINGER_MS_CONFIG, 500,
            ProducerConfig.BATCH_SIZE_CONFIG, 1_048_576,
            // Explicitly set the buffer size (32MB)
            ProducerConfig.BUFFER_MEMORY_CONFIG, 32_000_000,
            // Maximum blocking time before sending (5 seconds)
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000
    );

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final MetricsSerializer serializer;
    private final ExecutorService callbackExecutor;

    /**
     * Constructor required for production environments.
     */
    public KafkaExporter(KafkaExportURI config) {
        this.topic = validateTopic(config.topic());
        this.serializer = new ProtoMetricsSerializer();

        Map<String, Object> producerConfig = new HashMap<>(DEFAULT_CONFIG);
        // Key: Must forcefully specify key configurations
        // Due to the requirement of kafka.automq.AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_DOC, when the broker server configuration cannot be separated by ',', ';' is used. When creating the producer, ';' should be converted to ',' according to the Kafka format specification.
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers().replaceAll(";", ","));
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "otel-kafka-exporter-" + UUID.randomUUID());
        configureSecurity(producerConfig, config);

        this.producer = new KafkaProducer<>(
                producerConfig,
                new ByteArraySerializer(),
                new ByteArraySerializer()
        );

        // Use a bounded queue to prevent memory overflow
        this.callbackExecutor = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "kafka-exporter-callback"),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    // Add a method to the KafkaExporter class:
    private boolean isProducerOverloaded() {
        try {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            if (metrics == null) {
                return false;
            }

            double bufferTotal = -1;
            double bufferAvailable = -1;

            // Iterate through all metrics and match by name (avoid directly constructing MetricName)
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                MetricName metricName = entry.getKey();
                Metric metric = entry.getValue();
                // Filter key metrics belonging to the producer-metrics group
                if ("producer-metrics".equals(metricName.group())) {
                    Object value = metric.metricValue();
                    switch (metricName.name()) {
                        case "buffer-total-bytes":
                            if (value instanceof Double) {
                                bufferTotal = (double) value;
                            }
                            break;
                        case "buffer-available-bytes":
                            if (value instanceof Double) {
                                bufferAvailable = (double) value;
                            }
                            break;
                    }

                }
            }

            if (bufferTotal > 0 && bufferAvailable >= 0) {
                double used = bufferTotal - bufferAvailable;
                return used / bufferTotal > 0.9;
            }
            return false;
        } catch (Exception e) {
            LOGGER.error("Failed to check producer buffer state", e);
            return false;
        }
    }

    /**
     * Security protocol configuration (supports multiple mechanisms).
     */
    private void configureSecurity(Map<String, Object> config, KafkaExportURI exportConfig) {
        String protocol = exportConfig.securityProtocol().toUpperCase(Locale.ROOT);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (protocol.startsWith("SASL_")) {
            String saslMechanism = exportConfig.saslMechanism();
            // Dynamically generate JAAS configuration based on the mechanism
            switch (saslMechanism) {
                case "PLAIN":
                    config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
                    config.put(SaslConfigs.SASL_JAAS_CONFIG,
                            String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                                    exportConfig.saslUsername(),
                                    exportConfig.saslPassword()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported SASL mechanism: " + saslMechanism);
            }
            config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }
    }

    @Override
    public CompletableResultCode export(@NotNull Collection<MetricData> metrics) {
        // Use an independent ResultCode to track each batch
        CompletableResultCode resultCode = new CompletableResultCode();
        if (metrics.isEmpty()) {
            return resultCode.succeed();
        }

        // Check for backpressure
        if (isProducerOverloaded()) {
            LOGGER.warn("Producer buffer overloaded, discarding metrics to protect memory");
            // Discard data and return failure to make OpenTelemetry temporarily silent
            return CompletableResultCode.ofFailure();
        }

        List<Future<?>> futures = new ArrayList<>();
        try {
            for (MetricData metric : metrics) {
                List<byte[]> serializedChunks = splitMetricData(metric);
                for (byte[] value : serializedChunks) {
                    if (value == null || value.length == 0) {
                        LOGGER.warn("Skipping invalid metric: {}", metric.getName());
                        continue;
                    }
                    if (value.length > MAX_RECORD_SIZE) {
                        LOGGER.warn("Metric data size exceeds 1MB, discarding: {}", metric.getName());
                        continue;
                    }
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, value);
                    // Add callback tracking
                    futures.add(producer.send(record, new LoggingCallback(metric)));
                }
            }

            // Asynchronously track the sending status
            callbackExecutor.submit(() -> {
                boolean allSuccess = true;
                for (Future<?> future : futures) {
                    try {
                        future.get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        allSuccess = false;
                    } catch (Exception e) {
                        LOGGER.error("Failed to send metric batch", e);
                        allSuccess = false;
                    }
                }
                if (allSuccess) {
                    resultCode.succeed();
                } else {
                    resultCode.fail();
                }
            });

            return resultCode;
        } catch (RejectedExecutionException e) {
            LOGGER.error("Callback queue is full, metrics export rejected", e);
            return CompletableResultCode.ofFailure();
        } catch (Exception e) {
            LOGGER.error("Metric serialization failed", e);
            return CompletableResultCode.ofFailure();
        }
    }

    @Override
    public CompletableResultCode flush() {
        CompletableResultCode result = new CompletableResultCode();
        try {
            producer.flush();
            result.succeed();
        } catch (Exception e) {
            LOGGER.warn("Flush failed", e);
            result.fail();
        }
        return result;
    }

    @Override
    public CompletableResultCode shutdown() {
        CompletableResultCode result = new CompletableResultCode();
        try {
            // Shutdown in stages
            callbackExecutor.shutdown();
            if (!callbackExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warn("Force shutdown callback executor");
                callbackExecutor.shutdownNow();
            }

            // Key: Must close the producer!
            // Allow buffered data to be sent
            producer.close(Duration.ofSeconds(30));
            result.succeed();
        } catch (Exception e) {
            LOGGER.error("Shutdown failed", e);
            result.fail();
        }
        return result;
    }

    // Helper methods
    @Override
    public AggregationTemporality getAggregationTemporality(@NotNull InstrumentType instrumentType) {
        return AggregationTemporality.CUMULATIVE;
    }

    private String validateTopic(String topic) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka topic must be specified");
        }
        return topic;
    }

    /**
     * Enhanced callback (binds original metric information).
     */
    private static class LoggingCallback implements Callback {
        private final String metricName;

        LoggingCallback(MetricData metric) {
            this.metricName = metric != null ? metric.getName() : "unknown";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                // Key: Correctly handle the case where metadata may be null
                String topicInfo = metadata != null ? metadata.topic() : "unknown-topic";
                LOGGER.error("Failed to send metric [{}] to {}: {}",
                        metricName, topicInfo, exception.getMessage());
            } else if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Sent [{}] to {}-{}@{}",
                        metricName, metadata.topic(), metadata.partition(), metadata.offset());
            }
        }
    }

    private List<byte[]> splitMetricData(MetricData metric) {
        List<byte[]> chunks = new ArrayList<>();
        switch (metric.getType()) {
            case LONG_GAUGE:
                processLongGauge(metric, chunks);
                break;
            case DOUBLE_GAUGE:
                processDoubleGauge(metric, chunks);
                break;
            case LONG_SUM:
                processLongSum(metric, chunks);
                break;
            case DOUBLE_SUM:
                processDoubleSum(metric, chunks);
                break;
            case HISTOGRAM:
                processHistogram(metric, chunks);
                break;
            case SUMMARY:
                processSummary(metric, chunks);
                break;
            default:
                LOGGER.warn("Unsupported metric type: {}", metric.getType());
                break;
        }
        return chunks;
    }

    // LongGauge processor
    private void processLongGauge(MetricData metric, List<byte[]> chunks) {
        List<LongPointData> points = new ArrayList<>(metric.getLongGaugeData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<LongPointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildLongGaugeMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildLongGaugeMetric(MetricData original, List<LongPointData> subPoints) {
        return ImmutableMetricData.createLongGauge(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableGaugeData.create(subPoints)
        );
    }

    // DoubleGauge processor
    private void processDoubleGauge(MetricData metric, List<byte[]> chunks) {
        List<DoublePointData> points = new ArrayList<>(metric.getDoubleGaugeData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<DoublePointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildDoubleGaugeMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildDoubleGaugeMetric(MetricData original, List<DoublePointData> subPoints) {
        return ImmutableMetricData.createDoubleGauge(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableGaugeData.create(subPoints)
        );
    }

    // LongSum processor
    private void processLongSum(MetricData metric, List<byte[]> chunks) {
        List<LongPointData> points = new ArrayList<>(metric.getLongSumData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<LongPointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildLongSumMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildLongSumMetric(MetricData original, List<LongPointData> subPoints) {
        return ImmutableMetricData.createLongSum(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableSumData.create(
                        original.getLongSumData().isMonotonic(),
                        original.getLongSumData().getAggregationTemporality(),
                        subPoints
                )
        );
    }

    // DoubleSum processor
    private void processDoubleSum(MetricData metric, List<byte[]> chunks) {
        List<DoublePointData> points = new ArrayList<>(metric.getDoubleSumData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<DoublePointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildDoubleSumMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildDoubleSumMetric(MetricData original, List<DoublePointData> subPoints) {
        return ImmutableMetricData.createDoubleSum(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableSumData.create(
                        original.getDoubleSumData().isMonotonic(),
                        original.getDoubleSumData().getAggregationTemporality(),
                        subPoints
                )
        );
    }

    // Histogram processor
    private void processHistogram(MetricData metric, List<byte[]> chunks) {
        List<HistogramPointData> points = new ArrayList<>(metric.getHistogramData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<HistogramPointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildHistogramMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildHistogramMetric(MetricData original, List<HistogramPointData> subPoints) {
        return ImmutableMetricData.createDoubleHistogram(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableHistogramData.create(
                        original.getHistogramData().getAggregationTemporality(),
                        subPoints
                )
        );
    }

    // Summary processor
    private void processSummary(MetricData metric, List<byte[]> chunks) {
        List<SummaryPointData> points = new ArrayList<>(metric.getSummaryData().getPoints());
        int pointCount = points.size();

        if (pointCount <= MAX_SAMPLES_PER_RECORD) {
            chunks.add(serializer.serialize(metric));
            return;
        }

        for (int i = 0; i < pointCount; i += MAX_SAMPLES_PER_RECORD) {
            List<SummaryPointData> subPoints = getSubList(points, i);
            MetricData subMetric = buildSummaryMetric(metric, subPoints);
            chunks.add(serializer.serialize(subMetric));
        }
    }

    private MetricData buildSummaryMetric(MetricData original, List<SummaryPointData> subPoints) {
        return ImmutableMetricData.createDoubleSummary(
                original.getResource(),
                original.getInstrumentationScopeInfo(),
                original.getName(),
                original.getDescription(),
                original.getUnit(),
                ImmutableSummaryData.create(subPoints)
        );
    }

    // region Common utility methods
    private <T> List<T> getSubList(List<T> points, int startIndex) {
        int endIndex = Math.min(startIndex + MAX_SAMPLES_PER_RECORD, points.size());
        return points.subList(startIndex, endIndex);
    }

    /**
     * Metric serialization interface (supports extending multiple formats).
     */
    interface MetricsSerializer {
        byte[] serialize(MetricData metric);
    }

    /**
     * Protobuf serialization implementation (compatible with OTLP format).
     */
    static class ProtoMetricsSerializer implements MetricsSerializer {

        ProtoMetricsSerializer() {
        }

        @Override
        public byte[] serialize(MetricData metric) {
            try {
                // Call the internal utility class to convert MetricData to Proto ResourceMetrics
                // Directly call the internal utility class to convert data
                ResourceMetrics resourceMetrics = new MetricProtoConverter().convertToResourceMetrics(metric);
                return resourceMetrics.toByteArray();
            } catch (Throwable e) {
                LOGGER.error("Fail to serialize metric to OTLP format", e);
                return new byte[0];
            }
        }
    }
}
