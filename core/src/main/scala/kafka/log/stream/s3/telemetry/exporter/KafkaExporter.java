package kafka.log.stream.s3.telemetry.exporter;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.*;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.*;
import kafka.automq.telemetry.proto.metrics.v1.ResourceMetrics;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Kafka Metrics Exporter 实现 OpenTelemetry 指标导出到 Kafka Topic
 */
public class KafkaExporter implements MetricExporter {

    private static final Logger logger = LoggerFactory.getLogger(KafkaExporter.class);

    private static final int MAX_SAMPLES_PER_RECORD = 1000;
    // 1MB
    private static final int MAX_RECORD_SIZE = 1024 * 1024;

    // region 配置常量
    private static final Map<String, Object> DEFAULT_CONFIG = Map.of(
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.RETRIES_CONFIG, 3,
            ProducerConfig.LINGER_MS_CONFIG, 500,
            ProducerConfig.BATCH_SIZE_CONFIG, 1_048_576,
            // 显式设置缓冲区大小（32MB）
            ProducerConfig.BUFFER_MEMORY_CONFIG, 32_000_000,
            // 发送前的最大阻塞时间（5秒）
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000
    );

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final MetricsSerializer serializer;
    private final ExecutorService callbackExecutor;

    /**
     * 生产环境必须使用的构造方法
     */
    public KafkaExporter(KafkaExportURI config) {
        this.topic = validateTopic(config.topic());
        this.serializer = new ProtoMetricsSerializer();

        Map<String, Object> producerConfig = new HashMap<>(DEFAULT_CONFIG);
        // 关键：必须强制指定关键配置
        // 由于配置 kafka.automq.AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_DOC 要求，不能用,分割broker server配置时使用;创建producer时则要按照kafka格式规范将;转为,
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers().replaceAll(";", ","));
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "otel-kafka-exporter-" + UUID.randomUUID());
        configureSecurity(producerConfig, config);

        this.producer = new KafkaProducer<>(
                producerConfig,
                new ByteArraySerializer(),
                new ByteArraySerializer()
        );

        // 使用有界队列防止内存溢出
        this.callbackExecutor = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "kafka-exporter-callback"),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    // KafkaExporter 类中添加方法：
    private boolean isProducerOverloaded() {
        try {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            if (metrics == null) {
                return false;
            }

            double bufferTotal = -1;
            double bufferAvailable = -1;

            // 遍历所有指标，按名称匹配（避免直接构造 MetricName）
            for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
                MetricName metricName = entry.getKey();
                Metric metric = entry.getValue();
                // 筛选属于 producer-metrics 组的关键指标
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
            logger.error("Failed to check producer buffer state", e);
            return false;
        }
    }

    /**
     * 安全协议配置（支持多机制）
     */
    private void configureSecurity(Map<String, Object> config, KafkaExportURI exportConfig) {
        String protocol = exportConfig.securityProtocol().toUpperCase(Locale.ROOT);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (protocol.startsWith("SASL_")) {
            String saslMechanism = exportConfig.saslMechanism();
            // 根据机制动态生成JAAS配置
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
        // 使用独立ResultCode跟踪每个批次
        CompletableResultCode resultCode = new CompletableResultCode();
        if (metrics.isEmpty()) {
            return resultCode.succeed();
        }

        // 检查背压
        if (isProducerOverloaded()) {
            logger.warn("Producer buffer overloaded, discarding metrics to protect memory");
            // 丢弃数据，返回失败让 OpenTelemetry 暂时静默
            return CompletableResultCode.ofFailure();
        }

        List<Future<?>> futures = new ArrayList<>();
        try {
            for (MetricData metric : metrics) {
                List<byte[]> serializedChunks = splitMetricData(metric);
                for (byte[] value : serializedChunks) {
                    if (value == null || value.length == 0) {
                        logger.warn("Skipping invalid metric: {}", metric.getName());
                        continue;
                    }
                    if (value.length > MAX_RECORD_SIZE) {
                        logger.warn("Metric data size exceeds 1MB, discarding: {}", metric.getName());
                        continue;
                    }
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, value);
                    // 添加回调跟踪
                    futures.add(producer.send(record, new LoggingCallback(metric)));
                }
            }

            // 异步跟踪发送状态
            callbackExecutor.submit(() -> {
                boolean allSuccess = true;
                for (Future<?> future : futures) {
                    try {
                        future.get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        allSuccess = false;
                    } catch (Exception e) {
                        logger.error("Failed to send metric batch", e);
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
            logger.error("Callback queue is full, metrics export rejected", e);
            return CompletableResultCode.ofFailure();
        } catch (Exception e) {
            logger.error("Metric serialization failed", e);
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
            logger.warn("Flush failed", e);
            result.fail();
        }
        return result;
    }

    @Override
    public CompletableResultCode shutdown() {
        CompletableResultCode result = new CompletableResultCode();
        try {
            // 分阶段关闭
            callbackExecutor.shutdown();
            if (!callbackExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Force shutdown callback executor");
                callbackExecutor.shutdownNow();
            }

            // 关键：必须关闭producer！
            // 允许缓冲数据发送完成
            producer.close(Duration.ofSeconds(30));
            result.succeed();
        } catch (Exception e) {
            logger.error("Shutdown failed", e);
            result.fail();
        }
        return result;
    }

    // region 辅助方法
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
     * 增强型回调（绑定原始指标信息）
     */
    private static class LoggingCallback implements Callback {
        private final String metricName;

        LoggingCallback(MetricData metric) {
            this.metricName = metric != null ? metric.getName() : "unknown";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                // 关键：正确处理metadata可能为null的情况
                String topicInfo = metadata != null ? metadata.topic() : "unknown-topic";
                logger.error("Failed to send metric [{}] to {}: {}",
                        metricName, topicInfo, exception.getMessage());
            } else if (logger.isTraceEnabled()) {
                logger.trace("Sent [{}] to {}-{}@{}",
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
                logger.warn("Unsupported metric type: {}", metric.getType());
                break;
        }
        return chunks;
    }

    // LongGauge处理器
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

    // DoubleGauge处理器
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

    // LongSum处理器
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

    // DoubleSum处理器
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

    // Histogram处理器
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

    // Summary处理器
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

    // region 公共工具方法
    private <T> List<T> getSubList(List<T> points, int startIndex) {
        int endIndex = Math.min(startIndex + MAX_SAMPLES_PER_RECORD, points.size());
        return points.subList(startIndex, endIndex);
    }

    /**
     * 指标序列化接口（支持扩展多种格式）
     */
    interface MetricsSerializer {
        byte[] serialize(MetricData metric);
    }

    /**
     * Protobuf 序列化实现（与 OTLP 格式兼容）
     */
    static class ProtoMetricsSerializer implements MetricsSerializer {

        ProtoMetricsSerializer() {
        }

        @Override
        public byte[] serialize(MetricData metric) {
            try {
                // 调用内部工具类将 MetricData 转换为 Proto ResourceMetrics
                // 直接调用内部工具类转换数据
                ResourceMetrics resourceMetrics = new MetricProtoConverter().convertToResourceMetrics(metric);
                return resourceMetrics.toByteArray();
            } catch (Throwable e) {
                logger.error("Fail to serialize metric to OTLP format", e);
                return new byte[0];
            }
        }
    }
}
