/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry;

import com.automq.shell.AutoMQApplication;
import com.automq.shell.metrics.S3MetricsConfig;
import com.automq.shell.metrics.S3MetricsExporter;
import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.metrics.ObjectWALMetricsManager;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.instrumentation.runtimemetrics.java8.BufferPools;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import scala.collection.immutable.Set;

public class TelemetryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryManager.class);
    private static final Integer EXPORTER_TIMEOUT_MS = 5000;
    private static OpenTelemetrySdk openTelemetrySdk;
    private static boolean traceEnable = false;
    private final KafkaConfig kafkaConfig;
    private final String clusterId;
    protected final List<MetricReader> metricReaderList;
    private final List<AutoCloseable> autoCloseables;
    private JmxMetricInsight jmxMetricInsight;
    private PrometheusHttpServer prometheusHttpServer;

    public TelemetryManager(KafkaConfig kafkaConfig, String clusterId) {
        this.kafkaConfig = kafkaConfig;
        this.clusterId = clusterId;
        this.metricReaderList = new ArrayList<>();
        this.autoCloseables = new ArrayList<>();
        // redirect JUL from OpenTelemetry SDK to SLF4J
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private String getNodeType() {
        Set<ProcessRole> roles = kafkaConfig.processRoles();
        if (roles.size() == 1) {
            return roles.last().toString();
        }
        return "server";
    }

    public static boolean isTraceEnable() {
        return traceEnable;
    }

    public static void setTraceEnable(boolean traceEnable) {
        TelemetryManager.traceEnable = traceEnable;
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            LOGGER.error("Failed to get host name", e);
            return "unknown";
        }
    }

    public void init() {
        Attributes baseAttributes = Attributes.builder()
            .put(ResourceAttributes.SERVICE_NAME, clusterId)
            .put(ResourceAttributes.SERVICE_INSTANCE_ID, String.valueOf(kafkaConfig.nodeId()))
            .put(ResourceAttributes.HOST_NAME, getHostName())
            .put("instance", String.valueOf(kafkaConfig.nodeId())) // for Aliyun Prometheus compatibility
            .put("node_type", getNodeType())
            .build();

        Resource resource = Resource.empty().toBuilder()
            .putAll(baseAttributes)
            .build();

        OpenTelemetrySdkBuilder openTelemetrySdkBuilder = OpenTelemetrySdk.builder();

        if (kafkaConfig.s3MetricsEnable()) {
            SdkMeterProviderBuilder sdkMeterProviderBuilder = buildMetricsProvider(resource);
            if (sdkMeterProviderBuilder != null) {
                openTelemetrySdkBuilder.setMeterProvider(sdkMeterProviderBuilder.build());
            }
        }
        if (kafkaConfig.s3TracerEnable()) {
            SdkTracerProvider sdkTracerProvider = getTraceProvider(resource);
            if (sdkTracerProvider != null) {
                openTelemetrySdkBuilder.setTracerProvider(sdkTracerProvider);
            }
        }

        setOpenTelemetrySdk(openTelemetrySdkBuilder
            .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
                W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
            .build());

        if (kafkaConfig.s3MetricsEnable()) {
            addJmxMetrics(openTelemetrySdk);
            addJvmMetrics();

            // initialize S3Stream metrics
            Meter meter = openTelemetrySdk.getMeter(TelemetryConstants.TELEMETRY_SCOPE_NAME);
            initializeMetricsManager(meter);
        }

        setTraceEnable(kafkaConfig.s3TracerEnable());

        LOGGER.info("Instrument manager initialized with metrics: {} (level: {}), trace: {} report interval: {}",
            kafkaConfig.s3MetricsEnable(), kafkaConfig.s3MetricsLevel(), kafkaConfig.s3TracerEnable(), kafkaConfig.s3ExporterReportIntervalMs());
    }

    protected void initializeMetricsManager(Meter meter) {
        S3StreamMetricsManager.configure(new MetricsConfig(metricsLevel(), Attributes.empty(), kafkaConfig.s3ExporterReportIntervalMs()));
        S3StreamMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_METRICS_PREFIX);

        S3StreamKafkaMetricsManager.configure(new MetricsConfig(metricsLevel(), Attributes.empty(), kafkaConfig.s3ExporterReportIntervalMs()));
        S3StreamKafkaMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_METRICS_PREFIX);

        // kraft controller may not have s3WALPath config.
        if (StringUtils.isNotEmpty(kafkaConfig.s3WALPath()) && kafkaConfig.s3WALPath().startsWith("0@s3://")) {
            ObjectWALMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_WAL_METRICS_PREFIX);
        }
    }

    public static OpenTelemetrySdk getOpenTelemetrySdk() {
        return openTelemetrySdk;
    }

    public static void setOpenTelemetrySdk(OpenTelemetrySdk openTelemetrySdk) {
        TelemetryManager.openTelemetrySdk = openTelemetrySdk;
    }

    private void addJmxMetrics(OpenTelemetry ot) {
        jmxMetricInsight = JmxMetricInsight.createService(ot, kafkaConfig.s3ExporterReportIntervalMs());
        MetricConfiguration conf = new MetricConfiguration();

        Set<ProcessRole> roles = kafkaConfig.processRoles();
        buildMetricConfiguration(conf, TelemetryConstants.COMMON_JMX_YAML_CONFIG_PATH);
        if (roles.contains(ProcessRole.BrokerRole)) {
            buildMetricConfiguration(conf, TelemetryConstants.BROKER_JMX_YAML_CONFIG_PATH);
        }
        if (roles.contains(ProcessRole.ControllerRole)) {
            buildMetricConfiguration(conf, TelemetryConstants.CONTROLLER_JMX_YAML_CONFIG_PATH);
        }
        jmxMetricInsight.start(conf);
    }

    private void buildMetricConfiguration(MetricConfiguration conf, String path) {
        try (InputStream ins = this.getClass().getResourceAsStream(path)) {
            RuleParser parser = RuleParser.get();
            parser.addMetricDefsTo(conf, ins, path);
        } catch (Exception e) {
            LOGGER.error("Failed to parse JMX config file: {}", path, e);
        }
    }

    private void addJvmMetrics() {
        // JVM metrics
        autoCloseables.addAll(MemoryPools.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(Cpu.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(GarbageCollector.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(BufferPools.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(Threads.registerObservers(openTelemetrySdk));
    }

    protected MetricsLevel metricsLevel() {
        String levelStr = kafkaConfig.s3MetricsLevel();
        if (StringUtils.isBlank(levelStr)) {
            return MetricsLevel.INFO;
        }
        try {
            String up = levelStr.toUpperCase(Locale.ENGLISH);
            return MetricsLevel.valueOf(up);
        } catch (Exception e) {
            LOGGER.error("illegal metrics level: {}", levelStr);
            return MetricsLevel.INFO;
        }
    }

    private SdkTracerProvider getTraceProvider(Resource resource) {
        Optional<String> otlpEndpointOpt = getOTLPEndpoint(kafkaConfig.s3TraceExporterOTLPEndpoint());
        if (otlpEndpointOpt.isEmpty()) {
            otlpEndpointOpt = getOTLPEndpoint(kafkaConfig.s3ExporterOTLPEndpoint());
        }
        if (otlpEndpointOpt.isEmpty()) {
            LOGGER.error("No valid OTLP endpoint found for tracer");
            return null;
        }
        String otlpEndpoint = otlpEndpointOpt.get();
        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
            .setEndpoint(otlpEndpoint)
            .setTimeout(EXPORTER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .build();

        SpanProcessor spanProcessor = BatchSpanProcessor.builder(spanExporter)
            .setExporterTimeout(EXPORTER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .setScheduleDelay(kafkaConfig.s3SpanScheduledDelayMs(), TimeUnit.MILLISECONDS)
            .setMaxExportBatchSize(kafkaConfig.s3SpanMaxBatchSize())
            .setMaxQueueSize(kafkaConfig.s3SpanMaxQueueSize())
            .build();

        return SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor)
            .setResource(resource)
            .build();
    }

    protected SdkMeterProviderBuilder buildMetricsProvider(Resource resource) {
        SdkMeterProviderBuilder sdkMeterProviderBuilder = SdkMeterProvider.builder().setResource(resource);
        String exporterTypes = kafkaConfig.s3MetricsExporterType();
        if (!StringUtils.isBlank(exporterTypes)) {
            String[] exporterTypeArray = exporterTypes.split(",");
            for (String exporterType : exporterTypeArray) {
                exporterType = exporterType.trim();
                switch (exporterType) {
                    case "otlp":
                        initOTLPExporter(sdkMeterProviderBuilder, kafkaConfig);
                        break;
                    case "prometheus":
                        initPrometheusExporter(sdkMeterProviderBuilder, kafkaConfig);
                        break;
                    default:
                        LOGGER.error("illegal metrics exporter type: {}", exporterType);
                        break;
                }
            }
        }

        if (kafkaConfig.s3OpsTelemetryEnabled()) {
            initS3Exporter(sdkMeterProviderBuilder, kafkaConfig);
        }

        return sdkMeterProviderBuilder;
    }

    private void initS3Exporter(SdkMeterProviderBuilder sdkMeterProviderBuilder, KafkaConfig kafkaConfig) {
        if (kafkaConfig.automq().opsBuckets().isEmpty()) {
            LOGGER.error("property s3.ops.bucket is not set, skip initializing s3 metrics exporter.");
            return;
        }
        BucketURI bucket = kafkaConfig.automq().opsBuckets().get(0);

        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(kafkaConfig.automq().opsBuckets().get(0)).build();
        S3MetricsExporter s3MetricsExporter = new S3MetricsExporter(new S3MetricsConfig() {
            @Override
            public String clusterId() {
                return clusterId;
            }

            @Override
            public boolean isActiveController() {
                KafkaRaftServer raftServer = AutoMQApplication.getBean(KafkaRaftServer.class);
                return raftServer != null && raftServer.controller().exists(controller -> controller.controller() != null && controller.controller().isActive());
            }

            @Override
            public int nodeId() {
                return kafkaConfig.nodeId();
            }

            @Override
            public ObjectStorage objectStorage() {
                return objectStorage;
            }
        });
        s3MetricsExporter.start();
        PeriodicMetricReaderBuilder builder = PeriodicMetricReader.builder(s3MetricsExporter);
        MetricReader periodicReader = builder.setInterval(Duration.ofMillis(kafkaConfig.s3ExporterReportIntervalMs())).build();
        metricReaderList.add(periodicReader);

        SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(sdkMeterProviderBuilder, periodicReader,
            instrumentType -> TelemetryConstants.CARDINALITY_LIMIT);
        LOGGER.info("S3 exporter registered, bucket: {}", bucket);
    }

    private void initOTLPExporter(SdkMeterProviderBuilder sdkMeterProviderBuilder, KafkaConfig kafkaConfig) {
        Optional<String> otlpExporterHostOpt = getOTLPEndpoint(kafkaConfig.s3ExporterOTLPEndpoint());
        if (otlpExporterHostOpt.isEmpty()) {
            LOGGER.error("No valid OTLP endpoint found for metrics");
            return;
        }
        String otlpExporterHost = otlpExporterHostOpt.get();

        PeriodicMetricReaderBuilder builder = null;
        String protocol = kafkaConfig.s3ExporterOTLPProtocol();
        switch (protocol) {
            case "grpc":
                OtlpGrpcMetricExporterBuilder otlpExporterBuilder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(otlpExporterHost)
                    .setCompression(kafkaConfig.s3ExporterOTLPCompressionEnable() ? "gzip" : "none")
                    .setTimeout(Duration.ofMillis(30000));
                builder = PeriodicMetricReader.builder(otlpExporterBuilder.build());
                break;
            case "http":
                OtlpHttpMetricExporterBuilder otlpHttpExporterBuilder = OtlpHttpMetricExporter.builder()
                    .setEndpoint(otlpExporterHost)
                    .setCompression(kafkaConfig.s3ExporterOTLPCompressionEnable() ? "gzip" : "none")
                    .setTimeout(Duration.ofMillis(30000));
                builder = PeriodicMetricReader.builder(otlpHttpExporterBuilder.build());
                break;
            default:
                LOGGER.error("unsupported protocol: {}", protocol);
                break;
        }

        if (builder == null) {
            return;
        }

        MetricReader periodicReader = builder.setInterval(Duration.ofMillis(kafkaConfig.s3ExporterReportIntervalMs())).build();
        metricReaderList.add(periodicReader);
        SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(sdkMeterProviderBuilder, periodicReader,
            instrumentType -> TelemetryConstants.CARDINALITY_LIMIT);
        LOGGER.info("OTLP exporter registered, endpoint: {}, protocol: {}", otlpExporterHost, protocol);
    }

    private void initPrometheusExporter(SdkMeterProviderBuilder sdkMeterProviderBuilder, KafkaConfig kafkaConfig) {
        String promExporterHost = kafkaConfig.s3MetricsExporterPromHost();
        int promExporterPort = kafkaConfig.s3MetricsExporterPromPort();
        if (StringUtils.isBlank(promExporterHost) || promExporterPort <= 0) {
            LOGGER.error("illegal prometheus server address, host: {}, port: {}", promExporterHost, promExporterPort);
            return;
        }
        prometheusHttpServer = PrometheusHttpServer.builder()
            .setHost(promExporterHost)
            .setPort(promExporterPort)
            .build();
        SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(sdkMeterProviderBuilder, prometheusHttpServer,
            instrumentType -> TelemetryConstants.CARDINALITY_LIMIT);
        LOGGER.info("Prometheus exporter registered, host: {}, port: {}", promExporterHost, promExporterPort);
    }

    private Optional<String> getOTLPEndpoint(String endpoint) {
        if (StringUtils.isBlank(endpoint)) {
            return Optional.empty();
        }
        return Optional.of(endpoint);
    }

    public void shutdown() {
        autoCloseables.forEach(autoCloseable -> {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close auto closeable", e);
            }
        });
        if (prometheusHttpServer != null) {
            prometheusHttpServer.close();
        }
        metricReaderList.forEach(metricReader -> {
            metricReader.forceFlush();
            try {
                metricReader.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close metric reader", e);
            }
        });
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
        }
    }
}
