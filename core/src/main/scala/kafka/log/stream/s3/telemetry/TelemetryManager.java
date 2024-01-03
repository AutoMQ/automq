/*
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

package kafka.log.stream.s3.telemetry;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import scala.collection.immutable.Set;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

public class TelemetryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryManager.class);
    private static final Integer EXPORTER_TIMEOUT_MS = 5000;
    private static java.util.logging.Logger metricsLogger;
    private static OpenTelemetrySdk openTelemetrySdk;
    private static boolean traceEnable = false;
    private final KafkaConfig kafkaConfig;
    private final String clusterId;
    private final Map<AttributeKey<String>, String> labelMap;
    private final Supplier<AttributesBuilder> attributesBuilderSupplier;
    private final List<MetricReader> metricReaderList;
    private final List<AutoCloseable> autoCloseables;
    private PrometheusHttpServer prometheusHttpServer;

    public TelemetryManager(KafkaConfig kafkaConfig, String clusterId) {
        this.kafkaConfig = kafkaConfig;
        this.clusterId = clusterId;
        this.labelMap = new HashMap<>();
        this.metricReaderList = new ArrayList<>();
        this.autoCloseables = new ArrayList<>();
        this.attributesBuilderSupplier = Attributes::builder;
        init();
    }

    private String getNodeType() {
        Set<KafkaRaftServer.ProcessRole> roles = kafkaConfig.processRoles();
        if (roles.size() == 1) {
            return roles.last().toString();
        }
        return "server";
    }

    public static boolean isTraceEnable() {
        return traceEnable;
    }

    private void init() {
        String nodeType = getNodeType();
        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAMESPACE, clusterId)
                .put(ResourceAttributes.SERVICE_NAME, nodeType)
                .put(ResourceAttributes.SERVICE_INSTANCE_ID, String.valueOf(kafkaConfig.nodeId()))
                .build();

        labelMap.put(AttributeKey.stringKey("cluster_id"), clusterId);
        labelMap.put(AttributeKey.stringKey("node_type"), nodeType);
        labelMap.put(AttributeKey.stringKey("node_id"), String.valueOf(kafkaConfig.nodeId()));

        OpenTelemetrySdkBuilder openTelemetrySdkBuilder = OpenTelemetrySdk.builder();

        traceEnable = kafkaConfig.s3TracerEnable();

        if (kafkaConfig.s3MetricsEnable()) {
            SdkMeterProvider sdkMeterProvider = getMetricsProvider(resource);
            if (sdkMeterProvider != null) {
                openTelemetrySdkBuilder.setMeterProvider(sdkMeterProvider);
            }
        }
        if (kafkaConfig.s3TracerEnable()) {
            openTelemetrySdkBuilder.setTracerProvider(getTraceProvider(resource));
        }

        openTelemetrySdk = openTelemetrySdkBuilder
                .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .build();

        if (kafkaConfig.s3MetricsEnable()) {
            addJmxMetrics(openTelemetrySdk);
            addJvmMetrics();

            // initialize S3Stream metrics
            Meter meter = openTelemetrySdk.getMeter(TelemetryConstants.TELEMETRY_SCOPE_NAME);
            S3StreamMetricsManager.setMetricsLevel(metricsLevel());
            S3StreamMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_METRICS_PREFIX);
            S3StreamMetricsManager.initAttributesBuilder(() -> {
                AttributesBuilder builder = attributesBuilderSupplier.get();
                labelMap.forEach(builder::put);
                return builder;
            });
        }

        LOGGER.info("Instrument manager initialized with metrics: {} (level: {}), trace: {} report interval: {}",
                kafkaConfig.s3MetricsEnable(), kafkaConfig.s3MetricsLevel(), kafkaConfig.s3TracerEnable(), kafkaConfig.s3ExporterReportIntervalMs());
    }

    public static OpenTelemetrySdk getOpenTelemetrySdk() {
        return openTelemetrySdk;
    }

    private void addJmxMetrics(OpenTelemetry ot) {
        JmxMetricInsight jmxMetricInsight = JmxMetricInsight.createService(ot, kafkaConfig.s3ExporterReportIntervalMs());
        MetricConfiguration conf = new MetricConfiguration();

        Set<KafkaRaftServer.ProcessRole> roles = kafkaConfig.processRoles();
        if (roles.contains(KafkaRaftServer.BrokerRole$.MODULE$)) {
            buildMetricConfiguration(conf, TelemetryConstants.BROKER_JMX_YAML_CONFIG_PATH);
        }
        if (roles.contains(KafkaRaftServer.ControllerRole$.MODULE$)) {
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
        // set JVM metrics opt-in to prevent metrics conflict.
        System.setProperty("otel.semconv-stability.opt-in", "jvm");
        // JVM metrics
        autoCloseables.addAll(MemoryPools.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(Cpu.registerObservers(openTelemetrySdk));
        autoCloseables.addAll(GarbageCollector.registerObservers(openTelemetrySdk));
    }

    private MetricsLevel metricsLevel() {
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
        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(kafkaConfig.s3ExporterOTLPEndpoint())
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

    private SdkMeterProvider getMetricsProvider(Resource resource) {
        SdkMeterProviderBuilder sdkMeterProviderBuilder = SdkMeterProvider.builder().setResource(resource);
        String exporterTypes = kafkaConfig.s3MetricsExporterType();
        if (StringUtils.isBlank(exporterTypes)) {
            LOGGER.info("Metrics exporter not configured");
            return null;
        }
        String[] exporterTypeArray = exporterTypes.split(",");
        for (String exporterType : exporterTypeArray) {
            exporterType = exporterType.trim();
            switch (exporterType) {
                case "otlp":
                    initOTLPExporter(sdkMeterProviderBuilder, kafkaConfig);
                    break;
                case "log":
                    initLogExporter(sdkMeterProviderBuilder, kafkaConfig);
                    break;
                case "prometheus":
                    initPrometheusExporter(sdkMeterProviderBuilder, kafkaConfig);
                    break;
                default:
                    LOGGER.error("illegal metrics exporter type: {}", exporterType);
                    break;
            }
        }
        return sdkMeterProviderBuilder.build();
    }

    private void initOTLPExporter(SdkMeterProviderBuilder sdkMeterProviderBuilder, KafkaConfig kafkaConfig) {
        String otlpExporterHost = kafkaConfig.s3ExporterOTLPEndpoint();
        if (StringUtils.isBlank(otlpExporterHost)) {
            LOGGER.error("illegal OTLP collector endpoint: {}", otlpExporterHost);
            return;
        }
        if (!otlpExporterHost.startsWith("http://")) {
            otlpExporterHost = "https://" + otlpExporterHost;
        }
        OtlpGrpcMetricExporterBuilder otlpExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(otlpExporterHost)
                .setTimeout(Duration.ofMillis(30000));
        MetricReader periodicReader = PeriodicMetricReader.builder(otlpExporterBuilder.build())
                .setInterval(Duration.ofMillis(kafkaConfig.s3ExporterReportIntervalMs()))
                .build();
        metricReaderList.add(periodicReader);
        sdkMeterProviderBuilder.registerMetricReader(periodicReader);
        LOGGER.info("OTLP exporter registered, endpoint: {}", otlpExporterHost);
    }

    private void initLogExporter(SdkMeterProviderBuilder sdkMeterProviderBuilder, KafkaConfig kafkaConfig) {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        MetricReader periodicReader = PeriodicMetricReader.builder(LoggingMetricExporter.create(AggregationTemporality.DELTA))
                .setInterval(Duration.ofMillis(kafkaConfig.s3ExporterReportIntervalMs()))
                .build();
        metricReaderList.add(periodicReader);
        metricsLogger = java.util.logging.Logger.getLogger(LoggingMetricExporter.class.getName());
        metricsLogger.setLevel(Level.FINEST);
        sdkMeterProviderBuilder.registerMetricReader(periodicReader);
        LOGGER.info("Log exporter registered");
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
        sdkMeterProviderBuilder.registerMetricReader(prometheusHttpServer);
        LOGGER.info("Prometheus exporter registered, host: {}, port: {}", promExporterHost, promExporterPort);
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
