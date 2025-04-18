/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry;

import kafka.log.stream.s3.telemetry.exporter.MetricsExporter;
import kafka.log.stream.s3.telemetry.exporter.MetricsExporterURI;
import kafka.log.stream.s3.telemetry.otel.OTelHistogramReporter;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.metrics.cert.CertKafkaMetricsManager;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.wal.metrics.ObjectWALMetricsManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.resources.Resource;
import scala.collection.immutable.Set;

public class TelemetryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryManager.class);
    private final KafkaConfig kafkaConfig;
    private final String clusterId;
    protected final List<MetricReader> metricReaderList;
    private final List<AutoCloseable> autoCloseableList;
    private final OTelHistogramReporter oTelHistogramReporter;
    private JmxMetricInsight jmxMetricInsight;
    private OpenTelemetrySdk openTelemetrySdk;

    public TelemetryManager(KafkaConfig kafkaConfig, String clusterId) {
        this.kafkaConfig = kafkaConfig;
        this.clusterId = clusterId;
        this.metricReaderList = new ArrayList<>();
        this.autoCloseableList = new ArrayList<>();
        this.oTelHistogramReporter = new OTelHistogramReporter(KafkaYammerMetrics.defaultRegistry());
        // redirect JUL from OpenTelemetry SDK to SLF4J
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
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
        OpenTelemetrySdkBuilder openTelemetrySdkBuilder = OpenTelemetrySdk.builder();
        openTelemetrySdkBuilder.setMeterProvider(buildMeterProvider(kafkaConfig));
        openTelemetrySdk = openTelemetrySdkBuilder
            .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
            W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
            .build();

        addJmxMetrics(openTelemetrySdk);
        addJvmMetrics(openTelemetrySdk);

        // initialize S3Stream metrics
        Meter meter = openTelemetrySdk.getMeter(TelemetryConstants.TELEMETRY_SCOPE_NAME);
        initializeMetricsManager(meter);
    }

    protected SdkMeterProvider buildMeterProvider(KafkaConfig kafkaConfig) {
        AttributesBuilder baseAttributesBuilder = Attributes.builder()
            .put(MetricsConstants.SERVICE_NAME, clusterId)
            .put(MetricsConstants.SERVICE_INSTANCE, String.valueOf(kafkaConfig.nodeId()))
            .put(MetricsConstants.HOST_NAME, getHostName())
            .put(MetricsConstants.JOB, clusterId) // for Prometheus HTTP server compatibility
            .put(MetricsConstants.INSTANCE, String.valueOf(kafkaConfig.nodeId())); // for Aliyun Prometheus compatibility
        List<Pair<String, String>> extraAttributes = kafkaConfig.automq().baseLabels();
        if (extraAttributes != null) {
            for (Pair<String, String> pair : extraAttributes) {
                baseAttributesBuilder.put(pair.getKey(), pair.getValue());
            }
        }

        Resource resource = Resource.empty().toBuilder()
            .putAll(baseAttributesBuilder.build())
            .build();
        SdkMeterProviderBuilder sdkMeterProviderBuilder = SdkMeterProvider.builder().setResource(resource);
        MetricsExporterURI metricsExporterURI = buildMetricsExporterURI(clusterId, kafkaConfig);
        if (metricsExporterURI != null) {
            for (MetricsExporter metricsExporter : metricsExporterURI.metricsExporters()) {
                MetricReader metricReader = metricsExporter.asMetricReader();
                metricReaderList.add(metricReader);
                SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(sdkMeterProviderBuilder, metricReader,
                    instrumentType -> TelemetryConstants.CARDINALITY_LIMIT);
            }
        }
        return sdkMeterProviderBuilder.build();
    }

    protected MetricsExporterURI buildMetricsExporterURI(String clusterId, KafkaConfig kafkaConfig) {
        return MetricsExporterURI.parse(clusterId, kafkaConfig);
    }

    protected void initializeMetricsManager(Meter meter) {
        S3StreamMetricsManager.configure(new MetricsConfig(metricsLevel(), Attributes.empty(), kafkaConfig.s3ExporterReportIntervalMs()));
        S3StreamMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_METRICS_PREFIX);

        S3StreamKafkaMetricsManager.configure(new MetricsConfig(metricsLevel(), Attributes.empty(), kafkaConfig.s3ExporterReportIntervalMs()));
        S3StreamKafkaMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_METRICS_PREFIX);

        // kraft controller may not have s3WALPath config.
        ObjectWALMetricsManager.initMetrics(meter, TelemetryConstants.KAFKA_WAL_METRICS_PREFIX);
        // Obtain the certificate chain and truststore certificates.
        try {
            Password certChainPassword = kafkaConfig.getPassword("ssl.keystore.certificate.chain");
            Password truststoreCertsPassword = kafkaConfig.getPassword("ssl.truststore.certificates");

            String certChain = certChainPassword != null ? certChainPassword.value() : null;
            String truststoreCerts = truststoreCertsPassword != null ? truststoreCertsPassword.value() : null;
            CertKafkaMetricsManager.initMetrics(meter, truststoreCerts, certChain, TelemetryConstants.KAFKA_CERT_METRICS_PREFIX);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize cert metrics", e);
        }
        this.oTelHistogramReporter.start(meter);
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

    private void addJvmMetrics(OpenTelemetry openTelemetry) {
        // JVM metrics
        autoCloseableList.addAll(MemoryPools.registerObservers(openTelemetry));
        autoCloseableList.addAll(Cpu.registerObservers(openTelemetry));
        autoCloseableList.addAll(GarbageCollector.registerObservers(openTelemetry));
        autoCloseableList.addAll(Threads.registerObservers(openTelemetry));
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

    public void shutdown() {
        autoCloseableList.forEach(autoCloseable -> {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close auto closeable", e);
            }
        });
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

    // Deprecated methods, leave for compatibility
    public static boolean isTraceEnable() {
        return false;
    }

    public static OpenTelemetrySdk getOpenTelemetrySdk() {
        return null;
    }
}
