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

package kafka.log.stream.s3.metrics;

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
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
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ResourceAttributes;
import kafka.server.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;

public class MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporter.class);
    private static final String KAFKA_METRICS_PREFIX = "kafka_stream_";
    private static String clusterId = "default";
    private static java.util.logging.Logger metricsLogger;
    private final KafkaConfig kafkaConfig;
    private final Map<AttributeKey<String>, String> labelMap;
    private final Supplier<AttributesBuilder> attributesBuilderSupplier;
    private OpenTelemetrySdk openTelemetrySdk;
    private PrometheusHttpServer prometheusHttpServer;

    public MetricsExporter(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.labelMap = new HashMap<>();
        this.attributesBuilderSupplier = Attributes::builder;
        init();
    }

    public static void setClusterId(String clusterId) {
        MetricsExporter.clusterId = clusterId;
    }

    private void init() {
        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.HOST_ID, String.valueOf(kafkaConfig.brokerId()))
                .build();

        labelMap.put(AttributeKey.stringKey("cluster_id"), clusterId);
        labelMap.put(AttributeKey.stringKey("node_id"), String.valueOf(kafkaConfig.nodeId()));
        labelMap.put(AttributeKey.stringKey("node_type"), StringUtils.join(kafkaConfig.getList(KafkaConfig.ProcessRolesProp()), ","));

        SdkMeterProviderBuilder sdkMeterProvider = SdkMeterProvider.builder()
                .setResource(resource);

        String exporterTypes = kafkaConfig.s3MetricsExporterType();
        if (StringUtils.isBlank(exporterTypes)) {
            return;
        }
        String[] exporterTypeArray = exporterTypes.split(",");
        for (String exporterType : exporterTypeArray) {
            switch (exporterType) {
                case "otlp":
                    initOTLPExporter(sdkMeterProvider, kafkaConfig);
                    break;
                case "log":
                    initLogExporter(sdkMeterProvider, kafkaConfig);
                    break;
                case "prometheus":
                    initPrometheusExporter(sdkMeterProvider, kafkaConfig);
                    break;
                default:
                    LOGGER.error("illegal metrics exporter type: {}", exporterType);
                    break;
            }
        }

        openTelemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(sdkMeterProvider.build())
                .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .buildAndRegisterGlobal();
        Meter meter = openTelemetrySdk.getMeter("automq-for-kafka");
        S3StreamMetricsManager.initMetrics(meter, KAFKA_METRICS_PREFIX);
        S3StreamMetricsManager.initAttributesBuilder(() -> {
            AttributesBuilder builder = attributesBuilderSupplier.get();
            labelMap.forEach(builder::put);
            return builder;
        });
    }

    private void initOTLPExporter(SdkMeterProviderBuilder sdkMeterProvider, KafkaConfig kafkaConfig) {
        String otlpExporterHost = kafkaConfig.s3MetricsExporterOTLPEndpoint();
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
                .setInterval(Duration.ofMillis(kafkaConfig.s3MetricsExporterReportIntervalMs()))
                .build();
        sdkMeterProvider.registerMetricReader(periodicReader);
    }

    private void initLogExporter(SdkMeterProviderBuilder sdkMeterProvider, KafkaConfig kafkaConfig) {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        MetricReader periodicReader = PeriodicMetricReader.builder(LoggingMetricExporter.create(AggregationTemporality.DELTA))
                .setInterval(Duration.ofMillis(kafkaConfig.s3MetricsExporterReportIntervalMs()))
                .build();
        metricsLogger = java.util.logging.Logger.getLogger(LoggingMetricExporter.class.getName());
        metricsLogger.setLevel(Level.FINEST);
        sdkMeterProvider.registerMetricReader(periodicReader);
    }

    private void initPrometheusExporter(SdkMeterProviderBuilder sdkMeterProvider, KafkaConfig kafkaConfig) {
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
        sdkMeterProvider.registerMetricReader(prometheusHttpServer);
    }

    public void shutdown() {
        if (openTelemetrySdk != null) {
            openTelemetrySdk.shutdown();
        }
        if (prometheusHttpServer != null) {
            prometheusHttpServer.shutdown();
        }
    }
}
