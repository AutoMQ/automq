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

package kafka.log.stream.s3.telemetry.exporter;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;
import java.time.Duration;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OTLPMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OTLPMetricsExporter.class);
    private final int intervalMs;
    private final String endpoint;
    private final OTLPProtocol protocol;
    private final OTLPCompressionType compression;

    public OTLPMetricsExporter(int intervalMs, String endpoint, String protocol, String compression) {
        if (Utils.isBlank(endpoint) || "null".equals(endpoint)) {
            throw new IllegalArgumentException("OTLP endpoint is required");
        }
        this.intervalMs = intervalMs;
        this.endpoint = endpoint;
        this.protocol = OTLPProtocol.fromString(protocol);
        this.compression = OTLPCompressionType.fromString(compression);
        LOGGER.info("OTLPMetricsExporter initialized with endpoint: {}, protocol: {}, compression: {}, intervalMs: {}",
            endpoint, protocol, compression, intervalMs);
    }

    public String endpoint() {
        return endpoint;
    }

    public OTLPProtocol protocol() {
        return protocol;
    }

    public OTLPCompressionType compression() {
        return compression;
    }

    public int intervalMs() {
        return intervalMs;
    }

    @Override
    public MetricReader asMetricReader() {
        PeriodicMetricReaderBuilder builder;
        switch (protocol) {
            case GRPC:
                OtlpGrpcMetricExporterBuilder otlpExporterBuilder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(endpoint)
                    .setCompression(compression.getType())
                    .setTimeout(Duration.ofMillis(ExporterConstants.DEFAULT_EXPORTER_TIMEOUT_MS));
                builder = PeriodicMetricReader.builder(otlpExporterBuilder.build());
                break;
            case HTTP:
                OtlpHttpMetricExporterBuilder otlpHttpExporterBuilder = OtlpHttpMetricExporter.builder()
                    .setEndpoint(endpoint)
                    .setCompression(compression.getType())
                    .setTimeout(Duration.ofMillis(ExporterConstants.DEFAULT_EXPORTER_TIMEOUT_MS));
                builder = PeriodicMetricReader.builder(otlpHttpExporterBuilder.build());
                break;
            default:
                throw new IllegalArgumentException("Unsupported OTLP protocol: " + protocol);
        }

        return builder.setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
