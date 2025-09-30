package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.common.OTLPCompressionType;
import com.automq.opentelemetry.common.OTLPProtocol;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;

public class OTLPMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OTLPMetricsExporter.class);
    private final long intervalMs;
    private final String endpoint;
    private final OTLPProtocol protocol;
    private final OTLPCompressionType compression;
    private final long timeoutMs;
    // Default timeout for OTLP exporters
    private static final long DEFAULT_EXPORTER_TIMEOUT_MS = 30000;
    

    public OTLPMetricsExporter(long intervalMs, String endpoint, String protocol, String compression, long timeoutMs) {
        if (StringUtils.isBlank(endpoint) || "null".equals(endpoint)) {
            throw new IllegalArgumentException("OTLP endpoint is required");
        }
        this.intervalMs = intervalMs;
        this.endpoint = endpoint;
        this.protocol = OTLPProtocol.fromString(protocol);
        this.compression = OTLPCompressionType.fromString(compression);
        this.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_EXPORTER_TIMEOUT_MS;
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

    public long intervalMs() {
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
                        .setTimeout(Duration.ofMillis(timeoutMs));
                builder = PeriodicMetricReader.builder(otlpExporterBuilder.build());
                break;
            case HTTP:
                OtlpHttpMetricExporterBuilder otlpHttpExporterBuilder = OtlpHttpMetricExporter.builder()
                        .setEndpoint(endpoint)
                        .setCompression(compression.getType())
                        .setTimeout(Duration.ofMillis(timeoutMs));
                builder = PeriodicMetricReader.builder(otlpHttpExporterBuilder.build());
                break;
            default:
                throw new IllegalArgumentException("Unsupported OTLP protocol: " + protocol);
        }

        return builder.setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
