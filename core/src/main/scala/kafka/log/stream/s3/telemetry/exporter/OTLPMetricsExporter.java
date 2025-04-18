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

package kafka.log.stream.s3.telemetry.exporter;

import org.apache.kafka.common.utils.Utils;

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
