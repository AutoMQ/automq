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

package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.TelemetryConstants;
import com.sun.net.httpserver.Authenticator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServerBuilder;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class PrometheusMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsExporter.class);
    private final String host;
    private final int port;
    private final Set<String> baseLabelKeys;
    private final Authenticator authenticator;

    public PrometheusMetricsExporter(String host, int port, List<Pair<String, String>> baseLabels) {
        this(host, port, baseLabels, null);
    }

    public PrometheusMetricsExporter(String host, int port, List<Pair<String, String>> baseLabels,
        Authenticator authenticator) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Illegal Prometheus host");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Illegal Prometheus port");
        }
        this.host = host;
        this.port = port;
        this.baseLabelKeys = baseLabels.stream().map(Pair::getKey).collect(Collectors.toSet());
        this.authenticator = authenticator;
        LOGGER.info("PrometheusMetricsExporter initialized with host: {}, port: {}, labels: {}, authentication: {}",
            host, port, baseLabels, authenticator == null ? "disabled" : "enabled");
    }

    @Override
    public MetricReader asMetricReader() {
        PrometheusHttpServerBuilder builder = PrometheusHttpServer.builder()
            .setHost(host)
            .setPort(port)
            .setMemoryMode(MemoryMode.IMMUTABLE_DATA)
            // This filter is to align with the original behavior, allowing only specific resource attributes
            // to be converted to prometheus labels.
            .setAllowedResourceAttributesFilter(resourceAttributeKey ->
                TelemetryConstants.PROMETHEUS_JOB_KEY.equals(resourceAttributeKey)
                    || TelemetryConstants.PROMETHEUS_INSTANCE_KEY.equals(resourceAttributeKey)
                    || TelemetryConstants.HOST_NAME_KEY.equals(resourceAttributeKey)
                    || baseLabelKeys.contains(resourceAttributeKey));
        if (authenticator != null) {
            builder.setAuthenticator(authenticator);
        }
        return builder.build();
    }
}
