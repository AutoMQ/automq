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

import kafka.log.stream.s3.telemetry.MetricsConstants;

import org.apache.kafka.common.utils.Utils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class PrometheusMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsExporter.class);
    private final String host;
    private final int port;
    private final Set<String> baseLabelKeys;

    public PrometheusMetricsExporter(String host, int port, List<Pair<String, String>> baseLabels) {
        if (Utils.isBlank(host)) {
            throw new IllegalArgumentException("Illegal Prometheus host");
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Illegal Prometheus port");
        }
        this.host = host;
        this.port = port;
        this.baseLabelKeys = baseLabels.stream().map(Pair::getKey).collect(Collectors.toSet());
        LOGGER.info("PrometheusMetricsExporter initialized with host: {}, port: {}", host, port);
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public MetricReader asMetricReader() {
        return PrometheusHttpServer.builder()
            .setHost(host)
            .setPort(port)
            .setAllowedResourceAttributesFilter(resourceAttributes ->
                MetricsConstants.JOB.equals(resourceAttributes)
                    || MetricsConstants.INSTANCE.equals(resourceAttributes)
                    || MetricsConstants.HOST_NAME.equals(resourceAttributes)
                    || baseLabelKeys.contains(resourceAttributes))
            .build();
    }
}
