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

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.log.stream.s3.telemetry.MetricsConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
