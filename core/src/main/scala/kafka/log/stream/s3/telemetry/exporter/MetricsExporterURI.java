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

import kafka.server.KafkaConfig;

import org.apache.kafka.common.utils.Utils;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.utils.URIUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.annotations.NotNull;

public class MetricsExporterURI {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporterURI.class);
    private final List<MetricsExporter> metricsExporters;

    public MetricsExporterURI(List<MetricsExporter> metricsExporters) {
        this.metricsExporters = metricsExporters == null ? new ArrayList<>() : metricsExporters;
    }

    public static MetricsExporter parseExporter(String clusterId, KafkaConfig kafkaConfig, String uriStr) {
        try {
            URI uri = new URI(uriStr);
            String type = uri.getScheme();
            if (Utils.isBlank(type)) {
                LOGGER.error("Invalid metrics exporter URI: {}, exporter type is missing", uriStr);
                return null;
            }
            Map<String, List<String>> queries = URIUtils.splitQuery(uri);
            return parseExporter(clusterId, kafkaConfig, type, queries, uriStr);
        } catch (Exception e) {
            LOGGER.warn("Parse metrics exporter URI {} failed", uriStr, e);
            return null;
        }
    }

    public static MetricsExporter parseExporter(String clusterId, KafkaConfig kafkaConfig, String type, Map<String, List<String>> queries, String uriStr) {
        MetricsExporterType exporterType = MetricsExporterType.fromString(type);
        switch (exporterType) {
            case OTLP:
                return buildOTLPExporter(kafkaConfig.s3ExporterReportIntervalMs(), queries);
            case PROMETHEUS:
                return buildPrometheusExporter(queries, kafkaConfig.automq().baseLabels());
            case OPS:
                return buildOpsExporter(clusterId, kafkaConfig.nodeId(), kafkaConfig.s3ExporterReportIntervalMs(),
                    kafkaConfig.automq().opsBuckets(), kafkaConfig.automq().baseLabels());
            case KAFKA:
                return buildKafkaExporter(uriStr);
            default:
                return null;
        }
    }

    public static @NotNull MetricsExporterURI parse(String clusterId, KafkaConfig kafkaConfig) {
        String uriStr = kafkaConfig.automq().metricsExporterURI();
        if (Utils.isBlank(uriStr)) {
            return new MetricsExporterURI(Collections.emptyList());
        }
        String[] exporterUri = uriStr.split(",");
        if (exporterUri.length == 0) {
            return new MetricsExporterURI(Collections.emptyList());
        }
        List<MetricsExporter> exporters = new ArrayList<>();
        for (String uri : exporterUri) {
            if (Utils.isBlank(uri)) {
                continue;
            }
            MetricsExporter exporter = parseExporter(clusterId, kafkaConfig, uri);
            if (exporter != null) {
                exporters.add(exporter);
            }
        }
        return new MetricsExporterURI(exporters);
    }

    public static MetricsExporter buildOTLPExporter(int intervalMs, Map<String, List<String>> queries) {
        String endpoint = URIUtils.getString(queries, ExporterConstants.ENDPOINT, "");
        String protocol = URIUtils.getString(queries, ExporterConstants.PROTOCOL, OTLPProtocol.GRPC.getProtocol());
        String compression = URIUtils.getString(queries, ExporterConstants.COMPRESSION, OTLPCompressionType.NONE.getType());
        return new OTLPMetricsExporter(intervalMs, endpoint, protocol, compression);
    }

    public static MetricsExporter buildPrometheusExporter(Map<String, List<String>> queries, List<Pair<String, String>> baseLabels) {
        String host = URIUtils.getString(queries, ExporterConstants.HOST, ExporterConstants.DEFAULT_PROM_HOST);
        int port = Integer.parseInt(URIUtils.getString(queries, ExporterConstants.PORT, String.valueOf(ExporterConstants.DEFAULT_PROM_PORT)));
        return new PrometheusMetricsExporter(host, port, baseLabels);
    }

    public static MetricsExporter buildOpsExporter(String clusterId, int nodeId, int intervalMs, List<BucketURI> opsBuckets,
                                                   List<Pair<String, String>> baseLabels) {
        return new OpsMetricsExporter(clusterId, nodeId, intervalMs, opsBuckets, baseLabels);
    }

    public static MetricsExporter buildKafkaExporter(String uriStr) {
        return new KafkaMetricsExporter(uriStr);
    }

    public List<MetricsExporter> metricsExporters() {
        return metricsExporters;
    }

}
