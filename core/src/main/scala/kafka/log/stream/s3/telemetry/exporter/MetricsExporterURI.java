/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.exporter;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.utils.URIUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import kafka.server.KafkaConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsExporterURI {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsExporterURI.class);
    private final List<MetricsExporter> metricsExporters;

    public MetricsExporterURI(List<MetricsExporter> metricsExporters) {
        this.metricsExporters = metricsExporters;
    }

    public static MetricsExporterURI parse(String clusterId, KafkaConfig kafkaConfig) {
        if (kafkaConfig.s3MetricsExporterURI() == null) {
            return backwardCompatibleParse(clusterId, kafkaConfig);
        }
        return parse(clusterId, kafkaConfig, kafkaConfig.s3MetricsExporterURI());
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
            MetricsExporterType exporterType = MetricsExporterType.fromString(type);
            switch (exporterType) {
                case OTLP:
                    return buildOTLPExporter(kafkaConfig.s3ExporterReportIntervalMs(), queries);
                case PROMETHEUS:
                    return buildPrometheusExporter(queries);
                case OPS:
                    return buildOpsExporter(clusterId, kafkaConfig.nodeId(), kafkaConfig.s3ExporterReportIntervalMs(),
                        kafkaConfig.automq().opsBuckets());
                default:
                    LOGGER.error("Unsupported metrics exporter type: {}", exporterType);
                    return null;
            }
        } catch (Exception e) {
            LOGGER.error("Invalid metrics exporter URI: {}", uriStr, e);
            return null;
        }
    }

    public static MetricsExporterURI parse(String clusterId, KafkaConfig kafkaConfig, String uriStr) {
        if (Utils.isBlank(uriStr)) {
            return null;
        }
        String[] exporterUri = uriStr.split(",");
        if (exporterUri.length == 0) {
            return null;
        }
        List<MetricsExporter> exporters = new ArrayList<>();
        for (String uri : exporterUri) {
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

    public static MetricsExporter buildPrometheusExporter(Map<String, List<String>> queries) {
        String host = URIUtils.getString(queries, ExporterConstants.HOST, ExporterConstants.DEFAULT_PROM_HOST);
        int port = Integer.parseInt(URIUtils.getString(queries, ExporterConstants.PORT, String.valueOf(ExporterConstants.DEFAULT_PROM_PORT)));
        return new PrometheusMetricsExporter(host, port);
    }

    public static MetricsExporter buildOpsExporter(String clusterId, int nodeId, int intervalMs, List<BucketURI> opsBuckets) {
        return new OpsMetricsExporter(clusterId, nodeId, intervalMs, opsBuckets);
    }

    /**
     * For backward compatibility, will be deprecated soon.
     */
    private static MetricsExporterURI backwardCompatibleParse(String clusterId, KafkaConfig kafkaConfig) {
        if (!kafkaConfig.s3MetricsEnable()) {
            return null;
        }
        List<String> exportedUris = new ArrayList<>();
        String exporterTypes = kafkaConfig.s3MetricsExporterType();
        if (!StringUtils.isBlank(exporterTypes)) {
            String[] exporterTypeArray = exporterTypes.split(",");
            for (String exporterType : exporterTypeArray) {
                exporterType = exporterType.trim();
                switch (exporterType) {
                    case "otlp":
                        exportedUris.add(buildOTLPExporterURI(kafkaConfig));
                        break;
                    case "prometheus":
                        exportedUris.add(buildPrometheusExporterURI(kafkaConfig));
                        break;
                    default:
                        LOGGER.error("illegal metrics exporter type: {}", exporterType);
                        break;
                }
            }
        }

        if (kafkaConfig.s3OpsTelemetryEnabled()) {
            exportedUris.add(buildOpsExporterURI());
        }

        return parse(clusterId, kafkaConfig, String.join(",", exportedUris));
    }

    private static String buildOTLPExporterURI(KafkaConfig kafkaConfig) {
        StringBuilder uriBuilder = new StringBuilder()
            .append(ExporterConstants.OTLP_TYPE)
            .append(ExporterConstants.URI_DELIMITER)
            .append(ExporterConstants.ENDPOINT).append("=").append(kafkaConfig.s3ExporterOTLPEndpoint())
            .append("&")
            .append(ExporterConstants.PROTOCOL).append("=").append(kafkaConfig.s3ExporterOTLPProtocol());
        if (kafkaConfig.s3ExporterOTLPCompressionEnable()) {
            uriBuilder.append("&").append(ExporterConstants.COMPRESSION).append("=").append("gzip");
        }
        return uriBuilder.toString();
    }

    private static String buildPrometheusExporterURI(KafkaConfig kafkaConfig) {
        return ExporterConstants.PROMETHEUS_TYPE + ExporterConstants.URI_DELIMITER +
            ExporterConstants.HOST + "=" + kafkaConfig.s3MetricsExporterPromHost() + "&" +
            ExporterConstants.PORT + "=" + kafkaConfig.s3MetricsExporterPromPort();
    }

    private static String buildOpsExporterURI() {
        return ExporterConstants.OPS_TYPE + ExporterConstants.URI_DELIMITER;
    }

    public List<MetricsExporter> metricsExporters() {
        return metricsExporters;
    }

}
