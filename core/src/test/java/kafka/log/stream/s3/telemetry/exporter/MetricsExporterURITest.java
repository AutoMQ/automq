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
import java.util.Collections;
import java.util.List;
import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MetricsExporterURITest {

    @Test
    public void testsBackwardCompatibility() {
        String clusterId = "test_cluster";

        KafkaConfig kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsEnable()).thenReturn(false);
        MetricsExporterURI uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNull(uri);

        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.nodeId()).thenReturn(1);
        Mockito.when(kafkaConfig.s3MetricsEnable()).thenReturn(true);
        Mockito.when(kafkaConfig.s3MetricsExporterType()).thenReturn("otlp,prometheus");
        Mockito.when(kafkaConfig.s3ExporterOTLPEndpoint()).thenReturn("http://localhost:4318");
        Mockito.when(kafkaConfig.s3ExporterOTLPCompressionEnable()).thenReturn(true);
        Mockito.when(kafkaConfig.s3ExporterOTLPProtocol()).thenReturn("http");
        Mockito.when(kafkaConfig.s3MetricsExporterPromHost()).thenReturn("127.0.0.1");
        Mockito.when(kafkaConfig.s3MetricsExporterPromPort()).thenReturn(9999);
        Mockito.when(kafkaConfig.s3OpsTelemetryEnabled()).thenReturn(true);
        AutoMQConfig mockAutoMQ = Mockito.mock(AutoMQConfig.class);
        Mockito.when(mockAutoMQ.opsBuckets()).thenReturn(List.of(BucketURI.parse("0@s3://bucket0?region=us-west-1")));
        Mockito.when(kafkaConfig.automq()).thenReturn(mockAutoMQ);
        Mockito.when(kafkaConfig.s3ExporterReportIntervalMs()).thenReturn(1000);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertEquals(3, uri.metricsExporters().size());
        for (MetricsExporter metricsExporter : uri.metricsExporters()) {
            switch (metricsExporter.type()) {
                case OTLP:
                    OTLPMetricsExporter otlpExporter = (OTLPMetricsExporter) metricsExporter;
                    Assertions.assertEquals(1000, otlpExporter.intervalMs());
                    Assertions.assertEquals("http://localhost:4318", otlpExporter.endpoint());
                    Assertions.assertEquals(OTLPProtocol.HTTP, otlpExporter.protocol());
                    Assertions.assertEquals(OTLPCompressionType.GZIP, otlpExporter.compression());
                    break;
                case PROMETHEUS:
                    PrometheusMetricsExporter promExporter = (PrometheusMetricsExporter) metricsExporter;
                    Assertions.assertEquals("127.0.0.1", promExporter.host());
                    Assertions.assertEquals(9999, promExporter.port());
                    break;
                case OPS:
                    OpsMetricsExporter opsExporter = (OpsMetricsExporter) metricsExporter;
                    Assertions.assertEquals(clusterId, opsExporter.clusterId());
                    Assertions.assertEquals(1, opsExporter.nodeId());
                    Assertions.assertEquals(1000, opsExporter.intervalMs());
                    Assertions.assertEquals(1, opsExporter.opsBuckets().size());
                    Assertions.assertEquals("bucket0", opsExporter.opsBuckets().get(0).bucket());
                    Assertions.assertEquals("us-west-1", opsExporter.opsBuckets().get(0).region());
                    break;

            }
        }
    }

    @Test
    public void testParseURIString() {
        String clusterId = "test_cluster";
        // test empty exporter
        KafkaConfig kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn(null);
        Mockito.when(kafkaConfig.s3MetricsEnable()).thenReturn(false);
        MetricsExporterURI uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNull(uri);

        // test invalid uri
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("unknown://");
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test invalid type
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("unknown://?");
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("://?");
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal otlp config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("otlp://?endpoint=&protocol=grpc");
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal prometheus config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("prometheus://?host=&port=9999");
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal ops config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn("ops://?");
        AutoMQConfig mockAutoMQ = Mockito.mock(AutoMQConfig.class);
        Mockito.when(mockAutoMQ.opsBuckets()).thenReturn(Collections.emptyList());
        Mockito.when(kafkaConfig.automq()).thenReturn(mockAutoMQ);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test multi exporter config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.s3MetricsExporterURI()).thenReturn(
            "otlp://?endpoint=http://localhost:4317&protocol=http&compression=gzip," +
            "prometheus://?host=127.0.0.1&port=9999," +
            "ops://?");
        mockAutoMQ = Mockito.mock(AutoMQConfig.class);
        Mockito.when(mockAutoMQ.opsBuckets()).thenReturn(List.of(BucketURI.parse("0@s3://bucket0?region=us-west-1")));
        Mockito.when(kafkaConfig.automq()).thenReturn(mockAutoMQ);
        Mockito.when(kafkaConfig.s3ExporterReportIntervalMs()).thenReturn(1000);
        Mockito.when(kafkaConfig.nodeId()).thenReturn(1);

        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertEquals(3, uri.metricsExporters().size());
        for (MetricsExporter metricsExporter : uri.metricsExporters()) {
            switch (metricsExporter.type()) {
                case OTLP:
                    OTLPMetricsExporter otlpExporter = (OTLPMetricsExporter) metricsExporter;
                    Assertions.assertEquals(1000, otlpExporter.intervalMs());
                    Assertions.assertEquals("http://localhost:4317", otlpExporter.endpoint());
                    Assertions.assertEquals(OTLPProtocol.HTTP, otlpExporter.protocol());
                    Assertions.assertEquals(OTLPCompressionType.GZIP, otlpExporter.compression());
                    Assertions.assertNotNull(metricsExporter.asMetricReader());
                    break;
                case PROMETHEUS:
                    PrometheusMetricsExporter promExporter = (PrometheusMetricsExporter) metricsExporter;
                    Assertions.assertEquals("127.0.0.1", promExporter.host());
                    Assertions.assertEquals(9999, promExporter.port());
                    Assertions.assertNotNull(metricsExporter.asMetricReader());
                    break;
                case OPS:
                    OpsMetricsExporter opsExporter = (OpsMetricsExporter) metricsExporter;
                    Assertions.assertEquals(clusterId, opsExporter.clusterId());
                    Assertions.assertEquals(1, opsExporter.nodeId());
                    Assertions.assertEquals(1000, opsExporter.intervalMs());
                    Assertions.assertEquals(1, opsExporter.opsBuckets().size());
                    Assertions.assertEquals("bucket0", opsExporter.opsBuckets().get(0).bucket());
                    Assertions.assertEquals("us-west-1", opsExporter.opsBuckets().get(0).region());
                    break;
            }
        }
    }
}
