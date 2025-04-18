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
        Mockito.when(kafkaConfig.getBoolean(AutoMQConfig.S3_METRICS_ENABLE_CONFIG)).thenReturn(false);
        AutoMQConfig automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        MetricsExporterURI uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNull(uri);

        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.nodeId()).thenReturn(1);
        Mockito.when(kafkaConfig.getBoolean(AutoMQConfig.S3_METRICS_ENABLE_CONFIG)).thenReturn(true);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_TYPE_CONFIG)).thenReturn("otlp,prometheus");
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_ENDPOINT_CONFIG)).thenReturn("http://localhost:4318");
        Mockito.when(kafkaConfig.getBoolean(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_COMPRESSION_ENABLE_CONFIG)).thenReturn(true);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_EXPORTER_OTLP_PROTOCOL_CONFIG)).thenReturn("http");
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_METRICS_EXPORTER_PROM_HOST_CONFIG)).thenReturn("127.0.0.1");
        Mockito.when(kafkaConfig.getInt(AutoMQConfig.S3_METRICS_EXPORTER_PROM_PORT_CONFIG)).thenReturn(9999);
        Mockito.when(kafkaConfig.getBoolean(AutoMQConfig.S3_TELEMETRY_OPS_ENABLED_CONFIG)).thenReturn(true);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_OPS_BUCKETS_CONFIG)).thenReturn("0@s3://bucket0?region=us-west-1");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        Mockito.when(kafkaConfig.s3ExporterReportIntervalMs()).thenReturn(1000);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertEquals(3, uri.metricsExporters().size());
        for (MetricsExporter metricsExporter : uri.metricsExporters()) {
            if (metricsExporter instanceof OTLPMetricsExporter) {
                OTLPMetricsExporter otlpExporter = (OTLPMetricsExporter) metricsExporter;
                Assertions.assertEquals(1000, otlpExporter.intervalMs());
                Assertions.assertEquals("http://localhost:4318", otlpExporter.endpoint());
                Assertions.assertEquals(OTLPProtocol.HTTP, otlpExporter.protocol());
                Assertions.assertEquals(OTLPCompressionType.GZIP, otlpExporter.compression());
            } else if (metricsExporter instanceof PrometheusMetricsExporter) {
                PrometheusMetricsExporter promExporter = (PrometheusMetricsExporter) metricsExporter;
                Assertions.assertEquals("127.0.0.1", promExporter.host());
                Assertions.assertEquals(9999, promExporter.port());
            } else if (metricsExporter instanceof OpsMetricsExporter) {
                OpsMetricsExporter opsExporter = (OpsMetricsExporter) metricsExporter;
                Assertions.assertEquals(clusterId, opsExporter.clusterId());
                Assertions.assertEquals(1, opsExporter.nodeId());
                Assertions.assertEquals(1000, opsExporter.intervalMs());
                Assertions.assertEquals(1, opsExporter.opsBuckets().size());
                Assertions.assertEquals("bucket0", opsExporter.opsBuckets().get(0).bucket());
                Assertions.assertEquals("us-west-1", opsExporter.opsBuckets().get(0).region());
            } else {
                Assertions.fail("Unknown exporter type");
            }
        }
    }

    @Test
    public void testParseURIString() {
        String clusterId = "test_cluster";
        // test empty exporter
        KafkaConfig kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn(null);
        Mockito.when(kafkaConfig.getBoolean(AutoMQConfig.S3_METRICS_ENABLE_CONFIG)).thenReturn(false);
        AutoMQConfig automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        MetricsExporterURI uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNull(uri);

        // test invalid uri
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("unknown://");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test invalid type
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("unknown://?");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("://?");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal otlp config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("otlp://?endpoint=&protocol=grpc");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal prometheus config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("prometheus://?host=&port=9999");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test illegal ops config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn("ops://?");
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_OPS_BUCKETS_CONFIG)).thenReturn("");
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);
        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertTrue(uri.metricsExporters().isEmpty());

        // test multi exporter config
        kafkaConfig = Mockito.mock(KafkaConfig.class);
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn(
            "otlp://?endpoint=http://localhost:4317&protocol=http&compression=gzip," +
            "prometheus://?host=127.0.0.1&port=9999," +
            "ops://?");
        Mockito.when(kafkaConfig.getString(AutoMQConfig.S3_OPS_BUCKETS_CONFIG)).thenReturn("0@s3://bucket0?region=us-west-1");
        Mockito.when(kafkaConfig.s3ExporterReportIntervalMs()).thenReturn(1000);
        Mockito.when(kafkaConfig.nodeId()).thenReturn(1);
        automqConfig = new AutoMQConfig();
        automqConfig.setup(kafkaConfig);
        Mockito.when(kafkaConfig.automq()).thenReturn(automqConfig);

        uri = MetricsExporterURI.parse(clusterId, kafkaConfig);
        Assertions.assertNotNull(uri);
        Assertions.assertEquals(3, uri.metricsExporters().size());
        for (MetricsExporter metricsExporter : uri.metricsExporters()) {
            if (metricsExporter instanceof OTLPMetricsExporter) {
                OTLPMetricsExporter otlpExporter = (OTLPMetricsExporter) metricsExporter;
                Assertions.assertEquals(1000, otlpExporter.intervalMs());
                Assertions.assertEquals("http://localhost:4317", otlpExporter.endpoint());
                Assertions.assertEquals(OTLPProtocol.HTTP, otlpExporter.protocol());
                Assertions.assertEquals(OTLPCompressionType.GZIP, otlpExporter.compression());
                Assertions.assertNotNull(metricsExporter.asMetricReader());
            } else if (metricsExporter instanceof PrometheusMetricsExporter) {
                PrometheusMetricsExporter promExporter = (PrometheusMetricsExporter) metricsExporter;
                Assertions.assertEquals("127.0.0.1", promExporter.host());
                Assertions.assertEquals(9999, promExporter.port());
                Assertions.assertNotNull(metricsExporter.asMetricReader());
            } else if (metricsExporter instanceof OpsMetricsExporter) {
                OpsMetricsExporter opsExporter = (OpsMetricsExporter) metricsExporter;
                Assertions.assertEquals(clusterId, opsExporter.clusterId());
                Assertions.assertEquals(1, opsExporter.nodeId());
                Assertions.assertEquals(1000, opsExporter.intervalMs());
                Assertions.assertEquals(1, opsExporter.opsBuckets().size());
                Assertions.assertEquals("bucket0", opsExporter.opsBuckets().get(0).bucket());
                Assertions.assertEquals("us-west-1", opsExporter.opsBuckets().get(0).region());
            } else {
                Assertions.fail("Unknown exporter type");
            }
        }
    }
}
