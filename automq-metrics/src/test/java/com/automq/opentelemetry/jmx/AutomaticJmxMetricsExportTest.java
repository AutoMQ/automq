/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.opentelemetry.jmx;

import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.opentelemetry.exporter.PrometheusMetricsExporter;
import com.automq.opentelemetry.exporter.s3.S3MetricsExporter;
import com.automq.stream.s3.operator.ObjectStorage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises automatic JMX discovery through a shared SDK and the real OPS and Prometheus exporters.
 */
@Tag("S3Unit")
public class AutomaticJmxMetricsExportTest {

    /**
     * Given a new MBean after startup, both exporters publish the same values and stop after removal or close.
     */
    @Test
    public void testLateMBeanReachesBothExportersWithoutConnectorConfiguration() throws Exception {
        try (ExportHarness harness = new ExportHarness(20)) {
            harness.collector.start();
            ExportProbe probe = new ExportProbe(37);
            harness.register(probe);

            await(() -> harness.scrape().contains("jmx_domain=\"" + harness.domain + "\""));
            JsonNode value = harness.findOpsAttribute("Value");
            JsonNode connected = harness.findOpsAttribute("Connected");
            assertEquals(37.0, value.path("gauge").path("value").asDouble());
            assertEquals(1.0, connected.path("gauge").path("value").asDouble());
            assertCommonLabels(value, harness);
            String metricName = value.path("name").asText();
            assertTrue(metricName.startsWith("connect_jmx_"));
            assertTrue(metricName.endsWith("_value"));
            assertPrometheusSample(harness.scrape(), metricName, 37.0, harness);
            assertPrometheusSample(harness.scrape(), connected.path("name").asText(), 1.0, harness);

            probe.value.set(11);
            await(() -> hasPrometheusValue(harness.scrape(), metricName, 11.0));
            assertEquals(11.0, harness.findOpsAttribute("Value").path("gauge").path("value").asDouble());

            harness.unregister();
            await(() -> !harness.scrape().contains("jmx_domain=\"" + harness.domain + "\""));
            assertFalse(harness.readOps().stream().anyMatch(harness::belongsToProbe));

            harness.register(probe);
            await(() -> hasPrometheusValue(harness.scrape(), metricName, 11.0));
            assertEquals(11.0, harness.findOpsAttribute("Value").path("gauge").path("value").asDouble());
            harness.collector.close();
            int readsBeforeClose = probe.reads.get();
            harness.unregister();
            harness.register(probe);
            assertFalse(harness.scrape().contains("jmx_domain=\"" + harness.domain + "\""));
            assertFalse(harness.readOps().stream().anyMatch(harness::belongsToProbe));
            assertEquals(readsBeforeClose, probe.reads.get());
        }
    }

    /**
     * Given two readers, collecting the same sampled snapshot never re-invokes an MBean getter.
     */
    @Test
    public void testMultipleReadersReuseOneJmxSample() throws Exception {
        try (ExportHarness harness = new ExportHarness(TimeUnit.MINUTES.toMillis(1))) {
            ExportProbe probe = new ExportProbe(59);
            harness.register(probe);
            harness.collector.start();
            await(() -> harness.scrape().contains("jmx_domain=\"" + harness.domain + "\""));
            int sampledReads = probe.reads.get();
            assertTrue(sampledReads > 0);

            for (int i = 0; i < 3; i++) {
                JsonNode value = harness.findOpsAttribute("Value");
                assertEquals(59.0, value.path("gauge").path("value").asDouble());
                assertPrometheusSample(harness.scrape(), value.path("name").asText(), 59.0, harness);
            }

            assertEquals(sampledReads, probe.reads.get(), "Export readers must use the collector snapshot");
        }
    }

    private static void assertCommonLabels(JsonNode metric, ExportHarness harness) {
        JsonNode tags = metric.path("tags");
        assertEquals(harness.domain, tags.path("jmx_domain").asText());
        assertEquals("test-source", tags.path("jmx_prop_connector").asText());
        assertEquals("7", tags.path("jmx_prop_task").asText());
        assertEquals(harness.objectName.getCanonicalName(), tags.path("jmx_object_name").asText());
        assertEquals("connect-export-test", tags.path("connect_cluster_id").asText());
        assertEquals("instance-export-test", tags.path("automq_instance_id").asText());
        assertEquals("environment-export-test", tags.path("environment_id").asText());
    }

    private static void assertPrometheusSample(String scrape, String name, double value, ExportHarness harness) {
        String sample = sampleLine(scrape, name);
        assertFalse(sample.isEmpty(), "Prometheus must expose the OPS metric name: " + name);
        assertTrue(sample.contains("jmx_domain=\"" + harness.domain + "\""), sample);
        assertTrue(sample.contains("jmx_prop_connector=\"test-source\""), sample);
        assertTrue(sample.contains("connect_cluster_id=\"connect-export-test\""), sample);
        assertTrue(sample.contains("automq_instance_id=\"instance-export-test\""), sample);
        assertTrue(sample.contains("environment_id=\"environment-export-test\""), sample);
        assertTrue(hasPrometheusValue(scrape, name, value), sample);
    }

    private static boolean hasPrometheusValue(String scrape, String name, double expected) {
        String sample = sampleLine(scrape, name);
        if (sample.isEmpty()) {
            return false;
        }
        String value = sample.substring(sample.lastIndexOf('}') + 1).trim().split("\\s+")[0];
        return Double.parseDouble(value) == expected;
    }

    private static String sampleLine(String scrape, String name) {
        return scrape.lines().filter(line -> line.startsWith(name + "{")).findFirst().orElse("");
    }

    private static void await(CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.matches()) {
            assertTrue(System.nanoTime() < deadline, "Timed out waiting for JMX exporter state");
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface CheckedCondition {
        boolean matches() throws Exception;
    }

    private static final class ExportHarness implements AutoCloseable {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        private final String domain = "automq.export.test." + UUID.randomUUID().toString().replace("-", "");
        private final ObjectName objectName;
        private final List<String> uploadedObjects = new ArrayList<>();
        private final S3MetricsExporter opsExporter;
        private final PeriodicMetricReader opsReader;
        private final SdkMeterProvider provider;
        private final AutomaticJmxMetricsCollector collector;
        private final HttpClient httpClient = HttpClient.newHttpClient();
        private final URI scrapeUri;

        private ExportHarness(long samplingIntervalMs) throws Exception {
            objectName = new ObjectName(domain + ":type=SourceConnector,connector=test-source,task=7");
            List<Pair<String, String>> labels = List.of(
                Pair.of("connect_cluster_id", "connect-export-test"),
                Pair.of("automq_instance_id", "instance-export-test"),
                Pair.of("environment_id", "environment-export-test"));
            Attributes baseAttributes = Attributes.builder()
                .put("connect_cluster_id", "connect-export-test")
                .put("automq_instance_id", "instance-export-test")
                .put("environment_id", "environment-export-test")
                .build();
            ObjectStorage objectStorage = mock(ObjectStorage.class);
            when(objectStorage.write(any(), anyString(), any())).thenAnswer(invocation -> {
                ByteBuf compressed = invocation.getArgument(2);
                byte[] bytes = new byte[compressed.readableBytes()];
                compressed.readBytes(bytes);
                compressed.release();
                try (GZIPInputStream input = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                    uploadedObjects.add(new String(input.readAllBytes(), StandardCharsets.UTF_8));
                }
                return CompletableFuture.completedFuture(null);
            });
            MetricsExportConfig config = mock(MetricsExportConfig.class);
            when(config.clusterId()).thenReturn("connect-export-test");
            when(config.nodeId()).thenReturn(7);
            when(config.baseLabels()).thenReturn(labels);
            when(config.objectStorage()).thenReturn(objectStorage);
            opsExporter = new S3MetricsExporter(config);
            opsReader = PeriodicMetricReader.builder(opsExporter).setInterval(Duration.ofDays(1)).build();
            int port;
            try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                port = socket.getLocalPort();
            }
            MetricReader prometheus = new PrometheusMetricsExporter("127.0.0.1", port, labels).asMetricReader();
            scrapeUri = URI.create("http://127.0.0.1:" + port + "/metrics");
            provider = SdkMeterProvider.builder()
                .setResource(Resource.create(baseAttributes))
                .registerMetricReader(opsReader)
                .registerMetricReader(prometheus)
                .build();
            collector = new AutomaticJmxMetricsCollector(provider.get("jmx-export-test"), samplingIntervalMs, baseAttributes);
        }

        private void register(ExportProbe probe) throws Exception {
            server.registerMBean(new StandardMBean(probe, ExportProbeMBean.class), objectName);
        }

        private void unregister() throws Exception {
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        }

        private String scrape() throws Exception {
            HttpRequest request = HttpRequest.newBuilder(scrapeUri).timeout(Duration.ofSeconds(5)).GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            return response.body();
        }

        private List<JsonNode> readOps() throws Exception {
            uploadedObjects.clear();
            assertTrue(opsReader.forceFlush().join(10, TimeUnit.SECONDS).isSuccess());
            assertTrue(opsExporter.flush().isSuccess());
            List<JsonNode> result = new ArrayList<>();
            for (String object : uploadedObjects) {
                for (String line : object.split("\n")) {
                    if (!line.isBlank()) {
                        result.add(MAPPER.readTree(line));
                    }
                }
            }
            return result;
        }

        private JsonNode findOpsAttribute(String attribute) throws Exception {
            return readOps().stream()
                .filter(this::belongsToProbe)
                .filter(metric -> attribute.equals(metric.path("tags").path("jmx_attribute").asText()))
                .findFirst().orElseThrow(() -> new AssertionError("OPS did not export JMX attribute " + attribute));
        }

        private boolean belongsToProbe(JsonNode metric) {
            return domain.equals(metric.path("tags").path("jmx_domain").asText());
        }

        @Override
        public void close() throws Exception {
            collector.close();
            unregister();
            provider.close();
            opsExporter.close();
        }
    }

    /**
     * Defines ordinary numerical and boolean attributes without any connector-specific metric profile.
     */
    public interface ExportProbeMBean {
        /** Returns the current numeric value observed by the collector. */
        long getValue();

        /** Returns the Boolean connection state observed by the collector. */
        boolean isConnected();
    }

    private static final class ExportProbe implements ExportProbeMBean {
        private final AtomicLong value;
        private final AtomicInteger reads = new AtomicInteger();

        private ExportProbe(long value) {
            this.value = new AtomicLong(value);
        }

        @Override
        public long getValue() {
            reads.incrementAndGet();
            return value.get();
        }

        @Override
        public boolean isConnected() {
            return true;
        }
    }
}
