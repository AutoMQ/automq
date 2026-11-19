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

package com.automq.opentelemetry;

import com.automq.opentelemetry.exporter.MetricsExportConfig;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoMQJmxUnitlessMetricTest {

    @Test
    public void testLocalJvmMBeanExportsPrometheusGauge()
        throws Exception {

        int port = availablePort();

        MetricsExportConfig config = mock(MetricsExportConfig.class);

        when(config.baseLabels())
            .thenReturn(Collections.emptyList());

        when(config.intervalMs()).thenReturn(250);

        AutoMQTelemetryManager manager =
            new AutoMQTelemetryManager(
                "prometheus://?host=127.0.0.1&port=" + port,
                "jmx-regression",
                "node-1",
                config
            );

        manager.setJmxConfigPaths("/jmx-unitless-metric.yaml");

        try {
            manager.init();

            String metricName = "automq_test_jmx_thread_count";
            String sample = null;
            String metricType = null;
            String lastBody = "";

            for (int attempt = 0; attempt < 60; attempt++) {
                lastBody = scrape(port);

                sample = lastBody.lines()
                    .filter(line ->
                        line.startsWith(metricName + "{")
                            || line.startsWith(metricName + " "))
                    .findFirst()
                    .orElse(null);

                metricType = lastBody.lines()
                    .filter(line ->
                        line.startsWith("# TYPE " + metricName + " "))
                    .findFirst()
                    .orElse(null);

                if (sample != null) {
                    break;
                }

                Thread.sleep(250);
            }

            assertNotNull(
                sample,
                "JMX ThreadCount metric not exported"
            );

            assertNotNull(
                metricType,
                "JMX metric type declaration missing"
            );

            assertTrue(
                metricType.endsWith(" gauge"),
                "Expected a Prometheus gauge"
            );

            double value = Double.parseDouble(
                sample.substring(sample.lastIndexOf(' ') + 1)
            );

            assertTrue(value > 0, "Invalid JVM thread count");

        } finally {
            manager.shutdown();
        }
    }

    private static String scrape(int port) throws Exception {
        HttpURLConnection connection =
            (HttpURLConnection) new URL(
                "http://127.0.0.1:" + port + "/metrics"
            ).openConnection();

        connection.setConnectTimeout(3000);
        connection.setReadTimeout(10000);

        try {
            assertEquals(200, connection.getResponseCode());

            try (InputStream stream = connection.getInputStream()) {
                return new String(
                    stream.readAllBytes(),
                    StandardCharsets.UTF_8
                );
            }
        } finally {
            connection.disconnect();
        }
    }

    private static int availablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
