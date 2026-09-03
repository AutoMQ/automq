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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class PrometheusMetricsExporterTest {

    @Test
    public void testExposesMetricsWithoutAuthenticationByDefault() throws Exception {
        int port = availablePort();

        MetricReader metricReader = new PrometheusMetricsExporter(
            "127.0.0.1",
            port,
            Collections.emptyList()
        ).asMetricReader();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();

        try {
            LongCounter counter = meterProvider
                .get("prometheus-exporter-test")
                .counterBuilder("automq_test_counter")
                .build();

            counter.add(1);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/metrics"))
                .GET()
                .build();

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                request,
                HttpResponse.BodyHandlers.ofString()
            );

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertTrue(
                response.body().contains("automq_test_counter_total"),
                response.body()
            );
        } finally {
            meterProvider.close();
        }
    }

    @Test
    public void testReleasesPortAfterClose() throws Exception {
        int port = availablePort();

        MetricReader metricReader = new PrometheusMetricsExporter(
            "127.0.0.1",
            port,
            Collections.emptyList()
        ).asMetricReader();

        metricReader.close();

        try (ServerSocket socket = new ServerSocket(port)) {
            Assertions.assertTrue(socket.isBound());
        }
    }

    @Test
    public void testHealthEndpointAvailableWithoutAuthenticationByDefault() throws Exception {
        int port = availablePort();

        MetricReader metricReader = new PrometheusMetricsExporter(
            "127.0.0.1",
            port,
            Collections.emptyList()
        ).asMetricReader();

        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/-/healthy"))
                .GET()
                .build();

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                request,
                HttpResponse.BodyHandlers.ofString()
            );

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertEquals("Exporter is healthy.\n", response.body());
        } finally {
            metricReader.close();
        }
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
