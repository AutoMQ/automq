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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class PrometheusMetricsExporterTest {

    private static final String USERNAME = "prometheus";
    private static final String PASSWORD = "secret";

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

            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                null
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
    public void testMetricsRequiresAuthenticationWhenBasicAuthEnabled() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                null
            );

            Assertions.assertEquals(401, response.statusCode());
            Assertions.assertEquals(
                "Basic realm=\"AutoMQ Prometheus metrics\"",
                response.headers().firstValue("WWW-Authenticate").orElse(null)
            );
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testMetricsDescendantRequiresAuthentication() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics/test",
                null
            );

            Assertions.assertEquals(401, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testRejectsWrongUsername() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                basicAuthorization("wrong-user", PASSWORD)
            );

            Assertions.assertEquals(401, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testRejectsWrongPassword() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                basicAuthorization(USERNAME, "wrong-password")
            );

            Assertions.assertEquals(401, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testRejectsMalformedBase64Credentials() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                "Basic !!!not-base64!!!"
            );

            Assertions.assertEquals(401, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testRejectsWrongAuthenticationScheme() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                "Bearer token"
            );

            Assertions.assertEquals(401, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testExposesMetricsWithCorrectCredentials() throws Exception {
        int port = availablePort();

        MetricReader metricReader = authenticatedMetricReader(port);

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();

        try {
            LongCounter counter = meterProvider
                .get("prometheus-auth-test")
                .counterBuilder("automq_authenticated_counter")
                .build();

            counter.add(1);

            HttpResponse<String> response = sendRequest(
                port,
                "/metrics",
                basicAuthorization(USERNAME, PASSWORD)
            );

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertTrue(
                response.body().contains("automq_authenticated_counter_total"),
                response.body()
            );
        } finally {
            meterProvider.close();
        }
    }

    @Test
    public void testRootEndpointRemainsPublicWithAuthenticationEnabled() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/",
                null
            );

            Assertions.assertEquals(200, response.statusCode());
        } finally {
            metricReader.close();
        }
    }

    @Test
    public void testHealthEndpointRemainsPublicWithAuthenticationEnabled() throws Exception {
        int port = availablePort();
        MetricReader metricReader = authenticatedMetricReader(port);

        try {
            HttpResponse<String> response = sendRequest(
                port,
                "/-/healthy",
                null
            );

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertEquals("Exporter is healthy.\n", response.body());
        } finally {
            metricReader.close();
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
            HttpResponse<String> response = sendRequest(
                port,
                "/-/healthy",
                null
            );

            Assertions.assertEquals(200, response.statusCode());
            Assertions.assertEquals("Exporter is healthy.\n", response.body());
        } finally {
            metricReader.close();
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

    private static MetricReader authenticatedMetricReader(int port) {
        return new PrometheusMetricsExporter(
            "127.0.0.1",
            port,
            Collections.emptyList(),
            new PrometheusBasicAuthenticator(USERNAME, PASSWORD)
        ).asMetricReader();
    }

    private static HttpResponse<String> sendRequest(
        int port,
        String path,
        String authorization
    ) throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + port + path))
            .GET();

        if (authorization != null) {
            builder.header("Authorization", authorization);
        }

        return HttpClient.newHttpClient().send(
            builder.build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    private static String basicAuthorization(String username, String password) {
        String credentials = username + ":" + password;

        return "Basic " + Base64.getEncoder().encodeToString(
            credentials.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
