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

package kafka.server;

import kafka.automq.AutoMQConfig;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class TelemetrySupportTest {

    @Test
    public void testAuthenticationDisabledByDefault() {
        assertNull(TelemetrySupport.buildPrometheusAuthenticator(
            config(null, null, null)));
    }

    @Test
    public void testExplicitNoneDisablesAuthentication() {
        assertNull(TelemetrySupport.buildPrometheusAuthenticator(
            config("none", null, null)));
    }

    @Test
    public void testBasicAuthenticationChallengesMissingCredentials() {
        Authenticator authenticator =
            TelemetrySupport.buildPrometheusAuthenticator(
                config("basic", "prometheus", "secret"));

        HttpExchange exchange = exchange("/metrics", null);

        assertInstanceOf(
            Authenticator.Retry.class,
            authenticator.authenticate(exchange));

        assertEquals(
            "Basic realm=\"AutoMQ Prometheus metrics\"",
            exchange.getResponseHeaders().getFirst("WWW-Authenticate"));
    }

    @Test
    public void testBasicAuthenticationAcceptsConfiguredCredentials() {
        Authenticator authenticator =
            TelemetrySupport.buildPrometheusAuthenticator(
                config("basic", "prometheus", "secret"));

        String authorization = basicAuthorization("prometheus", "secret");

        assertInstanceOf(
            Authenticator.Success.class,
            authenticator.authenticate(exchange("/metrics", authorization)));
    }

    @Test
    public void testBasicAuthenticationRejectsWrongCredentials() {
        Authenticator authenticator =
            TelemetrySupport.buildPrometheusAuthenticator(
                config("basic", "prometheus", "secret"));

        assertInstanceOf(
            Authenticator.Failure.class,
            authenticator.authenticate(
                exchange("/metrics", basicAuthorization("prometheus", "wrong"))));
    }

    @Test
    public void testEncodedHealthAliasRequiresAuthentication() {
        Authenticator authenticator =
            TelemetrySupport.buildPrometheusAuthenticator(
                config("basic", "prometheus", "secret"));

        assertInstanceOf(
            Authenticator.Retry.class,
            authenticator.authenticate(exchange("/%2D/healthy", null)));
    }

    @Test
    public void testMissingBasicPasswordFailsClosed() {
        KafkaConfig kafkaConfig = config("basic", "prometheus", null);

        assertThrows(
            IllegalArgumentException.class,
            () -> TelemetrySupport.buildPrometheusAuthenticator(kafkaConfig));
    }

    @Test
    public void testInvalidBasicUsernameFailsClosed() {
        KafkaConfig kafkaConfig = config("basic", "user:name", "secret");

        assertThrows(
            IllegalArgumentException.class,
            () -> TelemetrySupport.buildPrometheusAuthenticator(kafkaConfig));
    }

    private static KafkaConfig config(
        String type,
        String username,
        String password
    ) {
        Properties props = new Properties();
        props.setProperty("broker.id", "1");
        props.setProperty("zookeeper.connect", "localhost:2181");

        if (type != null) {
            props.setProperty(
                AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_TYPE_CONFIG,
                type);
        }
        if (username != null) {
            props.setProperty(
                AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_USERNAME_CONFIG,
                username);
        }
        if (password != null) {
            props.setProperty(
                AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_PASSWORD_CONFIG,
                password);
        }

        return KafkaConfig.fromProps(props, false);
    }

    private static HttpExchange exchange(String path, String authorization) {
        HttpExchange exchange = mock(HttpExchange.class);

        Headers requestHeaders = new Headers();
        if (authorization != null) {
            requestHeaders.set("Authorization", authorization);
        }

        when(exchange.getRequestURI()).thenReturn(URI.create(path));
        when(exchange.getRequestHeaders()).thenReturn(requestHeaders);
        when(exchange.getResponseHeaders()).thenReturn(new Headers());

        return exchange;
    }

    private static String basicAuthorization(String username, String password) {
        byte[] credentials = (username + ":" + password)
            .getBytes(StandardCharsets.UTF_8);

        return "Basic " + Base64.getEncoder().encodeToString(credentials);
    }
}
