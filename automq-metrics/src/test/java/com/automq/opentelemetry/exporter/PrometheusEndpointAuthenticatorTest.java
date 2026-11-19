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

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrometheusEndpointAuthenticatorTest {

    @Test
    public void testExactPublicEndpointsRemainAccessible() {
        for (String path : new String[] {
            "/", "/-/healthy", "/-/healthy?probe=1"
        }) {
            assertInstanceOf(Authenticator.Success.class,
                authenticateWithoutCredentials(path));
        }
    }

    @Test
    public void testProtectedEndpointsRequireCredentials() {
        for (String path : new String[] {
            "/metrics",
            "/metrics/child",
            "/metricsXYZ",
            "/metrics-extra",
            "/-/healthy/",
            "/-/healthy-extra",
            "/unknown",
            "/%2F"
        }) {
            assertInstanceOf(Authenticator.Retry.class,
                authenticateWithoutCredentials(path),
                "Expected authentication for: " + path);
        }
    }

    @Test
    public void testEncodedPublicEndpointAliasesRequireAuthentication() {
        for (String path : new String[] {
            "/%2D/healthy",
            "/-/health%79"
        }) {
            assertInstanceOf(Authenticator.Retry.class,
                authenticateWithoutCredentials(path),
                "Encoded path must not bypass authentication: " + path);
        }
    }

    @Test
    public void testRejectsNullAuthenticationDelegate() {
        assertThrows(NullPointerException.class,
            () -> new PrometheusEndpointAuthenticator(null));
    }

    private static Authenticator.Result authenticateWithoutCredentials(
        String path
    ) {
        HttpExchange exchange = mock(HttpExchange.class);

        when(exchange.getRequestURI()).thenReturn(URI.create(path));
        when(exchange.getRequestHeaders()).thenReturn(new Headers());
        when(exchange.getResponseHeaders()).thenReturn(new Headers());

        Authenticator authenticator =
            new PrometheusEndpointAuthenticator(
                new PrometheusBasicAuthenticator(
                    "prometheus", "secret"));

        return authenticator.authenticate(exchange);
    }
}
