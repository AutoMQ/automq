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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Basic authentication for the built-in Prometheus endpoint.
 *
 * <p>Only the metrics resource is protected. Health and default HTTP resources remain available
 * without credentials so existing liveness/readiness probes are not disrupted.</p>
 */
public class PrometheusBasicAuthenticator extends Authenticator {
    static final String METRICS_PATH = "/metrics";
    static final String REALM = "AutoMQ Prometheus metrics";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String WWW_AUTHENTICATE_HEADER = "WWW-Authenticate";
    private static final String BASIC_SCHEME = "Basic";
    private static final String CHALLENGE = BASIC_SCHEME + " realm=\"" + REALM + "\"";
    private static final HttpPrincipal PUBLIC_PRINCIPAL = new HttpPrincipal("anonymous", "public");

    private final String username;
    private final byte[] expectedCredentials;

    public PrometheusBasicAuthenticator(String username, String password) {
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Prometheus Basic Auth username must not be blank");
        }
        if (username.indexOf(':') >= 0) {
            throw new IllegalArgumentException("Prometheus Basic Auth username must not contain ':'");
        }
        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Prometheus Basic Auth password must not be empty");
        }
        this.username = username;
        this.expectedCredentials = (username + ":" + password).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Result authenticate(HttpExchange exchange) {
        if (!isMetricsPath(exchange.getRequestURI().getPath())) {
            return new Success(PUBLIC_PRINCIPAL);
        }

        String authorization = exchange.getRequestHeaders().getFirst(AUTHORIZATION_HEADER);
        if (StringUtils.isBlank(authorization)) {
            addChallenge(exchange);
            return new Retry(401);
        }

        int separator = authorization.indexOf(' ');
        if (separator <= 0 || !BASIC_SCHEME.equalsIgnoreCase(authorization.substring(0, separator))) {
            addChallenge(exchange);
            return new Failure(401);
        }

        byte[] actualCredentials;
        try {
            actualCredentials = Base64.getDecoder().decode(authorization.substring(separator + 1).trim());
        } catch (IllegalArgumentException e) {
            addChallenge(exchange);
            return new Failure(401);
        }

        if (!MessageDigest.isEqual(expectedCredentials, actualCredentials)) {
            addChallenge(exchange);
            return new Failure(401);
        }
        return new Success(new HttpPrincipal(username, REALM));
    }

    private static boolean isMetricsPath(String path) {
        return METRICS_PATH.equals(path) || path.startsWith(METRICS_PATH + "/");
    }

    private static void addChallenge(HttpExchange exchange) {
        exchange.getResponseHeaders().set(WWW_AUTHENTICATE_HEADER, CHALLENGE);
    }
}
