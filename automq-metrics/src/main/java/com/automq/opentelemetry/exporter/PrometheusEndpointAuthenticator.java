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

import java.util.Objects;

/**
 * Applies the public-endpoint policy independently of credential verification.
 */
public final class PrometheusEndpointAuthenticator extends Authenticator {

    private static final HttpPrincipal PUBLIC_PRINCIPAL =
        new HttpPrincipal("anonymous", "public");

    private final Authenticator delegate;

    public PrometheusEndpointAuthenticator(Authenticator delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Result authenticate(HttpExchange exchange) {
        String rawPath = exchange.getRequestURI().getRawPath();

        if (isPublicPath(rawPath)) {
            return new Success(PUBLIC_PRINCIPAL);
        }

        return delegate.authenticate(exchange);
    }

    private static boolean isPublicPath(String rawPath) {
        return "/".equals(rawPath) || "/-/healthy".equals(rawPath);
    }
}
