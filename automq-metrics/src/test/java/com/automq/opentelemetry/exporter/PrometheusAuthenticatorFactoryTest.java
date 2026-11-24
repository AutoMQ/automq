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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PrometheusAuthenticatorFactoryTest {

    @Test
    public void testNoneDisablesAuthentication() {
        assertNull(PrometheusAuthenticatorFactory.create(
            "none", null, null));
    }

    @Test
    public void testBasicCreatesEndpointAuthenticator() {
        Authenticator authenticator =
            PrometheusAuthenticatorFactory.create(
                "basic", "prometheus", "secret");

        assertInstanceOf(
            PrometheusEndpointAuthenticator.class,
            authenticator);
    }

    @Test
    public void testRejectsUnsupportedAuthenticationTypes() {
        for (String type : new String[]{
            "bearer", "unknown", "BASIC", ""
        }) {
            assertThrows(IllegalArgumentException.class,
                () -> PrometheusAuthenticatorFactory.create(
                    type, "prometheus", "secret"));
        }

        assertThrows(IllegalArgumentException.class,
            () -> PrometheusAuthenticatorFactory.create(
                null, "prometheus", "secret"));
    }

    @Test
    public void testRejectsMissingBasicPassword() {
        assertThrows(IllegalArgumentException.class,
            () -> PrometheusAuthenticatorFactory.create(
                "basic", "prometheus", null));
    }

    @Test
    public void testRejectsInvalidBasicUsername() {
        assertThrows(IllegalArgumentException.class,
            () -> PrometheusAuthenticatorFactory.create(
                "basic", "user:name", "secret"));

        assertThrows(IllegalArgumentException.class,
            () -> PrometheusAuthenticatorFactory.create(
                "basic", "", "secret"));
    }
}
