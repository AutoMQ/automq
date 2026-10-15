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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

final class JmxMetricNaming {
    private JmxMetricNaming() {
    }

    static String metricName(String domain, String path) {
        String readable = (domain + "_" + path).replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase(Locale.ROOT);
        if (readable.length() > 180) {
            readable = readable.substring(0, 180);
        }
        return "connect_jmx_" + readable + "_" + digest(domain.length() + ":" + domain + path) + "_value";
    }

    private static String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                result.append(String.format(Locale.ROOT, "%02x", hash[i] & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("The JVM must support SHA-256", e);
        }
    }

    static String pathPart(String value) {
        return value.replace("~", "~0").replace("/", "~1");
    }

    // Escape underscores as well, so a native key cannot impersonate an encoded key.
    static String labelPart(String value) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
                || character >= '0' && character <= '9') {
                result.append(character);
            } else {
                result.append('_').append(Integer.toHexString(character)).append('_');
            }
        }
        return result.toString();
    }
}
