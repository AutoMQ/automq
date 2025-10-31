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

package com.automq.opentelemetry.exporter.s3.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;

/**
 * Stores runtime-provided suppliers that answer whether the current process
 * should act as the primary S3 metrics uploader.
 */
public final class RuntimeLeaderRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeLeaderRegistry.class);
    private static final ConcurrentMap<String, BooleanSupplier> SUPPLIERS = new ConcurrentHashMap<>();

    private RuntimeLeaderRegistry() {
    }

    public static void register(String key, BooleanSupplier supplier) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(supplier, "supplier");
        SUPPLIERS.put(key, supplier);
        LOGGER.info("Registered telemetry leader supplier for key {}", key);
    }

    public static void clear(String key) {
        if (key == null) {
            return;
        }
        if (SUPPLIERS.remove(key) != null) {
            LOGGER.info("Cleared telemetry leader supplier for key {}", key);
        }
    }

    public static BooleanSupplier supplier(String key) {
        return SUPPLIERS.get(key);
    }
}
