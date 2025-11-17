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

package org.apache.kafka.connect.automq.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BooleanSupplier;

/**
 * Stores runtime-provided suppliers that answer whether the current process
 * should act as the leader.
 */
public final class RuntimeLeaderRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeLeaderRegistry.class);
    private static BooleanSupplier supplier = () -> false;

    private RuntimeLeaderRegistry() {
    }

    public static void register(BooleanSupplier supplier) {
        RuntimeLeaderRegistry.supplier = supplier;
        LOGGER.info("Registered runtime leader supplier for log metrics.");
    }

    public static BooleanSupplier supplier() {
        return supplier;
    }
}
