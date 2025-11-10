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

import com.automq.opentelemetry.exporter.s3.LeaderNodeSelector;
import com.automq.opentelemetry.exporter.s3.LeaderNodeSelectorProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

abstract class AbstractRuntimeLeaderSelectorProvider implements LeaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRuntimeLeaderSelectorProvider.class);

    @Override
    public LeaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) {
        String key = registryKey();
        final AtomicBoolean missingLogged = new AtomicBoolean(false);
        final AtomicBoolean leaderLogged = new AtomicBoolean(false);

        return () -> {
            BooleanSupplier current = RuntimeLeaderRegistry.supplier(key);
            if (current == null) {
                if (missingLogged.compareAndSet(false, true)) {
                    LOGGER.warn("Telemetry leader supplier for key {} not yet available; treating node as follower until registration happens.", key);
                }
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from telemetry leadership for key {} because supplier is unavailable.", key);
                }
                return false;
            }

            if (missingLogged.get()) {
                missingLogged.set(false);
                LOGGER.info("Telemetry leader supplier for key {} is now available.", key);
            }

            try {
                boolean leader = current.getAsBoolean();
                if (leader) {
                    if (!leaderLogged.getAndSet(true)) {
                        LOGGER.info("Node became telemetry leader for key {}", key);
                    }
                } else {
                    if (leaderLogged.getAndSet(false)) {
                        LOGGER.info("Node stepped down from telemetry leadership for key {}", key);
                    }
                }
                return leader;
            } catch (RuntimeException e) {
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from telemetry leadership for key {} due to supplier exception.", key);
                }
                LOGGER.warn("Telemetry leader supplier for key {} threw exception. Treating as follower.", key, e);
                return false;
            }
        };
    }

    protected abstract String registryKey();
}
