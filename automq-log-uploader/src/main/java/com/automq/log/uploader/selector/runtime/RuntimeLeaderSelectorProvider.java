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

package com.automq.log.uploader.selector.runtime;

import com.automq.log.uploader.selector.LogLeaderNodeSelector;
import com.automq.log.uploader.selector.LogLeaderNodeSelectorProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class RuntimeLeaderSelectorProvider implements LogLeaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeLeaderSelectorProvider.class);

    @Override
    public LogLeaderNodeSelector createSelector() {
        final AtomicBoolean missingLogged = new AtomicBoolean(false);
        final AtomicBoolean leaderLogged = new AtomicBoolean(false);

        return () -> {
            BooleanSupplier supplier = RuntimeLeaderRegistry.supplier();
            if (supplier == null) {
                if (missingLogged.compareAndSet(false, true)) {
                    LOGGER.warn("No log uploader runtime leader supplier registered yet; treating node as follower until available.");
                }
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from log uploader leadership because supplier is unavailable.");
                }
                return false;
            }

            if (missingLogged.get()) {
                missingLogged.set(false);
                LOGGER.info("Log uploader runtime leader supplier for is now available.");
            }

            try {
                boolean leader = supplier.getAsBoolean();
                if (leader) {
                    if (!leaderLogged.getAndSet(true)) {
                        LOGGER.info("Node became log uploader leader");
                    }
                } else {
                    if (leaderLogged.getAndSet(false)) {
                        LOGGER.info("Node stepped down from log uploader leadership");
                    }
                }
                return leader;
            } catch (RuntimeException e) {
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from log uploader leadership due to supplier exception.");
                }
                LOGGER.warn("Log uploader leader supplier threw an exception; treating as follower", e);
                return false;
            }
        };
    }
}
