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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class RuntimeLeaderSelectorProvider implements LeaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeLeaderSelectorProvider.class);

    @Override
    public LeaderNodeSelector createSelector() {
        final AtomicBoolean missingLogged = new AtomicBoolean(false);
        final AtomicBoolean leaderLogged = new AtomicBoolean(false);

        return () -> {
            BooleanSupplier current = org.apache.kafka.connect.automq.runtime.RuntimeLeaderRegistry.supplier();
            if (current == null) {
                if (missingLogged.compareAndSet(false, true)) {
                    LOGGER.warn("leader supplier for key not yet available; treating node as follower until registration happens.");
                }
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from leadership because supplier is unavailable.");
                }
                return false;
            }

            if (missingLogged.get()) {
                missingLogged.set(false);
                LOGGER.info("leader supplier is now available.");
            }

            try {
                boolean leader = current.getAsBoolean();
                if (leader) {
                    if (!leaderLogged.getAndSet(true)) {
                        LOGGER.info("Node became leader");
                    }
                } else {
                    if (leaderLogged.getAndSet(false)) {
                        LOGGER.info("Node stepped down from leadership");
                    }
                }
                return leader;
            } catch (RuntimeException e) {
                if (leaderLogged.getAndSet(false)) {
                    LOGGER.info("Node stepped down from leadership due to supplier exception.");
                }
                LOGGER.warn("leader supplier threw exception. Treating as follower.", e);
                return false;
            }
        };
    }
}
