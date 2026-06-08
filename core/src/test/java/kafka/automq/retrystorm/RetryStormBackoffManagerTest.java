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

package kafka.automq.retrystorm;

import kafka.automq.AutoMQConfig;
import kafka.server.retrystorm.RetryStormBackoffLogger;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers manager integration with Kafka dynamic broker config callbacks.
 */
@Tag("S3Unit")
public class RetryStormBackoffManagerTest {

    /**
     * Given a validated dynamic update, reconfigure publishes the new runtime values.
     */
    @Test
    public void testReconfigureUpdatesRuntimeConfig() {
        RetryStormBackoffConfig config = new RetryStormBackoffConfig(true, 1000L);
        RetryStormBackoffManager manager = new RetryStormBackoffManager(config);

        manager.reconfigure(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, false,
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, 250L
        ));

        assertFalse(config.enabled());
        assertEquals(250L, config.maxDelayMs());
    }

    /**
     * Given an invalid dynamic update, manager validation rejects it before runtime state changes.
     */
    @Test
    public void testValidateRejectsInvalidReconfiguration() {
        RetryStormBackoffManager manager = new RetryStormBackoffManager(new RetryStormBackoffConfig(true, 1000L));

        assertThrows(ConfigException.class, () -> manager.validateReconfiguration(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, -1L
        )));
    }

    /**
     * Given many old delaying states, one periodic logging scan emits no more than ten snapshots.
     */
    @Test
    public void testPeriodicLogScanLimitsSnapshots() {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(1000L, 60000L, 1000L, 100000);
        CapturingLogger logger = new CapturingLogger();
        RetryStormBackoffManager manager = new RetryStormBackoffManager(
            new RetryStormBackoffConfig(true, 1000L),
            null,
            null,
            null,
            store,
            logger
        );
        for (int i = 0; i < 16; i++) {
            RetryStormBackoffStateStore.BackoffKey key =
                new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-" + i, "connection-1");
            store.recordAndDecide(key, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 1000L);
            store.recordAndDecide(key, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1L, 1000L);
        }

        manager.logDelayedStatesOnce();
        manager.shutdown();

        assertEquals(1, logger.calls.get());
        assertEquals(10, logger.lastSize);
    }

    private static class CapturingLogger implements RetryStormBackoffLogger {
        private final AtomicInteger calls = new AtomicInteger(0);
        private int lastSize;

        @Override
        public void logDelayedStates(List<RetryStormBackoffStateStore.DelayedStateSnapshot> snapshots) {
            calls.incrementAndGet();
            lastSize = snapshots.size();
        }
    }
}
