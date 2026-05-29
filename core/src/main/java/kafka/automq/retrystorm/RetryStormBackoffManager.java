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

import kafka.server.retrystorm.RetryStormBackoffLogger;
import kafka.server.retrystorm.RetryStormBackoffPolicy;
import kafka.server.retrystorm.RetryStormDelayedResponseScheduler;
import kafka.server.retrystorm.RetryStormResponseGate;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns retry storm backoff runtime components for a broker.
 *
 * <p>The manager connects Kafka dynamic config callbacks to the shared runtime config,
 * exposes the response gate to request handlers, and closes delayed response scheduling
 * during broker shutdown. It does not evaluate responses itself.</p>
 */
public class RetryStormBackoffManager implements Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryStormBackoffManager.class);
    private static final long LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long MIN_DELAYING_AGE_TO_LOG_MS = TimeUnit.SECONDS.toMillis(10);
    private static final int MAX_LOGGED_STATES_PER_INTERVAL = 10;

    private final RetryStormBackoffConfig config;
    private final RetryStormBackoffPolicy policy;
    private final RetryStormResponseGate responseGate;
    private final RetryStormDelayedResponseScheduler scheduler;
    private final RetryStormBackoffStateStore stateStore;
    private final RetryStormBackoffLogger logger;
    private final ScheduledExecutorService logScheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Creates a manager with only config ownership, primarily for dynamic config tests.
     */
    public RetryStormBackoffManager(RetryStormBackoffConfig config) {
        this(config, null, null, null, null, RetryStormBackoffLogger.NOOP);
    }

    /**
     * Creates a broker runtime manager over config, policy, response gate, and scheduler.
     */
    public RetryStormBackoffManager(RetryStormBackoffConfig config,
                                    RetryStormBackoffPolicy policy,
                                    RetryStormResponseGate responseGate,
                                    RetryStormDelayedResponseScheduler scheduler,
                                    RetryStormBackoffStateStore stateStore,
                                    RetryStormBackoffLogger logger) {
        this.config = config;
        this.policy = policy;
        this.responseGate = responseGate;
        this.scheduler = scheduler;
        this.stateStore = stateStore;
        this.logger = logger;
        this.logScheduler = stateStore == null
            ? null
            : Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("retry-storm-backoff-logger-%d", true),
                LOGGER,
                true,
                false
            );
    }

    /**
     * Returns the broker config keys this manager can validate and apply dynamically.
     */
    @Override
    public Set<String> reconfigurableConfigs() {
        return RetryStormBackoffConfig.RECONFIGURABLE_CONFIGS;
    }

    /**
     * Validates a dynamic broker config update before Kafka publishes it to the runtime config.
     */
    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        RetryStormBackoffConfig.validate(configs);
    }

    /**
     * Applies a validated dynamic broker config update to later retry storm policy decisions.
     */
    @Override
    public void reconfigure(Map<String, ?> configs) {
        config.update(configs);
    }

    /**
     * Kafka Reconfigurable lifecycle hook; retry storm config is initialized by BrokerServer.
     */
    @Override
    public void configure(Map<String, ?> configs) {
    }

    /**
     * Starts periodic delayed-state logging for the broker runtime manager.
     */
    public void startup() {
        if (logScheduler != null && started.compareAndSet(false, true)) {
            logScheduler.scheduleAtFixedRate(this::logDelayedStatesOnce,
                LOG_INTERVAL_MS, LOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Runs one delayed-state logging scan without updating state cache access time.
     */
    public void logDelayedStatesOnce() {
        if (stateStore == null) {
            return;
        }
        logger.logDelayedStates(stateStore.delayedSnapshots(
            System.currentTimeMillis(),
            MIN_DELAYING_AGE_TO_LOG_MS,
            MAX_LOGGED_STATES_PER_INTERVAL
        ));
    }

    /**
     * Closes delayed response scheduling and best-effort sends pending delayed responses.
     */
    public void shutdown() {
        if (logScheduler != null) {
            logScheduler.shutdownNow();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * Returns the shared runtime config snapshot used by policy evaluation.
     */
    public RetryStormBackoffConfig config() {
        return config;
    }

    /**
     * Returns the policy owned by this broker, or null in config-only tests.
     */
    public RetryStormBackoffPolicy policy() {
        return policy;
    }

    /**
     * Returns the response gate injected into request send paths, or null in config-only tests.
     */
    public RetryStormResponseGate responseGate() {
        return responseGate;
    }
}
