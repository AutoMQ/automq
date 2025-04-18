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

package kafka.automq.backpressure;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static kafka.automq.backpressure.BackPressureConfig.RECONFIGURABLE_CONFIGS;

public class DefaultBackPressureManager implements BackPressureManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackPressureManager.class);

    private final BackPressureConfig config;
    private final Regulator regulator;

    /**
     * The scheduler to schedule the checker periodically.
     * Package-private for testing.
     */
    ScheduledExecutorService checkerScheduler;
    /**
     * The map to store the source and the most recent load level from the checker.
     * Note: It should only be accessed in the {@link #checkerScheduler} thread.
     */
    private final Map<String, LoadLevel> loadLevels = new HashMap<>();
    /**
     * The last time to trigger the regulator.
     * Note: It should only be accessed in the {@link #checkerScheduler} thread.
     */
    private long lastRegulateTime = System.currentTimeMillis();
    /**
     * The last load level to trigger the regulator.
     * Only used for logging and monitoring.
     */
    private LoadLevel lastRegulateLevel = LoadLevel.NORMAL;
    /**
     * The current state metrics of the system.
     * Only used for monitoring.
     *
     * @see S3StreamKafkaMetricsManager#setBackPressureStateSupplier
     */
    private final Map<String, Integer> stateMetrics = new HashMap<>(LoadLevel.values().length);

    public DefaultBackPressureManager(BackPressureConfig config, Regulator regulator) {
        this.config = config;
        this.regulator = regulator;
    }

    @Override
    public void start() {
        this.checkerScheduler = Threads.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("back-pressure-checker-%d", false), LOGGER, true, false);
        S3StreamKafkaMetricsManager.setBackPressureStateSupplier(this::stateMetrics);
    }

    @Override
    public void registerChecker(Checker checker) {
        checkerScheduler.scheduleWithFixedDelay(() -> {
            loadLevels.put(checker.source(), checker.check());
            maybeRegulate();
        }, 0, checker.intervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        ThreadUtils.shutdownExecutor(checkerScheduler, 1, TimeUnit.SECONDS);
    }

    private void maybeRegulate() {
        if (!config.enabled()) {
            return;
        }
        maybeRegulate(false);
    }

    /**
     * Regulate the system if the cooldown time has passed.
     *
     * @param isInternal True if it is an internal call, which means it should not schedule the next regulate action.
     */
    private void maybeRegulate(boolean isInternal) {
        LoadLevel loadLevel = currentLoadLevel();
        long now = System.currentTimeMillis();
        long timeElapsed = now - lastRegulateTime;

        if (timeElapsed < config.cooldownMs()) {
            // Skip regulating if the cooldown time has not passed.
            if (!isInternal) {
                // Schedule the next regulate action if it is not an internal call.
                checkerScheduler.schedule(() -> maybeRegulate(true), config.cooldownMs() - timeElapsed, TimeUnit.MILLISECONDS);
            }
            return;
        }
        regulate(loadLevel, now);
    }

    /**
     * Get the current load level of the system, which is, the maximum load level from all checkers.
     */
    private LoadLevel currentLoadLevel() {
        return loadLevels.values().stream()
            .max(LoadLevel::compareTo)
            .orElse(LoadLevel.NORMAL);
    }

    private void regulate(LoadLevel loadLevel, long now) {
        if (LoadLevel.NORMAL.equals(loadLevel)) {
            if (!LoadLevel.NORMAL.equals(lastRegulateLevel)) {
                LOGGER.info("The system is back to a normal state, checkers: {}", loadLevels);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("The system is in a normal state, checkers: {}", loadLevels);
            }
        } else {
            LOGGER.info("The system is in a {} state, checkers: {}", loadLevel, loadLevels);
        }

        loadLevel.regulate(regulator);
        lastRegulateTime = now;
        lastRegulateLevel = loadLevel;
    }

    private Map<String, Integer> stateMetrics() {
        LoadLevel current = currentLoadLevel();
        for (LoadLevel level : LoadLevel.values()) {
            int value = level.equals(current) ? current.ordinal() : -1;
            stateMetrics.put(level.name(), value);
        }
        return stateMetrics;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        BackPressureConfig.validate(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        config.update(configs);
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
