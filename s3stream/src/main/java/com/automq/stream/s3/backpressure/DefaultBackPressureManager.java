/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.backpressure;

import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBackPressureManager implements BackPressureManager {

    public static final long DEFAULT_COOLDOWN_MS = TimeUnit.SECONDS.toMillis(15);

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBackPressureManager.class);

    private final Regulator regulator;
    /**
     * The cooldown time in milliseconds to wait between two regulator actions.
     */
    private final long cooldownMs;

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

    public DefaultBackPressureManager(Regulator regulator) {
        this(regulator, DEFAULT_COOLDOWN_MS);
    }

    public DefaultBackPressureManager(Regulator regulator, long cooldownMs) {
        this.regulator = regulator;
        this.cooldownMs = cooldownMs;
    }

    @Override
    public void start() {
        this.checkerScheduler = Threads.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("back-pressure-checker-%d", false), LOGGER);
    }

    @Override
    public void registerChecker(Checker checker) {
        checkerScheduler.scheduleAtFixedRate(() -> {
            loadLevels.put(checker.source(), checker.check());
            maybeRegulate();
        }, 0, checker.intervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        ThreadUtils.shutdownExecutor(checkerScheduler, 1, TimeUnit.SECONDS);
    }

    private void maybeRegulate() {
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

        if (timeElapsed < cooldownMs) {
            // Skip regulating if the cooldown time has not passed.
            if (!isInternal) {
                // Schedule the next regulate action if it is not an internal call.
                checkerScheduler.schedule(() -> maybeRegulate(true), cooldownMs - timeElapsed, TimeUnit.MILLISECONDS);
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("The system is in a normal state, checkers: {}", loadLevels);
            }
        } else {
            LOGGER.info("The system is in a {} state, checkers: {}", loadLevel, loadLevels);
        }

        loadLevel.regulate(regulator);
        lastRegulateTime = now;
    }
}
