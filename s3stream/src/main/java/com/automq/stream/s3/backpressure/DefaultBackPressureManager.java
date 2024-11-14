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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DefaultBackPressureManager implements BackPressureManager {

    public static final long DEFAULT_COOLDOWN_MS = TimeUnit.SECONDS.toMillis(20);

    private final Regulator regulator;
    private final long cooldownMs;

    public DefaultBackPressureManager(Regulator regulator) {
        this(regulator, DEFAULT_COOLDOWN_MS);
    }

    public DefaultBackPressureManager(Regulator regulator, long cooldownMs) {
        this.regulator = regulator;
        this.cooldownMs = cooldownMs;
    }

    @Override
    public void start() {
        // TODO
    }

    @Override
    public void registerChecker(String source, Supplier<LoadLevel> checker, long intervalMs) {
        // TODO
    }

    @Override
    public void shutdown() {
        // TODO
    }
}
