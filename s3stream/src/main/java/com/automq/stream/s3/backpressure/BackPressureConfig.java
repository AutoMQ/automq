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

public class BackPressureConfig {

    public static final String BACK_PRESSURE_ENABLED_CONFIG = "s3.backpressure.enabled";
    public static final String BACK_PRESSURE_ENABLED_DOC = "Whether back pressure is enabled";
    public static final boolean BACK_PRESSURE_ENABLED_DEFAULT = true;

    public static final String BACK_PRESSURE_COOLDOWN_MS_CONFIG = "s3.backpressure.cooldown.ms";
    public static final String BACK_PRESSURE_COOLDOWN_MS_DOC = "The cooldown time in milliseconds to wait between two regulator actions";
    public static final long BACK_PRESSURE_COOLDOWN_MS_DEFAULT = TimeUnit.SECONDS.toMillis(15);
}
