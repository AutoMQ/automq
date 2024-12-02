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

package kafka.automq.backpressure;

import org.apache.kafka.common.Reconfigurable;

/**
 * It checks the {@link LoadLevel} of the system and takes actions based on the load level
 * to prevent the system from being overwhelmed.
 */
public interface BackPressureManager extends Reconfigurable {

    /**
     * Start the back pressure manager.
     */
    void start();

    /**
     * Register a checker to check the load level of the system.
     * Note: It should be called between {@link #start()} and {@link #shutdown()}.
     */
    void registerChecker(Checker checker);

    /**
     * Shutdown the back pressure manager, and release all resources.
     */
    void shutdown();
}
