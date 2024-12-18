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

/**
 * A checker to check the load level of the system periodically.
 */
public interface Checker {

    /**
     * The source of the checker, which should be unique to identify the checker.
     */
    String source();

    /**
     * Check the load level of the system.
     */
    LoadLevel check();

    /**
     * The interval in milliseconds to check the load level of the system.
     */
    long intervalMs();
}
