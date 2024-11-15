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

/**
 * Represents the load level of the system.
 * {@link BackPressureManager} will take actions based on the load level.
 * Note: It MUST be ordered by the severity.
 */
public enum LoadLevel {
    /**
     * The system is in a normal state.
     */
    NORMAL {
        @Override
        public void regulate(Regulator regulator) {
            regulator.increase();
        }
    },
    /**
     * The system is in a high load state, and some actions should be taken to reduce the load.
     */
    HIGH {
        @Override
        public void regulate(Regulator regulator) {
            regulator.decrease();
        }
    },
    /**
     * The system is in a critical state, and the most severe actions should be taken.
     */
    CRITICAL {
        @Override
        public void regulate(Regulator regulator) {
            regulator.minimize();
        }
    };

    /**
     * Take actions based on the load level.
     */
    public abstract void regulate(Regulator regulator);
}
