/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class OverloadCircuitBreaker {
    private static final Logger LOGGER = LoggerFactory.getLogger(OverloadCircuitBreaker.class);
    static final long HALF_OPEN_WINDOW_MS = TimeUnit.SECONDS.toMillis(30);
    private State state = State.OPEN;
    private long timestamp;

    private final Time time;

    public OverloadCircuitBreaker(Time time) {
        this.time = time;
    }

    public void overload() {
        this.state = State.CLOSED;
        this.timestamp = time.milliseconds();
        LOGGER.error("[WARN] The controller is overload, enter overload protecting state.");
    }

    public void success() {
        if (this.state == State.OPEN) {
            return;
        }
        long now = time.milliseconds();
        if (this.state == State.CLOSED) {
            this.state = State.HALF_OPEN;
            timestamp = now;
            LOGGER.info("The controller is recovering from overload, enter half-open state.");
        } else if (this.state == State.HALF_OPEN) {
            if (now - timestamp >= HALF_OPEN_WINDOW_MS) {
                this.state = State.OPEN;
                LOGGER.info("The controller has recovered from overload, enter open state.");
            }
        }
    }

    public boolean isOverload() {
        return state != State.OPEN;
    }

    enum State {
        OPEN,
        CLOSED,
        HALF_OPEN
    }

}
