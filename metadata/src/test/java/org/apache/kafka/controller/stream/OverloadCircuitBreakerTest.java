/*
 * Copyright 2025, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
@Tag("S3Unit")
public class OverloadCircuitBreakerTest {

    @Test
    public void test() {
        Time time = new MockTime();
        OverloadCircuitBreaker breaker = new OverloadCircuitBreaker(time);
        assertFalse(breaker.isOverload());
        breaker.overload();
        assertTrue(breaker.isOverload());
        breaker.success();
        assertTrue(breaker.isOverload());
        time.sleep(OverloadCircuitBreaker.HALF_OPEN_WINDOW_MS);
        breaker.success();
        assertFalse(breaker.isOverload());
    }

}
