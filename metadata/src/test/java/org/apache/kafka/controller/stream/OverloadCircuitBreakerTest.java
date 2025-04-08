/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
