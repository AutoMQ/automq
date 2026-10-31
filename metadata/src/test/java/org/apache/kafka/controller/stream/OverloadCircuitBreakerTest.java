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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
@Tag("S3Unit")
public class OverloadCircuitBreakerTest {

    /**
     * Given overload protection, notify once when the breaker reopens after the half-open window.
     */
    @Test
    public void testRecoveryNotifiesOnlyWhenBreakerOpens() {
        Time time = new MockTime();
        OverloadCircuitBreaker breaker = new OverloadCircuitBreaker(time);
        AtomicInteger openCount = new AtomicInteger();
        breaker.setOnOpen(openCount::incrementAndGet);
        assertFalse(breaker.isOverload());
        breaker.overload();
        assertTrue(breaker.isOverload());
        breaker.success();
        assertTrue(breaker.isOverload());
        assertEquals(0, openCount.get());
        time.sleep(OverloadCircuitBreaker.HALF_OPEN_WINDOW_MS);
        breaker.success();
        assertFalse(breaker.isOverload());
        assertEquals(1, openCount.get());
        breaker.success();
        assertEquals(1, openCount.get());
        breaker.overload();
        breaker.success();
        time.sleep(OverloadCircuitBreaker.HALF_OPEN_WINDOW_MS);
        breaker.success();
        assertEquals(2, openCount.get());
    }

}
