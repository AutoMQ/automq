/*
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

package com.automq.stream.s3.metrics.stats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class MinIntervalRateTest {
    @Test
    public void testInitialCachedRateIsZero() {
        AtomicLong nowMs = new AtomicLong(1000);
        MinIntervalRate rate = new MinIntervalRate(30_000, nowMs::get);

        Assertions.assertEquals(0.0, rate.update(100));
    }

    @Test
    public void testReturnCachedRateBeforeMinInterval() {
        AtomicLong nowMs = new AtomicLong(0);
        MinIntervalRate rate = new MinIntervalRate(30_000, nowMs::get);

        nowMs.set(30_000);
        Assertions.assertEquals(10.0, rate.update(300));

        nowMs.set(31_000);
        Assertions.assertEquals(10.0, rate.update(1000));
    }

    @Test
    public void testRecomputeAfterMinInterval() {
        AtomicLong nowMs = new AtomicLong(0);
        MinIntervalRate rate = new MinIntervalRate(30_000, nowMs::get);

        nowMs.set(30_000);
        Assertions.assertEquals(10.0, rate.update(300));
        nowMs.set(60_000);
        Assertions.assertEquals(20.0, rate.update(900));
    }

    @Test
    public void testResetOnCounterRollback() {
        AtomicLong nowMs = new AtomicLong(0);
        MinIntervalRate rate = new MinIntervalRate(30_000, nowMs::get);

        nowMs.set(30_000);
        Assertions.assertEquals(10.0, rate.update(300));
        nowMs.set(60_000);
        Assertions.assertEquals(0.0, rate.update(100));
        nowMs.set(90_000);
        Assertions.assertEquals(10.0, rate.update(400));
    }

    @Test
    public void testInvalidMinInterval() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new MinIntervalRate(0));
    }
}
