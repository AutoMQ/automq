/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache;

import com.automq.stream.utils.Threads;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InflightReadThrottleTest {
    @Test
    public void testThrottle() {
        InflightReadThrottle throttle = new InflightReadThrottle(1024);
        UUID uuid = UUID.randomUUID();
        CompletableFuture<Void> cf = throttle.acquire(uuid, 512);
        Assertions.assertEquals(512, throttle.getRemainingInflightReadBytes());
        Assertions.assertTrue(cf.isDone());
        UUID uuid2 = UUID.randomUUID();
        CompletableFuture<Void> cf2 = throttle.acquire(uuid2, 600);
        Assertions.assertEquals(512, throttle.getRemainingInflightReadBytes());
        Assertions.assertEquals(1, throttle.getInflightQueueSize());
        Assertions.assertFalse(cf2.isDone());
        throttle.release(uuid);
        Threads.sleep(1000);
        Assertions.assertEquals(424, throttle.getRemainingInflightReadBytes());
        Assertions.assertTrue(cf2.isDone());
    }
}
