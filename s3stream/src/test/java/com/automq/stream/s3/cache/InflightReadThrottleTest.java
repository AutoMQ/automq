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
