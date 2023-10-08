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

package com.automq.stream.s3.compact;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class AsyncTokenBucketThrottleTest {
    private AsyncTokenBucketThrottle asyncTokenBucketThrottle;

    @AfterEach
    void tearDown() {
        if (asyncTokenBucketThrottle != null) {
            asyncTokenBucketThrottle.stop();
        }
    }

    @Test
    void testThrottleQuickly() throws ExecutionException, InterruptedException {
        asyncTokenBucketThrottle = new AsyncTokenBucketThrottle(100, 1, "testThrottleQuickly");
        final long startTimeStamp = System.currentTimeMillis();

        asyncTokenBucketThrottle.throttle(100)
            .thenRun(() -> {
                // should be satisfied immediately
                Assertions.assertTrue(System.currentTimeMillis() - startTimeStamp < 5);
            }).get();
    }

    @Test
    void testThrottleSeq() throws ExecutionException, InterruptedException {
        asyncTokenBucketThrottle = new AsyncTokenBucketThrottle(100, 1, "testThrottleSeq");
        final long startTimeStamp = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(asyncTokenBucketThrottle.throttle(100));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenRun(() -> {
                // should be satisfied within 3 seconds
                Assertions.assertTrue(System.currentTimeMillis() - startTimeStamp < 3000);
            }).get();
    }

    @Test
    void testThrottleBigSize() throws ExecutionException, InterruptedException {
        asyncTokenBucketThrottle = new AsyncTokenBucketThrottle(100, 1, "testThrottleBigSize");
        final long startTimeStamp = System.currentTimeMillis();

        asyncTokenBucketThrottle.throttle(200)
            .thenRun(() -> {
                // should be satisfied within 2 seconds
                Assertions.assertTrue(System.currentTimeMillis() - startTimeStamp < 2000);
            }).get();
    }
}