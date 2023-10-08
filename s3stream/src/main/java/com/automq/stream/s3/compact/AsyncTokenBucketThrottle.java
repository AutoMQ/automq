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

import io.github.bucket4j.Bucket;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class AsyncTokenBucketThrottle {
    private final long tokenSize;
    private final ScheduledExecutorService executorService;
    private final Bucket bucket;

    public AsyncTokenBucketThrottle(long tokenSize, int poolSize, String threadSuffix) {
        this.tokenSize = tokenSize;
        this.executorService = Executors.newScheduledThreadPool(poolSize, new DefaultThreadFactory("token-bucket-throttle-" + threadSuffix));
        this.bucket = Bucket.builder()
            .addLimit(limit -> limit.capacity(tokenSize).refillGreedy(tokenSize, Duration.ofSeconds(1)))
            .build();
    }

    public void stop() {
        this.executorService.shutdown();
    }

    public long getTokenSize() {
        return tokenSize;
    }

    public CompletableFuture<Void> throttle(long size) {
        return bucket.asScheduler().consume(size, executorService);
    }
}
