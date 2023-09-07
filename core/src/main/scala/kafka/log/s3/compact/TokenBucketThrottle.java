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

package kafka.log.s3.compact;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TokenBucketThrottle {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long tokenSize;
    private long availableTokens;

    private final ScheduledExecutorService executorService;

    public TokenBucketThrottle(long tokenSize) {
        this.tokenSize = tokenSize;
        this.executorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("token-bucket-throttle"));
        this.executorService.scheduleAtFixedRate(() -> {
            try {
                lock.lock();
                availableTokens = tokenSize;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    public void stop() {
        this.executorService.shutdown();
    }

    public long getTokenSize() {
        return tokenSize;
    }

    public void throttle(long size) {
        try {
            lock.lock();
            while (availableTokens < size) {
                condition.await();
            }
            availableTokens -= size;
        } catch (InterruptedException ignored) {
        } finally {
            lock.unlock();
        }
    }
}
