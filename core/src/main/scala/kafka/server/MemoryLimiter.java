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

package kafka.server;

import java.util.concurrent.Semaphore;

public class MemoryLimiter {
    private final int maxPermits;
    private final Semaphore permits;

    public MemoryLimiter(int size) {
        maxPermits = size;
        permits = new Semaphore(size);
    }

    /**
     * Acquire permits, if not enough, block until enough.
     * Note: the acquire is fair, the acquired will be permitted in the acquire order.
     */
    public synchronized Handler acquire(int permit) throws InterruptedException {
        if (permit > maxPermits) {
            permit = maxPermits;
        }
        boolean acquireRst = permits.tryAcquire(permit);
        if (!acquireRst) {
            permits.acquire(permit);
        }
        return new Handler(permit);
    }

    public class Handler implements AutoCloseable {
        private final int permit;

        public Handler(int permit) {
            this.permit = permit;
        }

        @Override
        public void close() {
            permits.release(permit);
        }
    }
}
