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

package com.automq.stream.s3.wal;

import java.util.concurrent.CompletableFuture;

public interface ReservationService {
    ReservationService NOOP = new NoopReservationService();

    /**
     * Acquire the permission for the given node id and epoch.
     *
     * @param nodeId   the current node id or the failed node id in failover mode
     * @param epoch    the current node epoch or the failed node epoch in failover mode
     * @param failover whether this is in failover mode
     * @return a future that will be completed when the permission is acquired
     */
    CompletableFuture<Void> acquire(long nodeId, long epoch, boolean failover);

    /**
     * Verify the permission for the given node id and epoch.
     *
     * @param nodeId   the current node id or the failed node id in failover mode
     * @param epoch    the current node epoch or the failed node epoch in failover mode
     * @param failover whether this is in failover mode
     * @return a future that will be completed with the result of the verification
     */
    CompletableFuture<Boolean> verify(long nodeId, long epoch, boolean failover);

    class NoopReservationService implements ReservationService {
        @Override
        public CompletableFuture<Void> acquire(long nodeId, long epoch, boolean failover) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Boolean> verify(long nodeId, long epoch, boolean failover) {
            return CompletableFuture.completedFuture(true);
        }
    }
}
