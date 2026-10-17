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
package org.apache.kafka.raft;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Tracks the latest high watermark returned on each inbound Raft connection.
 *
 * <p>This is a temporary, protocol-compatible approximation of KIP-1166 for the Kafka 3.9-based
 * code. Previously, a delayed Fetch was woken when the leader log end offset advanced, but not
 * when only the leader high watermark advanced. A follower or observer which had already fetched
 * the records could therefore wait for the Fetch timeout before learning that those records were
 * committed. The tracker lets the leader return an empty Fetch immediately when its current high
 * watermark is newer than the last high watermark returned on the same connection, while a high
 * watermark update wakes already delayed Fetches.</p>
 *
 * <p>Unlike the complete KIP-1166 solution, this tracker infers remote progress from connection
 * history instead of receiving the remote replica's applied high watermark in the Fetch request.
 * It should be removed when the protocol-level KIP-1166 implementation is adopted.</p>
 *
 * <p>The tracker is owned by the Raft request-processing thread. Expired entries are removed
 * opportunistically after the map exceeds its retained-entry limit; no background cleanup is
 * scheduled.</p>
 */
final class FetchHighWatermarkTracker {
    private static final int RETAINED_ENTRY_LIMIT = 10_000;
    private static final long ENTRY_EXPIRATION_MS = 5 * 60 * 1000L;

    private final Map<String, ConnectionState> connectionStates = new HashMap<>();
    private long nextExpirationTimeMs = Long.MIN_VALUE;

    boolean hasNewHighWatermark(
        String connectionId,
        int leaderEpoch,
        long highWatermark,
        long currentTimeMs
    ) {
        maybeRemoveExpiredEntries(currentTimeMs);
        ConnectionState state = connectionStates.get(connectionId);
        if (state == null) {
            return true;
        }

        state.lastAccessTimeMs = currentTimeMs;
        return state.leaderEpoch != leaderEpoch || state.highWatermark < highWatermark;
    }

    void update(
        String connectionId,
        int leaderEpoch,
        long highWatermark,
        long currentTimeMs
    ) {
        ConnectionState state = connectionStates.get(connectionId);
        if (state == null || state.leaderEpoch != leaderEpoch) {
            connectionStates.put(
                connectionId,
                new ConnectionState(leaderEpoch, highWatermark, currentTimeMs)
            );
        } else {
            state.highWatermark = Math.max(state.highWatermark, highWatermark);
            state.lastAccessTimeMs = currentTimeMs;
        }
        maybeRemoveExpiredEntries(currentTimeMs);
    }

    private void maybeRemoveExpiredEntries(long currentTimeMs) {
        if (connectionStates.size() <= RETAINED_ENTRY_LIMIT || currentTimeMs < nextExpirationTimeMs) {
            return;
        }

        long nextExpirationTimeMs = Long.MAX_VALUE;
        Iterator<ConnectionState> iterator = connectionStates.values().iterator();
        while (iterator.hasNext()) {
            ConnectionState state = iterator.next();
            long expirationTimeMs = state.lastAccessTimeMs + ENTRY_EXPIRATION_MS;
            if (expirationTimeMs <= currentTimeMs) {
                iterator.remove();
            } else {
                nextExpirationTimeMs = Math.min(nextExpirationTimeMs, expirationTimeMs);
            }
        }
        this.nextExpirationTimeMs = connectionStates.isEmpty() ? Long.MIN_VALUE : nextExpirationTimeMs;
    }

    private static final class ConnectionState {
        private final int leaderEpoch;
        private long highWatermark;
        private long lastAccessTimeMs;

        private ConnectionState(int leaderEpoch, long highWatermark, long lastAccessTimeMs) {
            this.leaderEpoch = leaderEpoch;
            this.highWatermark = highWatermark;
            this.lastAccessTimeMs = lastAccessTimeMs;
        }
    }
}
