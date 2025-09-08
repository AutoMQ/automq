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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.snapshot.RawSnapshotWriter;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public class FollowerState implements EpochState {
    private final int fetchTimeoutMs;
    private final int epoch;
    private final int leaderId;
    private final Endpoints leaderEndpoints;
    private final Set<Integer> voters;
    // Used for tracking the expiration of both the Fetch and FetchSnapshot requests
    private final Timer fetchTimer;
    private Optional<LogOffsetMetadata> highWatermark;
    /* Used to track the currently fetching snapshot. When fetching snapshot regular
     * Fetch request are paused
     */
    private Optional<RawSnapshotWriter> fetchingSnapshot = Optional.empty();
    // Used to throttle update voter request and allow for Fetch/FetchSnapshot requests
    private final Timer updateVoterPeriodTimer;

    private final Logger log;

    public FollowerState(
        Time time,
        int epoch,
        int leaderId,
        Endpoints leaderEndpoints,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        int fetchTimeoutMs,
        LogContext logContext
    ) {
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.epoch = epoch;
        this.leaderId = leaderId;
        this.leaderEndpoints = leaderEndpoints;
        this.voters = voters;
        this.fetchTimer = time.timer(fetchTimeoutMs);
        this.updateVoterPeriodTimer = time.timer(updateVoterPeriodMs());
        this.highWatermark = highWatermark;
        this.log = logContext.logger(FollowerState.class);
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, leaderId, voters);
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return leaderEndpoints;
    }

    @Override
    public String name() {
        return "Follower";
    }

    public long remainingFetchTimeMs(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        return fetchTimer.remainingMs();
    }

    public int leaderId() {
        return leaderId;
    }

    public Node leaderNode(ListenerName listener) {
        return leaderEndpoints
            .address(listener)
            .map(address -> new Node(leaderId, address.getHostString(), address.getPort()))
            .orElseThrow(() ->
                new IllegalArgumentException(
                    String.format(
                        "Unknown endpoint for leader %d and listener %s, known endpoints are %s",
                        leaderId,
                        listener,
                        leaderEndpoints
                    )
                )
            );
    }

    public boolean hasFetchTimeoutExpired(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        return fetchTimer.isExpired();
    }

    public void resetFetchTimeout(long currentTimeMs) {
        fetchTimer.update(currentTimeMs);
        fetchTimer.reset(fetchTimeoutMs);
    }

    public void overrideFetchTimeout(long currentTimeMs, long timeoutMs) {
        fetchTimer.update(currentTimeMs);
        fetchTimer.reset(timeoutMs);
    }

    private long updateVoterPeriodMs() {
        // Allow for a few rounds of fetch request before attempting to update
        // the voter state
        return fetchTimeoutMs * 3;
    }

    public boolean hasUpdateVoterPeriodExpired(long currentTimeMs) {
        updateVoterPeriodTimer.update(currentTimeMs);
        return updateVoterPeriodTimer.isExpired();
    }

    public long remainingUpdateVoterPeriodMs(long currentTimeMs) {
        updateVoterPeriodTimer.update(currentTimeMs);
        return updateVoterPeriodTimer.remainingMs();
    }

    public void resetUpdateVoterPeriod(long currentTimeMs) {
        updateVoterPeriodTimer.update(currentTimeMs);
        updateVoterPeriodTimer.reset(updateVoterPeriodMs());
    }

    public boolean updateHighWatermark(OptionalLong newHighWatermark) {
        if (!newHighWatermark.isPresent() && highWatermark.isPresent()) {
            throw new IllegalArgumentException(
                String.format("Attempt to overwrite current high watermark %s with unknown value", highWatermark)
            );
        }

        if (highWatermark.isPresent()) {
            long previousHighWatermark = highWatermark.get().offset();
            long updatedHighWatermark = newHighWatermark.getAsLong();

            if (updatedHighWatermark < 0) {
                throw new IllegalArgumentException(
                    String.format("Illegal negative (%d) high watermark update", updatedHighWatermark)
                );
            } else if (previousHighWatermark > updatedHighWatermark) {
                throw new IllegalArgumentException(
                    String.format(
                        "Non-monotonic update of high watermark from %d to %d",
                        previousHighWatermark,
                        updatedHighWatermark
                    )
                );
            } else if (previousHighWatermark == updatedHighWatermark) {
                return false;
            }
        }

        Optional<LogOffsetMetadata> oldHighWatermark = highWatermark;
        highWatermark = newHighWatermark.isPresent() ?
            Optional.of(new LogOffsetMetadata(newHighWatermark.getAsLong())) :
            Optional.empty();

        logHighWatermarkUpdate(oldHighWatermark, highWatermark);

        return true;
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    public Optional<RawSnapshotWriter> fetchingSnapshot() {
        return fetchingSnapshot;
    }

    public void setFetchingSnapshot(Optional<RawSnapshotWriter> newSnapshot) {
        fetchingSnapshot.ifPresent(RawSnapshotWriter::close);
        fetchingSnapshot = newSnapshot;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        log.debug(
            "Rejecting vote request from candidate ({}) since we already have a leader {} in epoch {}",
            candidateKey,
            leaderId,
            epoch
        );
        return false;
    }

    @Override
    public String toString() {
        return String.format(
            "FollowerState(fetchTimeoutMs=%d, epoch=%d, leader=%d, leaderEndpoints=%s, " +
            "voters=%s, highWatermark=%s, fetchingSnapshot=%s)",
            fetchTimeoutMs,
            epoch,
            leaderId,
            leaderEndpoints,
            voters,
            highWatermark,
            fetchingSnapshot
        );
    }

    @Override
    public void close() {
        fetchingSnapshot.ifPresent(RawSnapshotWriter::close);
    }

    private void logHighWatermarkUpdate(
        Optional<LogOffsetMetadata> oldHighWatermark,
        Optional<LogOffsetMetadata> newHighWatermark
    ) {
        if (!oldHighWatermark.equals(newHighWatermark)) {
            if (oldHighWatermark.isPresent()) {
                log.trace(
                    "High watermark set to {} from {} for epoch {}",
                    newHighWatermark,
                    oldHighWatermark.get(),
                    epoch
                );
            } else {
                log.info(
                    "High watermark set to {} for the first time for epoch {}",
                    newHighWatermark,
                    epoch
                );
            }
        }
    }
}
