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

import java.io.Closeable;
import java.util.Optional;

public interface EpochState extends Closeable {

    default Optional<LogOffsetMetadata> highWatermark() {
        return Optional.empty();
    }

    /**
     * Decide whether to grant a vote to a candidate.
     *
     * It is the responsibility of the caller to invoke
     * {@link QuorumState#transitionToUnattachedVotedState(int, ReplicaKey)} if vote is granted.
     *
     * @param candidateKey the id and directory of the candidate
     * @param isLogUpToDate whether the candidate’s log is at least as up-to-date as receiver’s log
     * @return true if it can grant the vote, false otherwise
     */
    boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate);

    /**
     * Get the current election state, which is guaranteed to be immutable.
     */
    ElectionState election();

    /**
     * Get the current (immutable) epoch.
     */
    int epoch();

    /**
     * Returns the known endpoints for the leader.
     *
     * If the leader is not known then {@code Endpoints.empty()} is returned.
     */
    Endpoints leaderEndpoints();

    /**
     * User-friendly description of the state
     */
    String name();
}
