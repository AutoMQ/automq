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

package org.apache.kafka.controller.availability;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterAvailabilitySnapshot {
    private final List<CoverageBroker> coverageBrokers;
    private final Map<Integer, BrokerAvailabilitySnapshot> brokerSnapshots;
    private final long nowMs;
    private final long staleMs;

    public ClusterAvailabilitySnapshot(List<CoverageBroker> coverageBrokers,
                                       Map<Integer, BrokerAvailabilitySnapshot> brokerSnapshots,
                                       long nowMs,
                                       long staleMs) {
        this.coverageBrokers = new ArrayList<>(coverageBrokers);
        this.brokerSnapshots = new HashMap<>(brokerSnapshots);
        this.nowMs = nowMs;
        this.staleMs = staleMs;
    }

    public List<CoverageBroker> coverageBrokers() {
        return coverageBrokers;
    }

    public Map<Integer, BrokerAvailabilitySnapshot> brokerSnapshots() {
        return brokerSnapshots;
    }

    public long nowMs() {
        return nowMs;
    }

    public long staleMs() {
        return staleMs;
    }
}
