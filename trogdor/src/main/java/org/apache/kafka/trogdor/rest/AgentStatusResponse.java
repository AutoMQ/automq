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

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.TreeMap;

/**
 * A response from the Trogdor agent about the worker states and specifications.
 */
public class AgentStatusResponse extends Message {
    private final long serverStartMs;
    private final TreeMap<Long, WorkerState> workers;

    @JsonCreator
    public AgentStatusResponse(@JsonProperty("serverStartMs") long serverStartMs,
            @JsonProperty("workers") TreeMap<Long, WorkerState> workers) {
        this.serverStartMs = serverStartMs;
        this.workers = workers == null ? new TreeMap<>() : workers;
    }

    @JsonProperty
    public long serverStartMs() {
        return serverStartMs;
    }

    @JsonProperty
    public TreeMap<Long, WorkerState> workers() {
        return workers;
    }
}
