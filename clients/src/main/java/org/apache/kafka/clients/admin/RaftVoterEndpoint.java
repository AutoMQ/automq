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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Locale;
import java.util.Objects;

/**
 * An endpoint for a raft quorum voter.
 */
@InterfaceStability.Stable
public class RaftVoterEndpoint {
    private final String name;
    private final String host;
    private final int port;

    static String requireNonNullAllCapsNonEmpty(String input) {
        if (input == null) {
            throw new IllegalArgumentException("Null argument not allowed.");
        }
        if (!input.trim().equals(input)) {
            throw new IllegalArgumentException("Leading or trailing whitespace is not allowed.");
        }
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Empty string is not allowed.");
        }
        if (!input.toUpperCase(Locale.ROOT).equals(input)) {
            throw new IllegalArgumentException("String must be UPPERCASE.");
        }
        return input;
    }

    /**
     * Create an endpoint for a metadata quorum voter.
     *
     * @param name              The human-readable name for this endpoint. For example, CONTROLLER.
     * @param host              The DNS hostname for this endpoint.
     * @param port              The network port for this endpoint.
     */
    public RaftVoterEndpoint(
        String name,
        String host,
        int port
    ) {
        this.name = requireNonNullAllCapsNonEmpty(name);
        this.host = Objects.requireNonNull(host);
        this.port = port;
    }

    public String name() {
        return name;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) return false;
        RaftVoterEndpoint other = (RaftVoterEndpoint) o;
        return name.equals(other.name) &&
            host.equals(other.host) &&
            port == other.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host, port);
    }

    @Override
    public String toString() {
        return "RaftVoterEndpoint" +
            "(name=" + name +
            ", host=" + host +
            ", port=" + port +
            ")";
    }
}
