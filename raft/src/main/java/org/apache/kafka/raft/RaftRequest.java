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
import org.apache.kafka.common.protocol.ApiMessage;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public abstract class RaftRequest implements RaftMessage {
    private final int correlationId;
    private final ApiMessage data;
    private final long createdTimeMs;

    public RaftRequest(int correlationId, ApiMessage data, long createdTimeMs) {
        this.correlationId = correlationId;
        this.data = data;
        this.createdTimeMs = createdTimeMs;
    }

    @Override
    public int correlationId() {
        return correlationId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public static final class Inbound extends RaftRequest {
        private final short apiVersion;
        private final ListenerName listenerName;
        // AutoMQ inject start
        private final Optional<String> connectionId;
        // AutoMQ inject end

        public final CompletableFuture<RaftResponse.Outbound> completion = new CompletableFuture<>();

        public Inbound(
            ListenerName listenerName,
            int correlationId,
            short apiVersion,
            ApiMessage data,
            long createdTimeMs
        ) {
            // AutoMQ inject start
            this(listenerName, correlationId, apiVersion, data, createdTimeMs, null);
            // AutoMQ inject end
        }

        // AutoMQ inject start
        /**
         * Create an inbound Raft request with the server-side network connection identifier.
         */
        public Inbound(
            ListenerName listenerName,
            int correlationId,
            short apiVersion,
            ApiMessage data,
            long createdTimeMs,
            String connectionId
        ) {
            super(correlationId, data, createdTimeMs);

            this.listenerName = listenerName;
            this.apiVersion = apiVersion;
            this.connectionId = Optional.ofNullable(connectionId);
        }

        /**
         * Return the server-side connection identifier when the request came from the network.
         */
        public Optional<String> connectionId() {
            return connectionId;
        }
        // AutoMQ inject end

        public short apiVersion() {
            return apiVersion;
        }

        public ListenerName listenerName() {
            return listenerName;
        }

        @Override
        public String toString() {
            return String.format(
                "InboundRequest(listenerName=%s, correlationId=%d, apiVersion=%d, data=%s, " +
                "createdTimeMs=%d)",
                listenerName,
                correlationId(),
                apiVersion,
                data(),
                createdTimeMs()
            );
        }
    }

    public static final class Outbound extends RaftRequest {
        private final Node destination;
        public final CompletableFuture<RaftResponse.Inbound> completion = new CompletableFuture<>();

        public Outbound(int correlationId, ApiMessage data, Node destination, long createdTimeMs) {
            super(correlationId, data, createdTimeMs);
            this.destination = destination;
        }

        public Node destination() {
            return destination;
        }

        @Override
        public String toString() {
            return String.format(
                "OutboundRequest(correlationId=%d, data=%s, createdTimeMs=%d, destination=%s)",
                correlationId(),
                data(),
                createdTimeMs(),
                destination
            );
        }
    }
}
