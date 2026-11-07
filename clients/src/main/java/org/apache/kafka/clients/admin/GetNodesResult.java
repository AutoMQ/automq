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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

import java.util.List;

public class GetNodesResult {
    private final KafkaFuture<Response> future;

    public GetNodesResult(KafkaFuture<Response> future) {
        this.future = future;
    }

    public KafkaFuture<List<NodeMetadata>> nodes() {
        return future.thenApply(Response::nodes);
    }

    public KafkaFuture<RouterChannelEpoch> routerChannelEpoch() {
        return future.thenApply(Response::routerChannelEpoch);
    }

    static class Response {
        private final List<NodeMetadata> nodes;
        private final RouterChannelEpoch routerChannelEpoch;

        Response(List<NodeMetadata> nodes, RouterChannelEpoch routerChannelEpoch) {
            this.nodes = nodes;
            this.routerChannelEpoch = routerChannelEpoch;
        }

        List<NodeMetadata> nodes() {
            return nodes;
        }

        RouterChannelEpoch routerChannelEpoch() {
            return routerChannelEpoch;
        }
    }

    public static class RouterChannelEpoch {
        private final long committed;
        private final long fenced;
        private final long current;
        private final long lastBumpUpTimestamp;

        public RouterChannelEpoch(long committed, long fenced, long current, long lastBumpUpTimestamp) {
            this.committed = committed;
            this.fenced = fenced;
            this.current = current;
            this.lastBumpUpTimestamp = lastBumpUpTimestamp;
        }

        public long getCommitted() {
            return committed;
        }

        public long getFenced() {
            return fenced;
        }

        public long getCurrent() {
            return current;
        }

        public long getLastBumpUpTimestamp() {
            return lastBumpUpTimestamp;
        }

        @Override
        public String toString() {
            return "RouterChannelEpoch{" +
                "committed=" + committed +
                ", fenced=" + fenced +
                ", current=" + current +
                ", lastBumpUpTimestamp=" + lastBumpUpTimestamp +
                '}';
        }
    }
}
