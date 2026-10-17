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

package kafka.automq.failover;

import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.stream.NodeMetadata;
import org.apache.kafka.controller.stream.NodeState;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * NodeRuntimeMetadata is a runtime view of a node's metadata.
 *
 * @see NodeMetadata
 */
public final class NodeRuntimeMetadata {

    /**
     * Expect the Node ID of a broker (as opposed to a controller) will start from 1000.
     *
     * @see ClusterControlManager#getNextNodeId()
     */
    private static final int MAX_CONTROLLER_ID = 1000 - 1;
    private static final long DONT_FAILOVER_AFTER_NEW_EPOCH_MS = TimeUnit.MINUTES.toMillis(1);
    private final int id;
    private final long epoch;
    private final String walConfigs;
    private final Map<String, String> tags;
    private final NodeState state;
    private final boolean hasOpeningStreams;

    /**
     *
     */
    public NodeRuntimeMetadata(int id, long epoch, String walConfigs, Map<String, String> tags, NodeState state,
        boolean hasOpeningStreams) {
        this.id = id;
        this.epoch = epoch;
        this.walConfigs = walConfigs;
        this.tags = tags;
        this.state = state;
        this.hasOpeningStreams = hasOpeningStreams;
    }

    public boolean shouldFailover() {
        return isFenced() && hasOpeningStreams
            // The node epoch is the start timestamp of node.
            // We need to avoid failover just after node restart.
            // The node may take some time to recover its data.
            && System.currentTimeMillis() - epoch > DONT_FAILOVER_AFTER_NEW_EPOCH_MS;
    }

    public boolean isFenced() {
        return NodeState.FENCED == state;
    }

    public boolean isActive() {
        return NodeState.ACTIVE == state;
    }

    public boolean isController() {
        return id <= MAX_CONTROLLER_ID;
    }

    public static boolean isController(int nodeId) {
        return nodeId <= MAX_CONTROLLER_ID;
    }

    private String getTagOrThrow(String key) {
        String value = tags.get(key);
        if (value == null) {
            throw new IllegalStateException(String.format("Node %d is missing tag %s, tags: %s", id, key, tags));
        }
        return value;
    }

    public int id() {
        return id;
    }

    public long epoch() {
        return epoch;
    }

    public String walConfigs() {
        return walConfigs;
    }

    public Map<String, String> tags() {
        return tags;
    }

    public NodeState state() {
        return state;
    }

    public boolean hasOpeningStreams() {
        return hasOpeningStreams;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (NodeRuntimeMetadata) obj;
        return this.id == that.id &&
            this.epoch == that.epoch &&
            Objects.equals(this.walConfigs, that.walConfigs) &&
            Objects.equals(this.tags, that.tags) &&
            Objects.equals(this.state, that.state) &&
            this.hasOpeningStreams == that.hasOpeningStreams;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, epoch, walConfigs, tags, state, hasOpeningStreams);
    }

    @Override
    public String toString() {
        return "NodeRuntimeMetadata[" +
            "id=" + id + ", " +
            "epoch=" + epoch + ", " +
            "walConfigs=" + walConfigs + ", " +
            "tags=" + tags + ", " +
            "state=" + state + ", " +
            "hasOpeningStreams=" + hasOpeningStreams + ']';
    }

}
