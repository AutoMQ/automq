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

import org.apache.kafka.common.message.AutomqGetNodesResponseData;

import java.util.Map;
import java.util.stream.Collectors;

public class NodeMetadata {
    private final int nodeId;
    private final long nodeEpoch;
    private final String walConfig;
    private final String state;
    private final boolean hasOpeningStreams;
    private final Map<String, String> tags;

    public NodeMetadata(AutomqGetNodesResponseData.NodeMetadata nodeMetadata) {
        this.nodeId = nodeMetadata.nodeId();
        this.nodeEpoch = nodeMetadata.nodeEpoch();
        this.walConfig = nodeMetadata.walConfig();
        this.state = nodeMetadata.state();
        this.hasOpeningStreams = nodeMetadata.hasOpeningStreams();
        this.tags = nodeMetadata.tags().stream().collect(Collectors.toMap(AutomqGetNodesResponseData.Tag::key, AutomqGetNodesResponseData.Tag::value));
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    public String getWalConfig() {
        return walConfig;
    }

    public String getState() {
        return state;
    }

    public boolean isHasOpeningStreams() {
        return hasOpeningStreams;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "NodeMetadata{" +
            "nodeId=" + nodeId +
            ", nodeEpoch=" + nodeEpoch +
            ", walConfig='" + walConfig + '\'' +
            ", state='" + state + '\'' +
            ", hasOpeningStreams=" + hasOpeningStreams +
            ", tags=" + tags +
            '}';
    }
}
