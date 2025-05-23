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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.message.AutomqGetNodesResponseData;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeMetadata {

    @JsonProperty("i")
    private int nodeId;

    @JsonProperty("e")
    private long nodeEpoch;

    @JsonProperty("w")
    private String walConfig;

    @JsonProperty("t")
    private Map<String, String> tags;

    public NodeMetadata() {
    }

    public NodeMetadata(int nodeId, long nodeEpoch, String walConfig, Map<String, String> tags) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.walConfig = walConfig;
        this.tags = tags;
    }

    public AutomqGetNodesResponseData.NodeMetadata to() {
        AutomqGetNodesResponseData.TagCollection tags = new AutomqGetNodesResponseData.TagCollection();
        this.tags.forEach((k, v) -> tags.add(new AutomqGetNodesResponseData.Tag().setKey(k).setValue(v)));
        return new AutomqGetNodesResponseData.NodeMetadata()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch)
            .setWalConfig(walConfig)
            .setTags(tags);
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    public void setNodeEpoch(long nodeEpoch) {
        this.nodeEpoch = nodeEpoch;
    }

    public String getWalConfig() {
        return walConfig;
    }

    public void setWalConfig(String walConfig) {
        this.walConfig = walConfig;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        NodeMetadata that = (NodeMetadata) o;
        return nodeId == that.nodeId && nodeEpoch == that.nodeEpoch && Objects.equals(walConfig, that.walConfig) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeEpoch, walConfig, tags);
    }

    @Override
    public String toString() {
        return "NodeMetadata{" +
            "nodeId=" + nodeId +
            ", nodeEpoch=" + nodeEpoch +
            ", walConfig='" + walConfig + '\'' +
            ", tags=" + tags +
            '}';
    }
}
