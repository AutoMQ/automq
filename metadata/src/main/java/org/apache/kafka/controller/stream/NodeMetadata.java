/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.controller.stream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;

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
    public String toString() {
        return "NodeMetadata{" +
            "nodeId=" + nodeId +
            ", nodeEpoch=" + nodeEpoch +
            ", walConfig='" + walConfig + '\'' +
            ", tags=" + tags +
            '}';
    }
}
