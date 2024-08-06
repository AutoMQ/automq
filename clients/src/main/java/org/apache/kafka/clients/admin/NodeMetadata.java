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

package org.apache.kafka.clients.admin;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.AutomqGetNodesResponseData;

public class NodeMetadata {
    public static final Pattern BLOCK_WAL_LIST_PATTERN = Pattern.compile("\\d+@block://(.+?)(?=\\?|,\\d+@|$)");

    private final int nodeId;
    private final long nodeEpoch;
    private final List<Disk> disks;
    private final String state;
    private final boolean hasOpeningStreams;
    private final Map<String, String> tags;

    public NodeMetadata(AutomqGetNodesResponseData.NodeMetadata nodeMetadata) {
        this.nodeId = nodeMetadata.nodeId();
        this.nodeEpoch = nodeMetadata.nodeEpoch();
        this.disks = toDisks(nodeMetadata.walConfig());
        this.state = nodeMetadata.state();
        this.hasOpeningStreams = nodeMetadata.hasOpeningStreams();
        this.tags = nodeMetadata.tags().stream().collect(Collectors.toMap(AutomqGetNodesResponseData.Tag::key, AutomqGetNodesResponseData.Tag::value));
    }

    private static List<Disk> toDisks(String walConfig) {
        return BLOCK_WAL_LIST_PATTERN.matcher(walConfig).results()
            .map(m -> m.group(1))
            .map(Disk::new)
            .collect(Collectors.toList());
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    public List<Disk> getDisks() {
        return disks;
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
            ", disks=" + disks +
            ", state='" + state + '\'' +
            ", hasOpeningStreams=" + hasOpeningStreams +
            ", tags=" + tags +
            '}';
    }

    public static class Disk {
        private final String id;

        public Disk(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Disk{" +
                "id='" + id + '\'' +
                '}';
        }
    }
}
