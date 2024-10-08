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

package org.apache.kafka.image.node.automq;

import org.apache.kafka.image.NodeS3StreamSetObjectMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class NodesImageNode implements MetadataNode {
    public static final String NAME = "nodes";
    private final TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodes;

    public NodesImageNode(TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodes) {
        this.nodes = nodes;
    }

    @Override
    public Collection<String> childNames() {
        List<String> childNames = new LinkedList<>();
        nodes.forEach((nodeId, metadata) -> childNames.add(Integer.toString(nodeId)));
        return childNames;
    }

    @Override
    public MetadataNode child(String name) {
        NodeS3StreamSetObjectMetadataImage node = nodes.get(Integer.parseInt(name));
        return node != null ? new NodeImageNode(node) : null;
    }
}
