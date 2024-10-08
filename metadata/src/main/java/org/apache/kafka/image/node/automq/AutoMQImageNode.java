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

import org.apache.kafka.image.KVImage;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.image.node.MetadataNode;

import java.util.Arrays;
import java.util.Collection;

public class AutoMQImageNode implements MetadataNode {
    public static final String NAME = "automq";

    private final KVImage kvImage;
    private final S3StreamsMetadataImage streamsMetadataImage;
    private final S3ObjectsImage objectsImage;

    public AutoMQImageNode(KVImage kvImage, S3StreamsMetadataImage streamsMetadataImage, S3ObjectsImage objectsImage) {
        this.kvImage = kvImage;
        this.streamsMetadataImage = streamsMetadataImage;
        this.objectsImage = objectsImage;
    }

    @Override
    public Collection<String> childNames() {
        return Arrays.asList(KVImageNode.NAME, StreamsImageNode.NAME, NodesImageNode.NAME, ObjectsImageNode.NAME);
    }

    @Override
    public MetadataNode child(String name) {
        switch (name) {
            case KVImageNode.NAME:
                return new KVImageNode(kvImage);
            case StreamsImageNode.NAME:
                return new StreamsImageNode(streamsMetadataImage.timelineStreamMetadata());
            case NodesImageNode.NAME:
                return new NodesImageNode(streamsMetadataImage.timelineNodeMetadata());
            case ObjectsImageNode.NAME:
                return new ObjectsImageNode(objectsImage);
            default:
                return null;
        }
    }
}
