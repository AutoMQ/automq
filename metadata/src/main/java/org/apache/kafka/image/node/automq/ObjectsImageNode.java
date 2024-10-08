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

import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.node.MetadataLeafNode;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.metadata.stream.S3Object;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectsImageNode implements MetadataNode {
    public static final String NAME = "objects";

    private final S3ObjectsImage objectsImage;

    public ObjectsImageNode(S3ObjectsImage objectsImage) {
        this.objectsImage = objectsImage;
    }

    @Override
    public Collection<String> childNames() {
        List<Long> objectIdList = new LinkedList<>(objectsImage.objectIds());
        return objectIdList.stream().map(Object::toString).collect(Collectors.toList());
    }

    @Override
    public MetadataNode child(String name) {
        long objectId = Long.parseLong(name);
        S3Object object = objectsImage.getObjectMetadata(objectId);
        if (object == null) {
            return null;
        }
        return new MetadataLeafNode(object.toString());
    }
}
