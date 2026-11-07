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
