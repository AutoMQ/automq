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
