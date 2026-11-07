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

import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class StreamsImageNode implements MetadataNode {
    public static final String NAME = "streams";

    private final TimelineHashMap<Long, S3StreamMetadataImage> streams;

    public StreamsImageNode(TimelineHashMap<Long, S3StreamMetadataImage> streams) {
        this.streams = streams;
    }

    @Override
    public Collection<String> childNames() {
        List<String> streamIdList = new LinkedList<>();
        streams.forEach((streamId, metadata) -> streamIdList.add(Long.toString(streamId)));
        return streamIdList;
    }

    @Override
    public MetadataNode child(String name) {
        long streamId = Long.parseLong(name);
        S3StreamMetadataImage image = streams.get(streamId);
        if (image == null) {
            return null;
        }
        return new StreamImageNode(image);
    }
}
