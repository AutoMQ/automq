/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image.node.automq;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.timeline.TimelineHashMap;

public class StreamsImageNode implements MetadataNode {
    public final static String NAME = "streams";

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
