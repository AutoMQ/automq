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
import org.apache.kafka.image.node.MetadataLeafNode;
import org.apache.kafka.image.node.MetadataNode;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class KVImageNode implements MetadataNode {
    public static final String NAME = "kv";
    private final KVImage kvImage;

    public KVImageNode(KVImage kvImage) {
        this.kvImage = kvImage;
    }

    @Override
    public Collection<String> childNames() {
        List<String> keys = new LinkedList<>();
        kvImage.kvs().forEach((k, v) -> keys.add(k));
        return keys;
    }

    @Override
    public MetadataNode child(String name) {
        ByteBuffer valueBuf = kvImage.getValue(name);
        if (valueBuf == null) {
            return null;
        }
        byte[] value = new byte[valueBuf.remaining()];
        valueBuf.duplicate().get(value);
        return new MetadataLeafNode(Arrays.toString(value));
    }
}
