/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image.node.automq;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import org.apache.kafka.image.KVImage;
import org.apache.kafka.image.node.MetadataLeafNode;
import org.apache.kafka.image.node.MetadataNode;

public class KVImageNode implements MetadataNode {
    public final static String NAME = "kv";
    private final KVImage kvImage;

    public KVImageNode(KVImage kvImage) {
        this.kvImage = kvImage;
    }

    @Override
    public Collection<String> childNames() {
        return kvImage.kv().keySet();
    }

    @Override
    public MetadataNode child(String name) {
        ByteBuffer valueBuf = kvImage.kv().get(name);
        if (valueBuf == null) {
            return null;
        }
        byte[] value = new byte[valueBuf.remaining()];
        valueBuf.duplicate().get(value);
        return new MetadataLeafNode(Arrays.toString(value));
    }
}
