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
