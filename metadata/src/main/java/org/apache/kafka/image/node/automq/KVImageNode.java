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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KVImageNode implements MetadataNode {
    public static final String NAME = "kv";
    /** Node name used for KV entries that have no namespace (legacy v0 records). */
    public static final String DEFAULT_NAMESPACE = "__automq_default";

    private final KVImage kvImage;

    public KVImageNode(KVImage kvImage) {
        this.kvImage = kvImage;
    }

    @Override
    public Collection<String> childNames() {
        // Return the set of namespace node names
        Map<String, Object> namespaces = new HashMap<>();
        kvImage.kvs().forEach((k, v) -> namespaces.put(nodeName(k.namespace()), null));
        return namespaces.keySet();
    }

    @Override
    public MetadataNode child(String name) {
        // name is a namespace node name; return a sub-node for that namespace
        String namespace = name.equals(DEFAULT_NAMESPACE) ? null : name;
        Map<String, ByteBuffer> entries = new HashMap<>();
        kvImage.kvs().forEach((k, v) -> {
            if (Objects.equals(k.namespace(), namespace)) {
                entries.put(k.key(), v);
            }
        });
        if (entries.isEmpty()) {
            return null;
        }
        return new NamespaceNode(entries);
    }

    private static String nodeName(String namespace) {
        return namespace == null ? DEFAULT_NAMESPACE : namespace;
    }

    /** A directory node representing one namespace, whose children are the KV keys. */
    private static class NamespaceNode implements MetadataNode {
        private final Map<String, ByteBuffer> entries;

        NamespaceNode(Map<String, ByteBuffer> entries) {
            this.entries = entries;
        }

        @Override
        public Collection<String> childNames() {
            return entries.keySet();
        }

        @Override
        public MetadataNode child(String name) {
            ByteBuffer valueBuf = entries.get(name);
            if (valueBuf == null) {
                return null;
            }
            byte[] value = new byte[valueBuf.remaining()];
            valueBuf.duplicate().get(value);
            return new MetadataLeafNode(Arrays.toString(value));
        }
    }
}
