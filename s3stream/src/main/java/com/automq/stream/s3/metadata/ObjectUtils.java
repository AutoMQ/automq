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

package com.automq.stream.s3.metadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

public class ObjectUtils {
    public static final long NOOP_OBJECT_ID = -1L;
    public static final long NOOP_OFFSET = -1L;
    private static final String OBJECT_TAG_KEY = "s3stream:namespace";
    private static final String SPARSE_INDEX_OBJECT_HASH_MIX = "sparse-index";
    private static String namespace = "DEFAULT";

    public static void setNamespace(String namespace) {
        ObjectUtils.namespace = namespace;
    }

    public static void main(String[] args) {
        System.out.printf("%s%n", genKey(0, 11154));
    }

    public static String genIndexKey(int version, long nodeId) {
        if (namespace.isEmpty()) {
            throw new IllegalStateException("NAMESPACE is not set");
        }
        return genIndexKey(version, namespace, nodeId);
    }

    public static String genIndexKey(int version, String namespace, long nodeId) {
        if (version == 0) {
            String hashPrefix = String.format("%08x", (SPARSE_INDEX_OBJECT_HASH_MIX + nodeId).hashCode());
            return hashPrefix + "/" + namespace + "/node-" + nodeId;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    public static String genKey(int version, long objectId) {
        if (namespace.isEmpty()) {
            throw new IllegalStateException("NAMESPACE is not set");
        }
        return genKey(version, namespace, objectId);
    }

    public static String genKey(int version, String namespace, long objectId) {
        if (version == 0) {
            String objectIdHex = String.format("%08x", objectId);
            String hashPrefix = new StringBuilder(objectIdHex).reverse().toString();
            return hashPrefix + "/" + namespace + "/" + objectId;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    public static long parseObjectId(int version, String key) {
        if (namespace.isEmpty()) {
            throw new IllegalStateException("NAMESPACE is not set");
        }
        return parseObjectId(version, key, namespace);
    }

    public static long parseObjectId(int version, String key, String namespace) {
        if (version == 0) {
            String[] parts = key.split("/");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid key: " + key);
            }
            return Long.parseLong(parts[2]);
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

    /**
     * Convert a list of tags to a tagging object.
     * It will return null if the input is null.
     */
    public static Tagging tagging(Map<String, String> tagging) {
        if (null == tagging) {
            return null;
        }
        List<Tag> tags = tagging.entrySet().stream()
            .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()))
            .map(Tag.Builder::build)
            .collect(Collectors.toList());
        return Tagging.builder().tagSet(tags).build();
    }
}
