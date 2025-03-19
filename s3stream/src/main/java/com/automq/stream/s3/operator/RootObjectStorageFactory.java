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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.CircuitBreaker;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RootObjectStorageFactory {
    public static final short ROOT = -1;
    private final Map<String, String> tagging;
    private final NetworkBandwidthLimiter inboundLimiter;
    private final NetworkBandwidthLimiter outboundLimiter;
    private final CircuitBreaker.NodeCircuitStatusManagerStub nodeCircuitStatusManagerStub;
    private final ObjectManager objectManager;
    private CircuitBreaker circuitBreaker;

    public RootObjectStorageFactory(
        Map<String, String> tagging,
        NetworkBandwidthLimiter inboundLimiter,
        NetworkBandwidthLimiter outboundLimiter,
        CircuitBreaker.NodeCircuitStatusManagerStub nodeCircuitStatusManagerStub,
        ObjectManager objectManager) {
        this.tagging = tagging;
        this.inboundLimiter = inboundLimiter;
        this.outboundLimiter = outboundLimiter;
        this.nodeCircuitStatusManagerStub = nodeCircuitStatusManagerStub;
        this.objectManager = objectManager;
    }

    public ObjectStorage get(ObjectStorageFactory.Builder builder) {
        return new Builder(builder).build();
    }

    class Builder {
        private final TopologyNode node;
        private final Type type;
        private final ObjectStorageFactory.Builder builder;

        public Builder(ObjectStorageFactory.Builder builder) {
            List<BucketURI> buckets = builder.buckets();
            this.type = builder.extension("type");
            this.builder = builder;
            if (type == Type.BACKGROUND) {
                TopologyNode node = parse(buckets);
                if (CircuitObjectStorage.PROTOCOL.equalsIgnoreCase(node.uri.protocol())) {
                    // background object force to use the main bucket
                    node = node.children().get(0);
                }
                this.node = node;
            } else {
                this.node = parse(buckets);
            }
        }

        public ObjectStorage build() {
            if (CircuitObjectStorage.PROTOCOL.equalsIgnoreCase(node.uri().protocol())) {
                ObjectStorage mainObjectStorage = leaf(node.children().get(0).uri());
                ObjectStorage fallbackObjectStorage = leaf(node.children().get(1).uri());
                if (nodeCircuitStatusManagerStub != null && circuitBreaker == null) {
                    circuitBreaker = new CircuitBreaker(mainObjectStorage, fallbackObjectStorage, nodeCircuitStatusManagerStub, objectManager, TimeUnit.MINUTES.toMillis(1));
                }
                return new CircuitObjectStorage(mainObjectStorage, fallbackObjectStorage, circuitBreaker);
            } else {
                return leaf(node.uri());
            }
        }

        private ObjectStorage leaf(BucketURI uri) {
            return ObjectStorageFactory.instance().builder(uri)
                .tagging(Optional.ofNullable(builder.tagging()).orElse(tagging))
                .inboundLimiter(Optional.ofNullable(builder.inboundLimiter()).orElse(inboundLimiter))
                .outboundLimiter(Optional.ofNullable(builder.outboundLimiter()).orElse(outboundLimiter))
                .readWriteIsolate(builder.readWriteIsolate())
                .threadPrefix(type.name())
                .build();
        }
    }

    public static TopologyNode parse(String bucketsStr) {
        return parse(BucketURI.parseBuckets(bucketsStr));
    }

    public static TopologyNode parse(List<BucketURI> buckets) {
        Map<Short, BucketURI> id2bucket = buckets.stream().collect(Collectors.toMap(BucketURI::bucketId, b -> b));
        if (id2bucket.isEmpty()) {
            throw new IllegalArgumentException("no valid bucket, buckets=" + id2bucket);
        }
        if (id2bucket.size() == 1) {
            BucketURI bucket = id2bucket.get(id2bucket.keySet().stream().iterator().next());
            return new TopologyNode(bucket);
        }
        if (!id2bucket.containsKey(ROOT)) {
            throw new IllegalArgumentException("missing root bucket, buckets=" + id2bucket);
        }
        BucketURI bucket = id2bucket.get(ROOT);
        TopologyNode root = new TopologyNode(bucket);
        return parse0(root, id2bucket);
    }

    private static TopologyNode parse0(TopologyNode parent, Map<Short, BucketURI> id2bucket) {
        if (CircuitObjectStorage.PROTOCOL.equalsIgnoreCase(parent.uri.protocol())) {
            short mainBucketId = Short.parseShort(parent.uri.extensionString("main"));
            short fallbackBucketId = Short.parseShort(parent.uri.extensionString("fallback"));
            BucketURI mainBucket = id2bucket.get(mainBucketId);
            BucketURI fallbackBucket = id2bucket.get(fallbackBucketId);
            if (mainBucket == null || fallbackBucket == null) {
                throw new IllegalArgumentException("missing main or fallback bucket, buckets=" + id2bucket);
            }
            parent.addChild(parse0(new TopologyNode(mainBucket), id2bucket));
            parent.addChild(parse0(new TopologyNode(fallbackBucket), id2bucket));
        }
        return parent;
    }

    public static class TopologyNode {
        BucketURI uri;
        List<TopologyNode> children;

        public TopologyNode(BucketURI bucketURI) {
            this.uri = bucketURI;
            this.children = new ArrayList<>();
        }

        public List<BucketURI> uris() {
            List<BucketURI> uris = new ArrayList<>();
            uris.add(uri);
            for (TopologyNode child : children) {
                uris.addAll(child.uris());
            }
            return uris;
        }

        public BucketURI uri() {
            return uri;
        }

        public void uri(BucketURI uri) {
            this.uri = uri;
        }

        public void addChild(TopologyNode child) {
            children.add(child);
        }

        public List<TopologyNode> children() {
            return children;
        }
    }

    public enum Type {
        MAIN,
        BACKGROUND
    }
}
