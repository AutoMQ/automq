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

import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ObjectStorageFactory {
    private static volatile ObjectStorageFactory instance;
    private final Map<String /* protocol */, Function<Builder, ObjectStorage>> protocolHandlers = new HashMap<>();

    static {
        ObjectStorageFactory.instance()
            .registerProtocolHandler("s3", builder ->
                AwsObjectStorage.builder()
                    .bucket(builder.bucketURI)
                    .tagging(builder.tagging)
                    .inboundLimiter(builder.inboundLimiter)
                    .outboundLimiter(builder.outboundLimiter)
                    .readWriteIsolate(builder.readWriteIsolate)
                    .checkS3ApiModel(builder.checkS3ApiModel)
                    .threadPrefix(builder.threadPrefix)
                    .build())
            .registerProtocolHandler("mem", builder -> new MemoryObjectStorage(builder.bucketURI.bucketId()));
    }

    private ObjectStorageFactory() {
    }

    public ObjectStorageFactory registerProtocolHandler(String protocol,
        Function<Builder, ObjectStorage> handler) {
        protocolHandlers.put(protocol, handler);
        return this;
    }

    public Builder builder(BucketURI bucket) {
        return new Builder().bucket(bucket);
    }

    public static ObjectStorageFactory instance() {
        if (instance == null) {
            synchronized (ObjectStorageFactory.class) {
                if (instance == null) {
                    instance = new ObjectStorageFactory();
                }
            }
        }
        return instance;
    }

    public class Builder {
        private final AtomicLong defaultThreadPrefixCounter = new AtomicLong();
        private BucketURI bucketURI;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
        private NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;
        private boolean readWriteIsolate;
        private boolean checkS3ApiModel = false;
        private String threadPrefix = "";

        Builder bucket(BucketURI bucketURI) {
            this.bucketURI = bucketURI;
            return this;
        }

        public BucketURI bucket() {
            return bucketURI;
        }

        public Builder tagging(Map<String, String> tagging) {
            this.tagging = tagging;
            return this;
        }

        public Map<String, String> tagging() {
            return tagging;
        }

        public Builder inboundLimiter(NetworkBandwidthLimiter inboundLimiter) {
            this.inboundLimiter = inboundLimiter;
            return this;
        }

        public NetworkBandwidthLimiter inboundLimiter() {
            return inboundLimiter;
        }

        public Builder outboundLimiter(NetworkBandwidthLimiter outboundLimiter) {
            this.outboundLimiter = outboundLimiter;
            return this;
        }

        public NetworkBandwidthLimiter outboundLimiter() {
            return outboundLimiter;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public boolean readWriteIsolate() {
            return readWriteIsolate;
        }

        public Builder checkS3ApiModel(boolean checkS3ApiModel) {
            this.checkS3ApiModel = checkS3ApiModel;
            return this;
        }

        public boolean checkS3ApiModel() {
            return checkS3ApiModel;
        }

        public Builder threadPrefix(String prefix) {
            this.threadPrefix = prefix;
            return this;
        }

        public ObjectStorage build() {
            if (StringUtils.isEmpty(this.threadPrefix)) {
                this.threadPrefix = Long.toString(defaultThreadPrefixCounter.getAndIncrement());
            }
            return protocolHandlers.get(bucketURI.protocol()).apply(this);
        }
    }
}
