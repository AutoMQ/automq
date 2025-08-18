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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.network.NetworkBandwidthLimiter;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ObjectStorageFactory {
    public static final String PROTOCOL_ROOT = "root";
    public static final String EXTENSION_TYPE_KEY = "type";
    public static final String EXTENSION_TYPE_MAIN = "main";
    public static final String EXTENSION_TYPE_BACKGROUND = "background";
    private static volatile ObjectStorageFactory instance;
    private final Map<String /* protocol */, Function<Builder, ObjectStorage>> protocolHandlers = new HashMap<>();

    static {
        ObjectStorageFactory.instance().registerProtocolHandler(PROTOCOL_ROOT, builder -> {
            throw new UnsupportedOperationException();
        });
        ObjectStorageFactory.instance()
            .registerProtocolHandler("s3", builder ->
                AwsObjectStorage.builder()
                    .bucket(builder.bucket)
                    .tagging(builder.tagging)
                    .inboundLimiter(builder.inboundLimiter)
                    .outboundLimiter(builder.outboundLimiter)
                    .readWriteIsolate(builder.readWriteIsolate)
                    .checkS3ApiModel(builder.checkS3ApiModel)
                    .threadPrefix(builder.threadPrefix)
                    .build())
            .registerProtocolHandler("mem", builder -> new MemoryObjectStorage(builder.bucket.bucketId()))
            .registerProtocolHandler("file", builder ->
                LocalFileObjectStorage.builder()
                    .bucket(builder.bucket)
                    .build());
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

    public Builder builder() {
        return new Builder();
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
        private BucketURI bucket;
        private List<BucketURI> buckets;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
        private NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;
        private boolean readWriteIsolate;
        private boolean checkS3ApiModel = false;
        private String threadPrefix = "";
        private final Map<String, Object> extensions = new HashMap<>();

        Builder bucket(BucketURI bucketURI) {
            this.bucket = bucketURI;
            return this;
        }

        public BucketURI bucket() {
            return bucket;
        }

        public Builder buckets(List<BucketURI> buckets) {
            this.buckets = buckets;
            if (bucket == null && buckets.size() == 1) {
                bucket = buckets.get(0);
            }
            return this;
        }

        public List<BucketURI> buckets() {
            return buckets;
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
            if (prefix == null) {
                return this;
            }
            this.threadPrefix = prefix;
            return this;
        }

        public String threadPrefix() {
            return threadPrefix;
        }

        public Builder extension(String key, Object value) {
            this.extensions.put(key, value);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> T extension(String key) {
            return (T) this.extensions.get(key);
        }

        public Map<String, Object> extensions() {
            return extensions;
        }

        public ObjectStorage build() {
            if (StringUtils.isEmpty(this.threadPrefix)) {
                this.threadPrefix = Long.toString(defaultThreadPrefixCounter.getAndIncrement());
            }
            Function<Builder, ObjectStorage> protocolHandle;
            if (buckets != null && buckets.size() > 1) {
                protocolHandle = protocolHandlers.get(PROTOCOL_ROOT);
            } else {
                protocolHandle = protocolHandlers.get(bucket.protocol());
            }
            if (protocolHandle == null) {
                throw new IllegalArgumentException("Cannot find protocol " + bucket.protocol());
            }
            return protocolHandle.apply(this);
        }
    }
}
