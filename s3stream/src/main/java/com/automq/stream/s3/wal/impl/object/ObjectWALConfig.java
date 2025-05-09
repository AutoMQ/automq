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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.utils.IdURI;

import org.apache.commons.lang3.StringUtils;

public class ObjectWALConfig {
    private final long batchInterval;
    private final long maxBytesInBatch;
    private final long maxUnflushedBytes;
    private final int maxInflightUploadCount;
    private final int readAheadObjectCount;
    private final String clusterId;
    private final int nodeId;
    private final long epoch;
    private final boolean failover;
    private final short bucketId;
    private final boolean strictBatchLimit;

    public static Builder builder() {
        return new Builder();
    }

    public ObjectWALConfig(long batchInterval, long maxBytesInBatch, long maxUnflushedBytes, int maxInflightUploadCount,
        int readAheadObjectCount, String clusterId, int nodeId, long epoch, boolean failover, short bucketId,
        boolean strictBatchLimit) {
        this.batchInterval = batchInterval;
        this.maxBytesInBatch = maxBytesInBatch;
        this.maxUnflushedBytes = maxUnflushedBytes;
        this.maxInflightUploadCount = maxInflightUploadCount;
        this.readAheadObjectCount = readAheadObjectCount;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.failover = failover;
        this.bucketId = bucketId;
        this.strictBatchLimit = strictBatchLimit;
    }

    public long batchInterval() {
        return batchInterval;
    }

    public long maxBytesInBatch() {
        return maxBytesInBatch;
    }

    public long maxUnflushedBytes() {
        return maxUnflushedBytes;
    }

    public int maxInflightUploadCount() {
        return maxInflightUploadCount;
    }

    public int readAheadObjectCount() {
        return readAheadObjectCount;
    }

    public String clusterId() {
        return clusterId;
    }

    public int nodeId() {
        return nodeId;
    }

    public long epoch() {
        return epoch;
    }

    public boolean failover() {
        return failover;
    }

    public short bucketId() {
        return bucketId;
    }

    public boolean strictBatchLimit() {
        return strictBatchLimit;
    }

    public static final class Builder {
        private long batchInterval = 256; // 256ms
        private long maxBytesInBatch = 8 * 1024 * 1024L; // 8MB
        private long maxUnflushedBytes = 1024 * 1024 * 1024L; // 1GB
        private int maxInflightUploadCount = 50;
        private int readAheadObjectCount = 4;
        private String clusterId;
        private int nodeId;
        private long epoch;
        private boolean failover;
        private short bucketId;
        private boolean strictBatchLimit = false;

        private Builder() {
        }

        public Builder withURI(IdURI uri) {
            withBucketId(uri.id());

            String batchInterval = uri.extensionString("batchInterval");
            if (StringUtils.isNumeric(batchInterval)) {
                withBatchInterval(Long.parseLong(batchInterval));
            }
            String maxBytesInBatch = uri.extensionString("maxBytesInBatch");
            if (StringUtils.isNumeric(maxBytesInBatch)) {
                withMaxBytesInBatch(Long.parseLong(maxBytesInBatch));
            }
            String maxUnflushedBytes = uri.extensionString("maxUnflushedBytes");
            if (StringUtils.isNumeric(maxUnflushedBytes)) {
                withMaxUnflushedBytes(Long.parseLong(maxUnflushedBytes));
            }
            String maxInflightUploadCount = uri.extensionString("maxInflightUploadCount");
            if (StringUtils.isNumeric(maxInflightUploadCount)) {
                withMaxInflightUploadCount(Integer.parseInt(maxInflightUploadCount));
            }
            String readAheadObjectCount = uri.extensionString("readAheadObjectCount");
            if (StringUtils.isNumeric(readAheadObjectCount)) {
                withReadAheadObjectCount(Integer.parseInt(readAheadObjectCount));
            }
            String strictBatchLimit = uri.extensionString("strictBatchLimit");
            if (StringUtils.isNumeric(strictBatchLimit)) {
                withStrictBatchLimit(Boolean.parseBoolean(strictBatchLimit));
            }
            return this;
        }

        public Builder withBatchInterval(long batchInterval) {
            this.batchInterval = batchInterval;
            return this;
        }

        public Builder withMaxBytesInBatch(long maxBytesInBatch) {
            this.maxBytesInBatch = maxBytesInBatch;
            return this;
        }

        public Builder withMaxUnflushedBytes(long maxUnflushedBytes) {
            this.maxUnflushedBytes = maxUnflushedBytes;
            return this;
        }

        public Builder withMaxInflightUploadCount(int maxInflightUploadCount) {
            if (maxInflightUploadCount < 1) {
                maxInflightUploadCount = 1;
            }

            this.maxInflightUploadCount = maxInflightUploadCount;
            return this;
        }

        public Builder withReadAheadObjectCount(int readAheadObjectCount) {
            if (readAheadObjectCount < 1) {
                readAheadObjectCount = 1;
            }

            this.readAheadObjectCount = readAheadObjectCount;
            return this;
        }

        public Builder withClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder withNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder withEpoch(long epoch) {
            this.epoch = epoch;
            return this;
        }

        public Builder withFailover(boolean failover) {
            this.failover = failover;
            return this;
        }

        public Builder withBucketId(short bucketId) {
            this.bucketId = bucketId;
            return this;
        }

        public Builder withStrictBatchLimit(boolean strictBatchLimit) {
            this.strictBatchLimit = strictBatchLimit;
            return this;
        }

        public ObjectWALConfig build() {
            return new ObjectWALConfig(batchInterval, maxBytesInBatch, maxUnflushedBytes, maxInflightUploadCount, readAheadObjectCount, clusterId, nodeId, epoch, failover, bucketId, strictBatchLimit);
        }
    }
}
