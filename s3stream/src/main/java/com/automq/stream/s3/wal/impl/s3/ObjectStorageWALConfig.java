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

package com.automq.stream.s3.wal.impl.s3;

public class ObjectStorageWALConfig {
    private final long batchInterval;
    private final long maxBytesInBatch;
    private final long maxUnflushedBytes;
    private final int maxInflightUploadCount;
    private final String clusterId;
    private final int nodeId;
    private final long epoch;
    private final boolean failover;

    public static Builder builder() {
        return new Builder();
    }

    public ObjectStorageWALConfig(long batchInterval, long maxBytesInBatch, long maxUnflushedBytes, int maxInflightUploadCount,
        String clusterId, int nodeId, long epoch, boolean failover) {
        this.batchInterval = batchInterval;
        this.maxBytesInBatch = maxBytesInBatch;
        this.maxUnflushedBytes = maxUnflushedBytes;
        this.maxInflightUploadCount = maxInflightUploadCount;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.failover = failover;
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

    public static final class Builder {
        private long batchInterval = 100; // 100ms
        private long maxBytesInBatch = 4 * 1024 * 1024L; // 4MB
        private long maxUnflushedBytes = 1024 * 1024 * 1024L; // 1GB
        private int maxInflightUploadCount = 50;
        private String clusterId;
        private int nodeId;
        private long epoch;
        private boolean failover;

        private Builder() {
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
            this.maxInflightUploadCount = maxInflightUploadCount;
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

        public ObjectStorageWALConfig build() {
            return new ObjectStorageWALConfig(batchInterval, maxBytesInBatch, maxUnflushedBytes, maxInflightUploadCount, clusterId, nodeId, epoch, failover);
        }
    }
}
