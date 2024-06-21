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

package com.automq.stream.s3;

import com.automq.stream.Version;
import java.util.Map;
import java.util.function.Supplier;

// TODO: rename & init
public class Config {
    private int nodeId;
    private String endpoint;
    private String region;
    private String bucket;
    private boolean forcePathStyle = false;
    private String walPath = "/tmp/s3stream_wal";
    private long walCacheSize = 200 * 1024 * 1024;
    private long walCapacity = 1024L * 1024 * 1024;
    private int walInitBufferSize = 1024 * 1024;
    private int walMaxBufferSize = 16 * 1024 * 1024;
    private int walThread = 8;
    private long walWindowInitial = 1048576L;
    private long walWindowIncrement = 4194304L;
    private long walWindowMax = 536870912L;
    private long walBlockSoftLimit = 256 * 1024;
    private int walWriteRateLimit = 3000;
    private long walUploadThreshold = 100 * 1024 * 1024;
    private int streamSplitSize = 16777216;
    private int objectBlockSize = 1048576;
    private int objectPartSize = 16777216;
    private Map<String, String> objectTagging = null;
    private long blockCacheSize = 100 * 1024 * 1024;
    private int streamObjectCompactionIntervalMinutes = 60;
    private long streamObjectCompactionMaxSizeBytes = 10737418240L;
    private int controllerRequestRetryMaxCount = Integer.MAX_VALUE;
    private long controllerRequestRetryBaseDelayMs = 500;
    private long nodeEpoch = 0L;
    private int streamSetObjectCompactionInterval = 20;
    private long streamSetObjectCompactionCacheSize = 200 * 1024 * 1024;
    private int streamSetObjectCompactionUploadConcurrency = 8;
    private long streamSetObjectCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int streamSetObjectCompactionForceSplitPeriod = 120;
    private int streamSetObjectCompactionMaxObjectNum = 500;
    private int maxStreamNumPerStreamSetObject = 100000;
    private int maxStreamObjectNumPerCommit = 10000;
    private boolean mockEnable = false;
    private boolean objectLogEnable = false;
    // 100MB/s
    private long networkBaselineBandwidth = 100 * 1024 * 1024;
    private int refillPeriodMs = 10;
    private long objectRetentionTimeInSecond = 10 * 60; // 10min
    private boolean failoverEnable = false;
    private Supplier<Version> version = () -> {
        throw new UnsupportedOperationException();
    };

    public int nodeId() {
        return nodeId;
    }

    public String endpoint() {
        return endpoint;
    }

    public String region() {
        return region;
    }

    public String bucket() {
        return bucket;
    }

    public boolean forcePathStyle() {
        return forcePathStyle;
    }

    public String walPath() {
        return walPath;
    }

    public long walCacheSize() {
        return walCacheSize;
    }

    public long walCapacity() {
        return walCapacity;
    }

    public int walInitBufferSize() {
        return walInitBufferSize;
    }

    public int walMaxBufferSize() {
        return walMaxBufferSize;
    }

    public int walThread() {
        return walThread;
    }

    public long walWindowInitial() {
        return walWindowInitial;
    }

    public long walWindowIncrement() {
        return walWindowIncrement;
    }

    public long walWindowMax() {
        return walWindowMax;
    }

    public long walBlockSoftLimit() {
        return walBlockSoftLimit;
    }

    public int walWriteRateLimit() {
        return walWriteRateLimit;
    }

    public long walUploadThreshold() {
        return walUploadThreshold;
    }

    public int streamSplitSize() {
        return streamSplitSize;
    }

    public int objectBlockSize() {
        return objectBlockSize;
    }

    public int objectPartSize() {
        return objectPartSize;
    }

    public Map<String, String> objectTagging() {
        return objectTagging;
    }

    public long blockCacheSize() {
        return blockCacheSize;
    }

    public int streamObjectCompactionIntervalMinutes() {
        return streamObjectCompactionIntervalMinutes;
    }

    public long streamObjectCompactionMaxSizeBytes() {
        return streamObjectCompactionMaxSizeBytes;
    }

    public int controllerRequestRetryMaxCount() {
        return controllerRequestRetryMaxCount;
    }

    public long controllerRequestRetryBaseDelayMs() {
        return controllerRequestRetryBaseDelayMs;
    }

    public long nodeEpoch() {
        return nodeEpoch;
    }

    public int streamSetObjectCompactionInterval() {
        return streamSetObjectCompactionInterval;
    }

    public long streamSetObjectCompactionCacheSize() {
        return streamSetObjectCompactionCacheSize;
    }

    public int streamSetObjectCompactionUploadConcurrency() {
        return streamSetObjectCompactionUploadConcurrency;
    }

    public long streamSetObjectCompactionStreamSplitSize() {
        return streamSetObjectCompactionStreamSplitSize;
    }

    public int streamSetObjectCompactionForceSplitPeriod() {
        return streamSetObjectCompactionForceSplitPeriod;
    }

    public int streamSetObjectCompactionMaxObjectNum() {
        return streamSetObjectCompactionMaxObjectNum;
    }

    public int maxStreamNumPerStreamSetObject() {
        return maxStreamNumPerStreamSetObject;
    }

    public int maxStreamObjectNumPerCommit() {
        return maxStreamObjectNumPerCommit;
    }

    public boolean mockEnable() {
        return mockEnable;
    }

    public boolean objectLogEnable() {
        return objectLogEnable;
    }

    public long networkBaselineBandwidth() {
        return networkBaselineBandwidth;
    }

    public int refillPeriodMs() {
        return refillPeriodMs;
    }

    public Config nodeId(int brokerId) {
        this.nodeId = brokerId;
        return this;
    }

    public Config endpoint(String s3Endpoint) {
        this.endpoint = s3Endpoint;
        return this;
    }

    public Config region(String s3Region) {
        this.region = s3Region;
        return this;
    }

    public Config bucket(String s3Bucket) {
        this.bucket = s3Bucket;
        return this;
    }

    public Config forcePathStyle(boolean s3ForcePathStyle) {
        this.forcePathStyle = s3ForcePathStyle;
        return this;
    }

    public Config walPath(String s3WALPath) {
        this.walPath = s3WALPath;
        return this;
    }

    public Config walCacheSize(long s3WALCacheSize) {
        this.walCacheSize = s3WALCacheSize;
        return this;
    }

    public Config walCapacity(long s3WALCapacity) {
        this.walCapacity = s3WALCapacity;
        return this;
    }

    public Config walInitBufferSize(int walInitBufferSize) {
        this.walInitBufferSize = walInitBufferSize;
        return this;
    }

    public Config walMaxBufferSize(int walMaxBufferSize) {
        this.walMaxBufferSize = walMaxBufferSize;
        return this;
    }

    public Config walThread(int s3WALThread) {
        this.walThread = s3WALThread;
        return this;
    }

    public Config walWindowInitial(long s3WALWindowInitial) {
        this.walWindowInitial = s3WALWindowInitial;
        return this;
    }

    public Config walWindowIncrement(long s3WALWindowIncrement) {
        this.walWindowIncrement = s3WALWindowIncrement;
        return this;
    }

    public Config walWindowMax(long s3WALWindowMax) {
        this.walWindowMax = s3WALWindowMax;
        return this;
    }

    public Config walBlockSoftLimit(long s3WALBlockSoftLimit) {
        this.walBlockSoftLimit = s3WALBlockSoftLimit;
        return this;
    }

    public Config walWriteRateLimit(int s3WALWriteRateLimit) {
        this.walWriteRateLimit = s3WALWriteRateLimit;
        return this;
    }

    public Config walUploadThreshold(long s3WALObjectSize) {
        this.walUploadThreshold = s3WALObjectSize;
        return this;
    }

    public Config streamSplitSize(int s3StreamSplitSize) {
        this.streamSplitSize = s3StreamSplitSize;
        return this;
    }

    public Config objectBlockSize(int s3ObjectBlockSize) {
        this.objectBlockSize = s3ObjectBlockSize;
        return this;
    }

    public Config objectPartSize(int s3ObjectPartSize) {
        this.objectPartSize = s3ObjectPartSize;
        return this;
    }

    public Config objectTagging(Map<String, String> s3ObjectTagging) {
        this.objectTagging = s3ObjectTagging;
        return this;
    }

    public Config blockCacheSize(long s3CacheSize) {
        this.blockCacheSize = s3CacheSize;
        return this;
    }

    public Config streamObjectCompactionIntervalMinutes(int s3StreamObjectCompactionIntervalMinutes) {
        this.streamObjectCompactionIntervalMinutes = s3StreamObjectCompactionIntervalMinutes;
        return this;
    }

    public Config streamObjectCompactionMaxSizeBytes(long s3StreamObjectCompactionMaxSizeBytes) {
        this.streamObjectCompactionMaxSizeBytes = s3StreamObjectCompactionMaxSizeBytes;
        return this;
    }

    public Config controllerRequestRetryMaxCount(int s3ControllerRequestRetryMaxCount) {
        this.controllerRequestRetryMaxCount = s3ControllerRequestRetryMaxCount;
        return this;
    }

    public Config controllerRequestRetryBaseDelayMs(long s3ControllerRequestRetryBaseDelayMs) {
        this.controllerRequestRetryBaseDelayMs = s3ControllerRequestRetryBaseDelayMs;
        return this;
    }

    public Config nodeEpoch(long brokerEpoch) {
        this.nodeEpoch = brokerEpoch;
        return this;
    }

    public Config streamSetObjectCompactionInterval(int streamSetObjectCompactionInterval) {
        this.streamSetObjectCompactionInterval = streamSetObjectCompactionInterval;
        return this;
    }

    public Config streamSetObjectCompactionCacheSize(long streamSetObjectCompactionCacheSize) {
        this.streamSetObjectCompactionCacheSize = streamSetObjectCompactionCacheSize;
        return this;
    }

    public Config streamSetObjectCompactionUploadConcurrency(int streamSetObjectCompactionUploadConcurrency) {
        this.streamSetObjectCompactionUploadConcurrency = streamSetObjectCompactionUploadConcurrency;
        return this;
    }

    public Config streamSetObjectCompactionStreamSplitSize(long streamSetObjectCompactionStreamSplitSize) {
        this.streamSetObjectCompactionStreamSplitSize = streamSetObjectCompactionStreamSplitSize;
        return this;
    }

    public Config streamSetObjectCompactionForceSplitPeriod(int streamSetObjectCompactionForceSplitPeriod) {
        this.streamSetObjectCompactionForceSplitPeriod = streamSetObjectCompactionForceSplitPeriod;
        return this;
    }

    public Config streamSetObjectCompactionMaxObjectNum(int streamSetObjectCompactionMaxObjectNum) {
        this.streamSetObjectCompactionMaxObjectNum = streamSetObjectCompactionMaxObjectNum;
        return this;
    }

    public Config maxStreamNumPerStreamSetObject(int maxStreamNumPerStreamSetObject) {
        this.maxStreamNumPerStreamSetObject = maxStreamNumPerStreamSetObject;
        return this;
    }

    public Config maxStreamObjectNumPerCommit(int maxStreamObjectNumPerCommit) {
        this.maxStreamObjectNumPerCommit = maxStreamObjectNumPerCommit;
        return this;
    }

    public Config mockEnable(boolean s3MockEnable) {
        this.mockEnable = s3MockEnable;
        return this;
    }

    public Config objectLogEnable(boolean s3ObjectLogEnable) {
        this.objectLogEnable = s3ObjectLogEnable;
        return this;
    }

    public Config networkBaselineBandwidth(long networkBaselineBandwidth) {
        this.networkBaselineBandwidth = networkBaselineBandwidth;
        return this;
    }

    public Config refillPeriodMs(int refillPeriodMs) {
        this.refillPeriodMs = refillPeriodMs;
        return this;
    }

    public Config objectRetentionTimeInSecond(long seconds) {
        objectRetentionTimeInSecond = seconds;
        return this;
    }

    public long objectRetentionTimeInSecond() {
        return objectRetentionTimeInSecond;
    }

    public Config failoverEnable(boolean failoverEnable) {
        this.failoverEnable = failoverEnable;
        return this;
    }

    public boolean failoverEnable() {
        return failoverEnable;
    }

    public Config version(Supplier<Version> version) {
        this.version = version;
        return this;
    }

    public Version version() {
        return version.get();
    }
}
