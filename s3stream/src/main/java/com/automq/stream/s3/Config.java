package com.automq.stream.s3;

import com.automq.stream.Version;
import com.automq.stream.s3.operator.BucketURI;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

// TODO: rename & init
public class Config {

    private int nodeId;
    private List<BucketURI> dataBuckets;
    private String walConfig = "0@file:///tmp/s3stream_wal";
    private long walCacheSize = 200 * 1024 * 1024;
    private long walUploadThreshold = 100 * 1024 * 1024;
    private long walUploadIntervalMs = -1L; // -1 means disabled
    private int streamSplitSize = 16777216;
    private int objectBlockSize = 1048576;
    private int objectPartSize = 16777216;
    private Map<String, String> objectTagging;
    private long blockCacheSize = 100 * 1024 * 1024;
    private int streamObjectCompactionIntervalMinutes = 60;
    private long streamObjectCompactionMaxSizeBytes = 10737418240L;
    private int controllerRequestRetryMaxCount = Integer.MAX_VALUE;
    private long controllerRequestRetryBaseDelayMs = 500;
    private long nodeEpoch = 0L;
    private int streamSetObjectCompactionInterval = 5;
    private long streamSetObjectCompactionCacheSize = 200 * 1024 * 1024;
    private int streamSetObjectCompactionUploadConcurrency = 8;
    private long streamSetObjectCompactionStreamSplitSize = 8 * 1024 * 1024;
    private int streamSetObjectCompactionForceSplitPeriod = 120;
    private int streamSetObjectCompactionMaxObjectNum = 500;
    private int maxStreamNumPerStreamSetObject = 20000;
    private int maxStreamObjectNumPerCommit = 10000;
    private boolean mockEnable = false;
    private long networkBaselineBandwidth = 1024 * 1024 * 1024;
    private int refillPeriodMs = 10;
    private long objectRetentionTimeInSecond = 10 * 60;
    private boolean failoverEnable = false;
    private boolean snapshotReadEnable = false;

    private Supplier<Version> version = () -> {
        throw new UnsupportedOperationException("Version supplier is not configured");
    };

    /* ======================
       Getter methods
       ====================== */

    public int nodeId() {
        return nodeId;
    }

    public List<BucketURI> dataBuckets() {
        return dataBuckets;
    }

    public String walConfig() {
        return walConfig;
    }

    public long walCacheSize() {
        return walCacheSize;
    }

    public long walUploadThreshold() {
        return walUploadThreshold;
    }

    public long walUploadIntervalMs() {
        return walUploadIntervalMs;
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

    public int refillPeriodMs() {
        return refillPeriodMs;
    }

    public long objectRetentionTimeInSecond() {
        return objectRetentionTimeInSecond;
    }

    public boolean failoverEnable() {
        return failoverEnable;
    }

    public boolean snapshotReadEnable() {
        return snapshotReadEnable;
    }

    public Version version() {
        return version.get();
    }

    /* ======================
       Builder-style setters
       ====================== */

    public Config nodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public Config dataBuckets(List<BucketURI> dataBuckets) {
        this.dataBuckets = dataBuckets;
        return this;
    }

    public Config walConfig(String walConfig) {
        this.walConfig = walConfig;
        return this;
    }

    public Config walCacheSize(long walCacheSize) {
        this.walCacheSize = walCacheSize;
        return this;
    }

    public Config walUploadThreshold(long walUploadThreshold) {
        this.walUploadThreshold = walUploadThreshold;
        return this;
    }

    public Config refillPeriodMs(int refillPeriodMs) {
        this.refillPeriodMs = refillPeriodMs;
        return this;
    }

    public Config failoverEnable(boolean failoverEnable) {
        this.failoverEnable = failoverEnable;
        return this;
    }

    public Config snapshotReadEnable(boolean snapshotReadEnable) {
        this.snapshotReadEnable = snapshotReadEnable;
        return this;
    }

    public Config version(Supplier<Version> version) {
        this.version = version;
        return this;
    }

    /* ======================
       ✅ VALIDATION METHOD (NEW)
       ====================== */

    /**
     * Validate required configuration fields.
     * This method is not invoked automatically to avoid changing existing behavior.
     */
    public void validate() {

        if (nodeId <= 0) {
            throw new IllegalArgumentException(
                "Invalid configuration: nodeId must be greater than 0"
            );
        }

        if (dataBuckets == null || dataBuckets.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid configuration: dataBuckets must not be null or empty"
            );
        }

        if (walCacheSize <= 0) {
            throw new IllegalArgumentException(
                "Invalid configuration: walCacheSize must be greater than 0"
            );
        }

        if (walUploadThreshold <= 0) {
            throw new IllegalArgumentException(
                "Invalid configuration: walUploadThreshold must be greater than 0"
            );
        }
    }
}
