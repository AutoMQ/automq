/*
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

package com.automq.stream.s3;

// TODO: rename & init
public class Config {
    private int brokerId;
    private String endpoint;
    private String region;
    private String bucket;
    private boolean forcePathStyle = false;
    private String accessKey;
    private String secretKey;
    private String walPath = "/tmp/s3stream_wal";
    private long walCacheSize = 200 * 1024 * 1024;
    private long walCapacity = 1024L * 1024 * 1024;
    private int walHeaderFlushIntervalSeconds = 10;
    private int walThread = 8;
    private long walWindowInitial = 1048576L;
    private long walWindowIncrement = 4194304L;
    private long walWindowMax = 536870912L;
    private long walBlockSoftLimit = 128 * 1024;
    private long walUploadThreshold = 100 * 1024 * 1024;
    private int streamSplitSize = 16777216;
    private int objectBlockSize = 8388608;
    private int objectPartSize = 16777216;
    private long blockCacheSize = 100 * 1024 * 1024;
    private int streamObjectCompactionIntervalMinutes = 60;
    private long streamObjectCompactionMaxSizeBytes = 10737418240L;
    private int streamObjectCompactionLivingTimeMinutes = 60;
    private int controllerRequestRetryMaxCount = Integer.MAX_VALUE;
    private long controllerRequestRetryBaseDelayMs = 500;
    private long brokerEpoch = 0L;
    private int sstCompactionInterval = 20;
    private long sstCompactionCacheSize = 200 * 1024 * 1024;
    private int sstCompactionUploadConcurrency = 8;
    private long sstCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int sstCompactionForceSplitPeriod = 120;
    private int sstCompactionMaxObjectNum = 500;
    private int maxStreamNumPerSST = 10000;
    private int maxStreamObjectNumPerCommit = 10000;
    private boolean mockEnable = false;
    private boolean objectLogEnable = false;
    // 100MB/s
    private long networkBaselineBandwidth = 100 * 1024 * 1024;
    private int refillPeriodMs = 1000;

    public int brokerId() {
        return brokerId;
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

    public int walHeaderFlushIntervalSeconds() {
        return walHeaderFlushIntervalSeconds;
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

    public long blockCacheSize() {
        return blockCacheSize;
    }

    public int streamObjectCompactionIntervalMinutes() {
        return streamObjectCompactionIntervalMinutes;
    }

    public long streamObjectCompactionMaxSizeBytes() {
        return streamObjectCompactionMaxSizeBytes;
    }

    public int streamObjectCompactionLivingTimeMinutes() {
        return streamObjectCompactionLivingTimeMinutes;
    }

    public int controllerRequestRetryMaxCount() {
        return controllerRequestRetryMaxCount;
    }

    public long controllerRequestRetryBaseDelayMs() {
        return controllerRequestRetryBaseDelayMs;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public int sstCompactionInterval() {
        return sstCompactionInterval;
    }

    public long sstCompactionCacheSize() {
        return sstCompactionCacheSize;
    }

    public int sstCompactionUploadConcurrency() {
        return sstCompactionUploadConcurrency;
    }

    public long sstCompactionStreamSplitSize() {
        return sstCompactionStreamSplitSize;
    }

    public int sstCompactionForceSplitPeriod() {
        return sstCompactionForceSplitPeriod;
    }

    public int sstCompactionMaxObjectNum() {
        return sstCompactionMaxObjectNum;
    }

    public int maxStreamNumPerSST() {
        return maxStreamNumPerSST;
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

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }

    public long networkBaselineBandwidth() {
        return networkBaselineBandwidth;
    }

    public int refillPeriodMs() {
        return refillPeriodMs;
    }

    public Config brokerId(int brokerId) {
        this.brokerId = brokerId;
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

    public Config walHeaderFlushIntervalSeconds(int s3WALHeaderFlushIntervalSeconds) {
        this.walHeaderFlushIntervalSeconds = s3WALHeaderFlushIntervalSeconds;
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

    public Config streamObjectCompactionLivingTimeMinutes(int s3StreamObjectCompactionLivingTimeMinutes) {
        this.streamObjectCompactionLivingTimeMinutes = s3StreamObjectCompactionLivingTimeMinutes;
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

    public Config brokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
        return this;
    }

    public Config sstCompactionInterval(int s3WALObjectCompactionInterval) {
        this.sstCompactionInterval = s3WALObjectCompactionInterval;
        return this;
    }

    public Config sstCompactionCacheSize(long sstCompactionCacheSize) {
        this.sstCompactionCacheSize = sstCompactionCacheSize;
        return this;
    }

    public Config sstCompactionUploadConcurrency(int sstCompactionUploadConcurrency) {
        this.sstCompactionUploadConcurrency = sstCompactionUploadConcurrency;
        return this;
    }

    public Config sstCompactionStreamSplitSize(long sstCompactionStreamSplitSize) {
        this.sstCompactionStreamSplitSize = sstCompactionStreamSplitSize;
        return this;
    }

    public Config sstCompactionForceSplitPeriod(int sstCompactionForceSplitPeriod) {
        this.sstCompactionForceSplitPeriod = sstCompactionForceSplitPeriod;
        return this;
    }

    public Config sstCompactionMaxObjectNum(int sstCompactionMaxObjectNum) {
        this.sstCompactionMaxObjectNum = sstCompactionMaxObjectNum;
        return this;
    }

    public Config maxStreamNumPerSST(int maxStreamNumPerSST) {
        this.maxStreamNumPerSST = maxStreamNumPerSST;
        return this;
    }

    public Config maxStreamObjectNumPerCommit(int s3MaxStreamObjectNumPerCommit) {
        this.maxStreamObjectNumPerCommit = s3MaxStreamObjectNumPerCommit;
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

    public Config accessKey(String s3AccessKey) {
        this.accessKey = s3AccessKey;
        return this;
    }

    public Config secretKey(String s3SecretKey) {
        this.secretKey = s3SecretKey;
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
}
