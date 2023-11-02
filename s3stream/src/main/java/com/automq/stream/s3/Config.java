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
    private String s3Endpoint;
    private String s3Region;
    private String s3Bucket;
    private boolean s3ForcePathStyle = false;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3WALPath = "/tmp/s3stream_wal";
    private long s3WALCacheSize = 200 * 1024 * 1024;
    private long s3WALCapacity = 1024L * 1024 * 1024;
    private int s3WALHeaderFlushIntervalSeconds = 10;
    private int s3WALThread = 8;
    private long s3WALWindowInitial = 1048576L;
    private long s3WALWindowIncrement = 4194304L;
    private long s3WALWindowMax = 536870912L;
    private long s3WALBlockSoftLimit = 128 * 1024;
    private long s3WALObjectSize = 100 * 1024 * 1024;
    private int s3StreamSplitSize = 16777216;
    private int s3ObjectBlockSize = 8388608;
    private int s3ObjectPartSize = 16777216;
    private long s3BlockCacheSize = 100 * 1024 * 1024;
    private int s3StreamObjectCompactionIntervalMinutes = 60;
    private long s3StreamObjectCompactionMaxSizeBytes = 10737418240L;
    private int s3StreamObjectCompactionLivingTimeMinutes = 60;
    private int s3ControllerRequestRetryMaxCount = 5;
    private long s3ControllerRequestRetryBaseDelayMs = 500;
    private long brokerEpoch = 0L;
    private int s3WALObjectCompactionInterval = 20;
    private long s3WALObjectCompactionCacheSize = 200 * 1024 * 1024;
    private int s3WALObjectCompactionUploadConcurrency = 8;
    private long s3WALObjectCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int s3WALObjectCompactionForceSplitPeriod = 120;
    private int s3WALObjectCompactionMaxObjectNum = 500;
    private int s3MaxStreamNumPerWALObject = 10000;
    private int s3MaxStreamObjectNumPerCommit = 10000;
    private boolean s3MockEnable = false;
    private boolean s3ObjectLogEnable = false;
    // 100MB/s
    private long networkBaselineBandwidth = 100 * 1024 * 1024;
    private int refillPeriodMs = 1000;

    public int brokerId() {
        return brokerId;
    }

    public String s3Endpoint() {
        return s3Endpoint;
    }

    public String s3Region() {
        return s3Region;
    }

    public String s3Bucket() {
        return s3Bucket;
    }

    public boolean s3ForcePathStyle() {
        return s3ForcePathStyle;
    }

    public String s3WALPath() {
        return s3WALPath;
    }

    public long s3WALCacheSize() {
        return s3WALCacheSize;
    }

    public long s3WALCapacity() {
        return s3WALCapacity;
    }

    public int s3WALHeaderFlushIntervalSeconds() {
        return s3WALHeaderFlushIntervalSeconds;
    }

    public int s3WALThread() {
        return s3WALThread;
    }

    public long s3WALWindowInitial() {
        return s3WALWindowInitial;
    }

    public long s3WALWindowIncrement() {
        return s3WALWindowIncrement;
    }

    public long s3WALWindowMax() {
        return s3WALWindowMax;
    }

    public long s3WALBlockSoftLimit() {
        return s3WALBlockSoftLimit;
    }

    public long s3WALObjectSize() {
        return s3WALObjectSize;
    }

    public int s3StreamSplitSize() {
        return s3StreamSplitSize;
    }

    public int s3ObjectBlockSize() {
        return s3ObjectBlockSize;
    }

    public int s3ObjectPartSize() {
        return s3ObjectPartSize;
    }

    public long s3BlockCacheSize() {
        return s3BlockCacheSize;
    }

    public int s3StreamObjectCompactionIntervalMinutes() {
        return s3StreamObjectCompactionIntervalMinutes;
    }

    public long s3StreamObjectCompactionMaxSizeBytes() {
        return s3StreamObjectCompactionMaxSizeBytes;
    }

    public int s3StreamObjectCompactionLivingTimeMinutes() {
        return s3StreamObjectCompactionLivingTimeMinutes;
    }

    public int s3ControllerRequestRetryMaxCount() {
        return s3ControllerRequestRetryMaxCount;
    }

    public long s3ControllerRequestRetryBaseDelayMs() {
        return s3ControllerRequestRetryBaseDelayMs;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public int s3WALObjectCompactionInterval() {
        return s3WALObjectCompactionInterval;
    }

    public long s3WALObjectCompactionCacheSize() {
        return s3WALObjectCompactionCacheSize;
    }

    public int s3WALObjectCompactionUploadConcurrency() {
        return s3WALObjectCompactionUploadConcurrency;
    }

    public long s3WALObjectCompactionStreamSplitSize() {
        return s3WALObjectCompactionStreamSplitSize;
    }

    public int s3WALObjectCompactionForceSplitPeriod() {
        return s3WALObjectCompactionForceSplitPeriod;
    }

    public int s3WALObjectCompactionMaxObjectNum() {
        return s3WALObjectCompactionMaxObjectNum;
    }

    public int s3MaxStreamNumPerWALObject() {
        return s3MaxStreamNumPerWALObject;
    }

    public int s3MaxStreamObjectNumPerCommit() {
        return s3MaxStreamObjectNumPerCommit;
    }

    public boolean s3MockEnable() {
        return s3MockEnable;
    }

    public boolean s3ObjectLogEnable() {
        return s3ObjectLogEnable;
    }

    public String s3AccessKey() {
        return s3AccessKey;
    }

    public String s3SecretKey() {
        return s3SecretKey;
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

    public Config s3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
        return this;
    }

    public Config s3Region(String s3Region) {
        this.s3Region = s3Region;
        return this;
    }

    public Config s3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
        return this;
    }

    public Config s3ForcePathStyle(boolean s3ForcePathStyle) {
        this.s3ForcePathStyle = s3ForcePathStyle;
        return this;
    }

    public Config s3WALPath(String s3WALPath) {
        this.s3WALPath = s3WALPath;
        return this;
    }

    public Config s3WALCacheSize(long s3WALCacheSize) {
        this.s3WALCacheSize = s3WALCacheSize;
        return this;
    }

    public Config s3WALCapacity(long s3WALCapacity) {
        this.s3WALCapacity = s3WALCapacity;
        return this;
    }

    public Config s3WALHeaderFlushIntervalSeconds(int s3WALHeaderFlushIntervalSeconds) {
        this.s3WALHeaderFlushIntervalSeconds = s3WALHeaderFlushIntervalSeconds;
        return this;
    }

    public Config s3WALThread(int s3WALThread) {
        this.s3WALThread = s3WALThread;
        return this;
    }

    public Config s3WALWindowInitial(long s3WALWindowInitial) {
        this.s3WALWindowInitial = s3WALWindowInitial;
        return this;
    }

    public Config s3WALWindowIncrement(long s3WALWindowIncrement) {
        this.s3WALWindowIncrement = s3WALWindowIncrement;
        return this;
    }

    public Config s3WALWindowMax(long s3WALWindowMax) {
        this.s3WALWindowMax = s3WALWindowMax;
        return this;
    }

    public Config s3WALBlockSoftLimit(long s3WALBlockSoftLimit) {
        this.s3WALBlockSoftLimit = s3WALBlockSoftLimit;
        return this;
    }

    public Config s3WALObjectSize(long s3WALObjectSize) {
        this.s3WALObjectSize = s3WALObjectSize;
        return this;
    }

    public Config s3StreamSplitSize(int s3StreamSplitSize) {
        this.s3StreamSplitSize = s3StreamSplitSize;
        return this;
    }

    public Config s3ObjectBlockSize(int s3ObjectBlockSize) {
        this.s3ObjectBlockSize = s3ObjectBlockSize;
        return this;
    }

    public Config s3ObjectPartSize(int s3ObjectPartSize) {
        this.s3ObjectPartSize = s3ObjectPartSize;
        return this;
    }

    public Config s3BlockCacheSize(long s3CacheSize) {
        this.s3BlockCacheSize = s3CacheSize;
        return this;
    }

    public Config s3StreamObjectCompactionIntervalMinutes(int s3StreamObjectCompactionIntervalMinutes) {
        this.s3StreamObjectCompactionIntervalMinutes = s3StreamObjectCompactionIntervalMinutes;
        return this;
    }

    public Config s3StreamObjectCompactionMaxSizeBytes(long s3StreamObjectCompactionMaxSizeBytes) {
        this.s3StreamObjectCompactionMaxSizeBytes = s3StreamObjectCompactionMaxSizeBytes;
        return this;
    }

    public Config s3StreamObjectCompactionLivingTimeMinutes(int s3StreamObjectCompactionLivingTimeMinutes) {
        this.s3StreamObjectCompactionLivingTimeMinutes = s3StreamObjectCompactionLivingTimeMinutes;
        return this;
    }

    public Config s3ControllerRequestRetryMaxCount(int s3ControllerRequestRetryMaxCount) {
        this.s3ControllerRequestRetryMaxCount = s3ControllerRequestRetryMaxCount;
        return this;
    }

    public Config s3ControllerRequestRetryBaseDelayMs(long s3ControllerRequestRetryBaseDelayMs) {
        this.s3ControllerRequestRetryBaseDelayMs = s3ControllerRequestRetryBaseDelayMs;
        return this;
    }

    public Config brokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
        return this;
    }

    public Config s3WALObjectCompactionInterval(int s3WALObjectCompactionInterval) {
        this.s3WALObjectCompactionInterval = s3WALObjectCompactionInterval;
        return this;
    }

    public Config s3WALObjectCompactionCacheSize(long s3WALObjectCompactionCacheSize) {
        this.s3WALObjectCompactionCacheSize = s3WALObjectCompactionCacheSize;
        return this;
    }

    public Config s3WALObjectCompactionUploadConcurrency(int s3WALObjectCompactionUploadConcurrency) {
        this.s3WALObjectCompactionUploadConcurrency = s3WALObjectCompactionUploadConcurrency;
        return this;
    }

    public Config s3WALObjectCompactionStreamSplitSize(long s3WALObjectCompactionStreamSplitSize) {
        this.s3WALObjectCompactionStreamSplitSize = s3WALObjectCompactionStreamSplitSize;
        return this;
    }

    public Config s3WALObjectCompactionForceSplitPeriod(int s3WALObjectCompactionForceSplitPeriod) {
        this.s3WALObjectCompactionForceSplitPeriod = s3WALObjectCompactionForceSplitPeriod;
        return this;
    }

    public Config s3WALObjectCompactionMaxObjectNum(int s3WALObjectCompactionMaxObjectNum) {
        this.s3WALObjectCompactionMaxObjectNum = s3WALObjectCompactionMaxObjectNum;
        return this;
    }

    public Config s3MaxStreamNumPerWALObject(int s3MaxStreamNumPerWALObject) {
        this.s3MaxStreamNumPerWALObject = s3MaxStreamNumPerWALObject;
        return this;
    }

    public Config s3MaxStreamObjectNumPerCommit(int s3MaxStreamObjectNumPerCommit) {
        this.s3MaxStreamObjectNumPerCommit = s3MaxStreamObjectNumPerCommit;
        return this;
    }

    public Config s3MockEnable(boolean s3MockEnable) {
        this.s3MockEnable = s3MockEnable;
        return this;
    }

    public Config s3ObjectLogEnable(boolean s3ObjectLogEnable) {
        this.s3ObjectLogEnable = s3ObjectLogEnable;
        return this;
    }

    public Config s3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
        return this;
    }

    public Config s3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
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
