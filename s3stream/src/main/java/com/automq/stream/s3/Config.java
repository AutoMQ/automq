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
    private String s3WALPath = "/tmp/s3stream_wal";
    private long s3WALCacheSize = 200 * 1024 * 1024;
    private long s3WALCapacity = 1024L * 1024 * 1024;
    private int s3WALHeaderFlushIntervalSeconds = 10;
    private int s3WALThread = 8;
    private int s3WALQueue = 10000;
    private long s3WALWindowInitial = 1048576L;
    private long s3WALWindowIncrement = 4194304L;
    private long s3WALWindowMax = 536870912L;
    private long s3WALObjectSize = 100 * 1024 * 1024;
    private int s3StreamSplitSize = 16777216;
    private int s3ObjectBlockSize = 8388608;
    private int s3ObjectPartSize = 16777216;
    private long s3CacheSize = 100 * 1024 * 1024;
    private int s3StreamObjectCompactionTaskIntervalMinutes = 60;
    private long s3StreamObjectCompactionMaxSizeBytes = 10737418240L;
    private int s3StreamObjectCompactionLivingTimeMinutes = 60;
    private int s3ControllerRequestRetryMaxCount = 5;
    private long s3ControllerRequestRetryBaseDelayMs = 500;
    private long brokerEpoch = 0L;
    private int s3ObjectCompactionInterval = 20;
    private long s3ObjectCompactionCacheSize = 200 * 1024 * 1024;
    private long s3ObjectCompactionNWInBandwidth = 50 * 1024 * 1024;
    private long s3ObjectCompactionNWOutBandwidth= 50 * 1024 * 1024;
    private int s3ObjectCompactionUploadConcurrency = 8;
    private double s3ObjectCompactionExecutionScoreThreshold = 0.5;
    private long s3ObjectCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int s3ObjectCompactionForceSplitPeriod = 120;
    private int s3ObjectCompactionMaxObjectNum = 500;
    private boolean s3MockEnable = false;

    public int brokerId() {
        return brokerId;
    }

    public String s3Endpont() {
        return s3Endpoint;
    }

    public String s3Region() {
        return s3Region;
    }

    public String s3Bucket() {
        return s3Bucket;
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

    public int s3WALQueue() {
        return s3WALQueue;
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

    public long s3CacheSize() {
        return s3CacheSize;
    }

    public int s3StreamObjectCompactionTaskIntervalMinutes() {
        return s3StreamObjectCompactionTaskIntervalMinutes;
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

    public int s3ObjectCompactionInterval() {
        return s3ObjectCompactionInterval;
    }

    public long s3ObjectCompactionCacheSize() {
        return s3ObjectCompactionCacheSize;
    }

    public long s3ObjectCompactionNWInBandwidth() {
        return s3ObjectCompactionNWInBandwidth;
    }

    public long s3ObjectCompactionNWOutBandwidth() {
        return s3ObjectCompactionNWOutBandwidth;
    }

    public int s3ObjectCompactionUploadConcurrency() {
        return s3ObjectCompactionUploadConcurrency;
    }

    public double s3ObjectCompactionExecutionScoreThreshold() {
        return s3ObjectCompactionExecutionScoreThreshold;
    }

    public long s3ObjectCompactionStreamSplitSize() {
        return s3ObjectCompactionStreamSplitSize;
    }

    public int s3ObjectCompactionForceSplitPeriod() {
        return s3ObjectCompactionForceSplitPeriod;
    }

    public int s3ObjectCompactionMaxObjectNum() {
        return s3ObjectCompactionMaxObjectNum;
    }

    public boolean s3MockEnable() {
        return s3MockEnable;
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

    public Config s3WALQueue(int s3WALQueue) {
        this.s3WALQueue = s3WALQueue;
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

    public Config s3CacheSize(long s3CacheSize) {
        this.s3CacheSize = s3CacheSize;
        return this;
    }

    public Config s3StreamObjectCompactionTaskIntervalMinutes(int s3StreamObjectCompactionTaskIntervalMinutes) {
        this.s3StreamObjectCompactionTaskIntervalMinutes = s3StreamObjectCompactionTaskIntervalMinutes;
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

    public Config s3ObjectCompactionInterval(int s3ObjectCompactionInterval) {
        this.s3ObjectCompactionInterval = s3ObjectCompactionInterval;
        return this;
    }

    public Config s3ObjectCompactionCacheSize(long s3ObjectCompactionCacheSize) {
        this.s3ObjectCompactionCacheSize = s3ObjectCompactionCacheSize;
        return this;
    }

    public Config s3ObjectCompactionNWInBandwidth(long s3ObjectCompactionNWInBandwidth) {
        this.s3ObjectCompactionNWInBandwidth = s3ObjectCompactionNWInBandwidth;
        return this;
    }

    public Config s3ObjectCompactionNWOutBandwidth(long s3ObjectCompactionNWOutBandwidth) {
        this.s3ObjectCompactionNWOutBandwidth = s3ObjectCompactionNWOutBandwidth;
        return this;
    }

    public Config s3ObjectCompactionUploadConcurrency(int s3ObjectCompactionUploadConcurrency) {
        this.s3ObjectCompactionUploadConcurrency = s3ObjectCompactionUploadConcurrency;
        return this;
    }

    public Config s3ObjectCompactionExecutionScoreThreshold(double s3ObjectCompactionExecutionScoreThreshold) {
        this.s3ObjectCompactionExecutionScoreThreshold = s3ObjectCompactionExecutionScoreThreshold;
        return this;
    }

    public Config s3ObjectCompactionStreamSplitSize(long s3ObjectCompactionStreamSplitSize) {
        this.s3ObjectCompactionStreamSplitSize = s3ObjectCompactionStreamSplitSize;
        return this;
    }

    public Config s3ObjectCompactionForceSplitPeriod(int s3ObjectCompactionForceSplitPeriod) {
        this.s3ObjectCompactionForceSplitPeriod = s3ObjectCompactionForceSplitPeriod;
        return this;
    }

    public Config s3ObjectCompactionMaxObjectNum(int s3ObjectCompactionMaxObjectNum) {
        this.s3ObjectCompactionMaxObjectNum = s3ObjectCompactionMaxObjectNum;
        return this;
    }

    public Config s3MockEnable(boolean s3MockEnable) {
        this.s3MockEnable = s3MockEnable;
        return this;
    }
}
