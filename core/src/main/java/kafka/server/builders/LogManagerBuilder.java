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

package kafka.server.builders;

import kafka.api.ApiVersion;
import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.BrokerTopicStats;
import kafka.server.LogDirFailureChannel;
import kafka.server.metadata.ConfigRepository;
import kafka.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import scala.collection.JavaConverters;

import java.io.File;
import java.util.Collections;
import java.util.List;


public class LogManagerBuilder {
    private List<File> logDirs = null;
    private List<File> initialOfflineDirs = Collections.emptyList();
    private ConfigRepository configRepository = null;
    private LogConfig initialDefaultConfig = null;
    private CleanerConfig cleanerConfig = null;
    private int recoveryThreadsPerDataDir = 1;
    private long flushCheckMs = 1000L;
    private long flushRecoveryOffsetCheckpointMs = 10000L;
    private long flushStartOffsetCheckpointMs = 10000L;
    private long retentionCheckMs = 1000L;
    private int maxTransactionTimeoutMs = 15 * 60 * 1000;
    private int maxPidExpirationMs = 60000;
    private ApiVersion interBrokerProtocolVersion = ApiVersion.latestVersion();
    private Scheduler scheduler = null;
    private BrokerTopicStats brokerTopicStats = null;
    private LogDirFailureChannel logDirFailureChannel = null;
    private Time time = Time.SYSTEM;
    private boolean keepPartitionMetadataFile = true;

    public LogManagerBuilder setLogDirs(List<File> logDirs) {
        this.logDirs = logDirs;
        return this;
    }

    public LogManagerBuilder setInitialOfflineDirs(List<File> initialOfflineDirs) {
        this.initialOfflineDirs = initialOfflineDirs;
        return this;
    }

    public LogManagerBuilder setConfigRepository(ConfigRepository configRepository) {
        this.configRepository = configRepository;
        return this;
    }

    public LogManagerBuilder setInitialDefaultConfig(LogConfig initialDefaultConfig) {
        this.initialDefaultConfig = initialDefaultConfig;
        return this;
    }

    public LogManagerBuilder setCleanerConfig(CleanerConfig cleanerConfig) {
        this.cleanerConfig = cleanerConfig;
        return this;
    }

    public LogManagerBuilder setRecoveryThreadsPerDataDir(int recoveryThreadsPerDataDir) {
        this.recoveryThreadsPerDataDir = recoveryThreadsPerDataDir;
        return this;
    }

    public LogManagerBuilder setFlushCheckMs(long flushCheckMs) {
        this.flushCheckMs = flushCheckMs;
        return this;
    }

    public LogManagerBuilder setFlushRecoveryOffsetCheckpointMs(long flushRecoveryOffsetCheckpointMs) {
        this.flushRecoveryOffsetCheckpointMs = flushRecoveryOffsetCheckpointMs;
        return this;
    }

    public LogManagerBuilder setFlushStartOffsetCheckpointMs(long flushStartOffsetCheckpointMs) {
        this.flushStartOffsetCheckpointMs = flushStartOffsetCheckpointMs;
        return this;
    }

    public LogManagerBuilder setRetentionCheckMs(long retentionCheckMs) {
        this.retentionCheckMs = retentionCheckMs;
        return this;
    }

    public LogManagerBuilder setMaxTransactionTimeoutMs(int maxTransactionTimeoutMs) {
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        return this;
    }

    public LogManagerBuilder setMaxPidExpirationMs(int maxPidExpirationMs) {
        this.maxPidExpirationMs = maxPidExpirationMs;
        return this;
    }

    public LogManagerBuilder setInterBrokerProtocolVersion(ApiVersion interBrokerProtocolVersion) {
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
        return this;
    }

    public LogManagerBuilder setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public LogManagerBuilder setBrokerTopicStats(BrokerTopicStats brokerTopicStats) {
        this.brokerTopicStats = brokerTopicStats;
        return this;
    }

    public LogManagerBuilder setLogDirFailureChannel(LogDirFailureChannel logDirFailureChannel) {
        this.logDirFailureChannel = logDirFailureChannel;
        return this;
    }

    public LogManagerBuilder setTime(Time time) {
        this.time = time;
        return this;
    }

    public LogManagerBuilder setKeepPartitionMetadataFile(boolean keepPartitionMetadataFile) {
        this.keepPartitionMetadataFile = keepPartitionMetadataFile;
        return this;
    }

    public LogManager build() {
        if (logDirs == null) throw new RuntimeException("you must set logDirs");
        if (configRepository == null) throw new RuntimeException("you must set configRepository");
        if (initialDefaultConfig == null) throw new RuntimeException("you must set initialDefaultConfig");
        if (cleanerConfig == null) throw new RuntimeException("you must set cleanerConfig");
        if (scheduler == null) throw new RuntimeException("you must set scheduler");
        if (brokerTopicStats == null) throw new RuntimeException("you must set brokerTopicStats");
        if (logDirFailureChannel == null) throw new RuntimeException("you must set logDirFailureChannel");

        return new LogManager(JavaConverters.asScalaIteratorConverter(logDirs.iterator()).asScala().toSeq(),
                              JavaConverters.asScalaIteratorConverter(initialOfflineDirs.iterator()).asScala().toSeq(),
                              configRepository,
                              initialDefaultConfig,
                              cleanerConfig,
                              recoveryThreadsPerDataDir,
                              flushCheckMs,
                              flushRecoveryOffsetCheckpointMs,
                              flushStartOffsetCheckpointMs,
                              retentionCheckMs,
                              maxTransactionTimeoutMs,
                              maxPidExpirationMs,
                              interBrokerProtocolVersion,
                              scheduler,
                              brokerTopicStats,
                              logDirFailureChannel,
                              time,
                              keepPartitionMetadataFile);
    }
}
