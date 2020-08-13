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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.rocksdb.Cache;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBMetricsRecorder {

    private static class DbAndCacheAndStatistics {
        public final RocksDB db;
        public final Cache cache;
        public final Statistics statistics;

        public DbAndCacheAndStatistics(final RocksDB db, final Cache cache, final Statistics statistics) {
            Objects.requireNonNull(db, "database instance must not be null");
            this.db = db;
            this.cache = cache;
            if (statistics != null) {
                statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
            }
            this.statistics = statistics;
        }

        public void maybeCloseStatistics() {
            if (statistics != null) {
                statistics.close();
            }
        }
    }

    private final Logger logger;

    private Sensor bytesWrittenToDatabaseSensor;
    private Sensor bytesReadFromDatabaseSensor;
    private Sensor memtableBytesFlushedSensor;
    private Sensor memtableHitRatioSensor;
    private Sensor writeStallDurationSensor;
    private Sensor blockCacheDataHitRatioSensor;
    private Sensor blockCacheIndexHitRatioSensor;
    private Sensor blockCacheFilterHitRatioSensor;
    private Sensor bytesReadDuringCompactionSensor;
    private Sensor bytesWrittenDuringCompactionSensor;
    private Sensor numberOfOpenFilesSensor;
    private Sensor numberOfFileErrorsSensor;

    private final Map<String, DbAndCacheAndStatistics> storeToValueProviders = new ConcurrentHashMap<>();
    private final String metricsScope;
    private final String storeName;
    private final String threadId;
    private TaskId taskId;
    private StreamsMetricsImpl streamsMetrics;

    public RocksDBMetricsRecorder(final String metricsScope,
                                  final String threadId,
                                  final String storeName) {
        this.metricsScope = metricsScope;
        this.threadId = threadId;
        this.storeName = storeName;
        final LogContext logContext = new LogContext(String.format("[RocksDB Metrics Recorder for %s] ", storeName));
        logger = logContext.logger(RocksDBMetricsRecorder.class);
    }

    public String storeName() {
        return storeName;
    }

    public TaskId taskId() {
        return taskId;
    }

    /**
     * The initialisation of the metrics recorder is idempotent.
     */
    public void init(final StreamsMetricsImpl streamsMetrics,
                     final TaskId taskId) {
        Objects.requireNonNull(streamsMetrics, "Streams metrics must not be null");
        Objects.requireNonNull(streamsMetrics, "task ID must not be null");
        if (this.taskId != null && !this.taskId.equals(taskId)) {
            throw new IllegalStateException("Metrics recorder is re-initialised with different task: previous task is " +
                this.taskId + " whereas current task is " + taskId + ". This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        if (this.streamsMetrics != null && this.streamsMetrics != streamsMetrics) {
            throw new IllegalStateException("Metrics recorder is re-initialised with different Streams metrics. "
                + "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        initSensors(streamsMetrics, taskId);
        this.taskId = taskId;
        this.streamsMetrics = streamsMetrics;
    }

    public void addValueProviders(final String segmentName,
                                  final RocksDB db,
                                  final Cache cache,
                                  final Statistics statistics) {
        if (storeToValueProviders.isEmpty()) {
            logger.debug("Adding metrics recorder of task {} to metrics recording trigger", taskId);
            streamsMetrics.rocksDBMetricsRecordingTrigger().addMetricsRecorder(this);
        } else if (storeToValueProviders.containsKey(segmentName)) {
            throw new IllegalStateException("Value providers for store \"" + segmentName + "\" of task " + taskId +
                " has been already added. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        verifyStatistics(segmentName, statistics);
        logger.debug("Adding value providers for store {} of task {}", segmentName, taskId);
        storeToValueProviders.put(segmentName, new DbAndCacheAndStatistics(db, cache, statistics));
    }

    private void verifyStatistics(final String segmentName, final Statistics statistics) {
        if (!storeToValueProviders.isEmpty() && (
                statistics == null &&
                storeToValueProviders.values().stream().anyMatch(valueProviders -> valueProviders.statistics != null)
                ||
                statistics != null &&
                storeToValueProviders.values().stream().anyMatch(valueProviders -> valueProviders.statistics == null))) {

            throw new IllegalStateException("Statistics for store \"" + segmentName + "\" of task " + taskId +
                " is" + (statistics == null ? " " : " not ") + "null although the statistics of another store in this " +
                "metrics recorder is" + (statistics != null ? " " : " not ") + "null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
    }

    private void initSensors(final StreamsMetricsImpl streamsMetrics, final TaskId taskId) {
        final RocksDBMetricContext metricContext =
            new RocksDBMetricContext(threadId, taskId.toString(), metricsScope, storeName);
        bytesWrittenToDatabaseSensor = RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricContext);
        bytesReadFromDatabaseSensor = RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricContext);
        memtableBytesFlushedSensor = RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricContext);
        memtableHitRatioSensor = RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricContext);
        writeStallDurationSensor = RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricContext);
        blockCacheDataHitRatioSensor = RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricContext);
        blockCacheIndexHitRatioSensor = RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricContext);
        blockCacheFilterHitRatioSensor = RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricContext);
        bytesWrittenDuringCompactionSensor =
            RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricContext);
        bytesReadDuringCompactionSensor = RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricContext);
        numberOfOpenFilesSensor = RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricContext);
        numberOfFileErrorsSensor = RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricContext);
    }

    public void removeValueProviders(final String segmentName) {
        logger.debug("Removing value providers for store {} of task {}", segmentName, taskId);
        final DbAndCacheAndStatistics removedValueProviders = storeToValueProviders.remove(segmentName);
        if (removedValueProviders == null) {
            throw new IllegalStateException("No value providers for store \"" + segmentName + "\" of task " + taskId +
                " could be found. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        removedValueProviders.maybeCloseStatistics();
        if (storeToValueProviders.isEmpty()) {
            logger.debug(
                "Removing metrics recorder for store {} of task {} from metrics recording trigger",
                storeName,
                taskId
            );
            streamsMetrics.rocksDBMetricsRecordingTrigger().removeMetricsRecorder(this);
        }
    }

    public void record(final long now) {
        logger.debug("Recording metrics for store {}", storeName);
        long bytesWrittenToDatabase = 0;
        long bytesReadFromDatabase = 0;
        long memtableBytesFlushed = 0;
        long memtableHits = 0;
        long memtableMisses = 0;
        long blockCacheDataHits = 0;
        long blockCacheDataMisses = 0;
        long blockCacheIndexHits = 0;
        long blockCacheIndexMisses = 0;
        long blockCacheFilterHits = 0;
        long blockCacheFilterMisses = 0;
        long writeStallDuration = 0;
        long bytesWrittenDuringCompaction = 0;
        long bytesReadDuringCompaction = 0;
        long numberOfOpenFiles = 0;
        long numberOfFileErrors = 0;
        boolean shouldRecord = true;
        for (final DbAndCacheAndStatistics valueProviders : storeToValueProviders.values()) {
            if (valueProviders.statistics == null) {
                shouldRecord = false;
                break;
            }
            bytesWrittenToDatabase += valueProviders.statistics.getAndResetTickerCount(TickerType.BYTES_WRITTEN);
            bytesReadFromDatabase += valueProviders.statistics.getAndResetTickerCount(TickerType.BYTES_READ);
            memtableBytesFlushed += valueProviders.statistics.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES);
            memtableHits += valueProviders.statistics.getAndResetTickerCount(TickerType.MEMTABLE_HIT);
            memtableMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.MEMTABLE_MISS);
            blockCacheDataHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
            blockCacheDataMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
            blockCacheIndexHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
            blockCacheIndexMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);
            blockCacheFilterHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT);
            blockCacheFilterMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);
            writeStallDuration += valueProviders.statistics.getAndResetTickerCount(TickerType.STALL_MICROS);
            bytesWrittenDuringCompaction += valueProviders.statistics.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES);
            bytesReadDuringCompaction += valueProviders.statistics.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES);
            numberOfOpenFiles += valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_OPENS)
                - valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_CLOSES);
            numberOfFileErrors += valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_ERRORS);
        }
        if (shouldRecord) {
            bytesWrittenToDatabaseSensor.record(bytesWrittenToDatabase, now);
            bytesReadFromDatabaseSensor.record(bytesReadFromDatabase, now);
            memtableBytesFlushedSensor.record(memtableBytesFlushed, now);
            memtableHitRatioSensor.record(computeHitRatio(memtableHits, memtableMisses), now);
            blockCacheDataHitRatioSensor.record(computeHitRatio(blockCacheDataHits, blockCacheDataMisses), now);
            blockCacheIndexHitRatioSensor.record(computeHitRatio(blockCacheIndexHits, blockCacheIndexMisses), now);
            blockCacheFilterHitRatioSensor.record(computeHitRatio(blockCacheFilterHits, blockCacheFilterMisses), now);
            writeStallDurationSensor.record(writeStallDuration, now);
            bytesWrittenDuringCompactionSensor.record(bytesWrittenDuringCompaction, now);
            bytesReadDuringCompactionSensor.record(bytesReadDuringCompaction, now);
            numberOfOpenFilesSensor.record(numberOfOpenFiles, now);
            numberOfFileErrorsSensor.record(numberOfFileErrors, now);
        }
    }

    private double computeHitRatio(final long hits, final long misses) {
        if (hits == 0) {
            return 0;
        }
        return (double) hits / (hits + misses);
    }
}