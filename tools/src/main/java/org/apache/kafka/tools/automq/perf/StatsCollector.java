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

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.tools.automq.perf.Stats.CumulativeStats;
import org.apache.kafka.tools.automq.perf.Stats.PeriodStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class StatsCollector {

    private static final double NANOS_PER_SEC = TimeUnit.SECONDS.toNanos(1);
    private static final double MICROS_PER_MILLI = TimeUnit.MILLISECONDS.toMicros(1);

    private static final double BYTES_PER_MB = 1 << 20;
    private static final double BYTES_PER_GB = 1 << 30;

    // max to 9999.9 s (167 min)
    private static final DecimalFormat DURATION_FORMAT = new PaddingDecimalFormat("0.0", 6);
    // max to 99.99%
    private static final DecimalFormat PERCENT_FORMAT = new PaddingDecimalFormat("0.00", 5);
    // max to 99999 MiB (100 GiB)
    private static final DecimalFormat MEMORY_FORMAT =  new PaddingDecimalFormat("0", 5);
    // max to 999999.99 msg/s (1M msg/s)
    private static final DecimalFormat RATE_FORMAT = new PaddingDecimalFormat("0.00", 9);
    // max to 999.99 MiB/s (1 GiB/s)
    private static final DecimalFormat THROUGHPUT_FORMAT = new PaddingDecimalFormat("0.00", 6);
    // max to 99999.999 ms (100 s)
    private static final DecimalFormat LATENCY_FORMAT = new PaddingDecimalFormat("0.000", 9);
    // max to 999.99
    private static final DecimalFormat COUNT_FORMAT = new PaddingDecimalFormat("0.00", 6);

    private static final String PERIOD_LOG_FORMAT = "{}s" +
        " | CPU {}% | Mem {} MiB heap / {} MiB direct" +
        " | Prod rate {} msg/s / {} MiB/s | Prod err {} err/s" +
        " | Cons rate {} msg/s / {} MiB/s | Backlog: {} K msg" +
        " | Prod Latency (ms) avg: {} - min: {} - 50%: {} - 99%: {} - 99.9%: {} - max: {}" +
        " | E2E Latency (ms) avg: {} - min: {} - 50%: {} - 99%: {} - 99.9%: {} - max: {}";
    private static final String SUMMARY_LOG_FORMAT = "Summary" +
        " | Prod rate {} msg/s / {} MiB/s | Prod total {} M msg / {} GiB / {} K err" +
        " | Cons rate {} msg/s / {} MiB/s | Cons total {} M msg / {} GiB" +
        " | Prod Latency (ms) avg: {} - min: {} - 50%: {} - 75%: {} - 90%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - max: {}" +
        " | E2E Latency (ms) avg: {} - min: {} - 50%: {} - 75%: {} - 90%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - max: {}";

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsCollector.class);

    public static Result printAndCollectStats(Stats stats, StopCondition condition, long intervalNanos,
        PerfConfig config) {
        final long start = System.nanoTime();
        CpuMonitor cpu = new CpuMonitor();
        Result result = new Result(config);

        long last = start;
        while (true) {
            try {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(intervalNanos));
            } catch (InterruptedException e) {
                break;
            }

            PeriodStats periodStats = stats.toPeriodStats();
            double elapsed = (periodStats.nowNanos - last) / NANOS_PER_SEC;
            double elapsedTotal = (periodStats.nowNanos - start) / NANOS_PER_SEC;

            PeriodResult periodResult = new PeriodResult(cpu, periodStats, elapsed, config.groupsPerTopic);
            result.update(periodResult, elapsedTotal);
            periodResult.logIt(elapsedTotal);

            if (condition.shouldStop(start, periodStats.nowNanos)) {
                CumulativeStats cumulativeStats = stats.toCumulativeStats();
                elapsedTotal = (cumulativeStats.nowNanos - start) / NANOS_PER_SEC;

                CumulativeResult cumulativeResult = new CumulativeResult(cumulativeStats, elapsedTotal);
                result.update(cumulativeResult);
                cumulativeResult.logIt();
                break;
            }

            last = periodStats.nowNanos;
        }
        return result;
    }

    @FunctionalInterface
    public interface StopCondition {
        /**
         * Check if the loop in {@link StatsCollector#printAndCollectStats} should stop.
         *
         * @param startNanos start time in nanoseconds
         * @param nowNanos   current time in nanoseconds
         * @return true if the loop should stop
         */
        boolean shouldStop(long startNanos, long nowNanos);
    }

    public static long currentNanos() {
        Instant currentTime = Instant.now();
        return TimeUnit.SECONDS.toNanos(currentTime.getEpochSecond()) + currentTime.getNano();
    }

    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class Result {
        public final PerfConfig config;
        public final long startMs;
        public double produceRateTotal;
        public double produceThroughputTotalBps;
        public double produceCountTotal;
        public double produceSizeTotalBytes;
        public double produceErrorTotal;
        public double consumeRateTotal;
        public double consumeThroughputTotalBps;
        public double consumeCountTotal;
        public double consumeSizeTotalBytes;
        public double produceLatencyMeanTotalMicros;
        public double produceLatency50thTotalMicros;
        public double produceLatency75thTotalMicros;
        public double produceLatency90thTotalMicros;
        public double produceLatency95thTotalMicros;
        public double produceLatency99thTotalMicros;
        public double produceLatency999thTotalMicros;
        public double produceLatency9999thTotalMicros;
        public double produceLatencyMaxTotalMicros;
        public double endToEndLatencyMeanTotalMicros;
        public double endToEndLatency50thTotalMicros;
        public double endToEndLatency75thTotalMicros;
        public double endToEndLatency90thTotalMicros;
        public double endToEndLatency95thTotalMicros;
        public double endToEndLatency99thTotalMicros;
        public double endToEndLatency999thTotalMicros;
        public double endToEndLatency9999thTotalMicros;
        public double endToEndLatencyMaxTotalMicros;
        public final List<Double> timeSecs = new ArrayList<>();
        public final List<Double> produceRate = new ArrayList<>();
        public final List<Double> produceThroughputBps = new ArrayList<>();
        public final List<Double> errorRate = new ArrayList<>();
        public final List<Double> consumeRate = new ArrayList<>();
        public final List<Double> consumeThroughputBps = new ArrayList<>();
        public final List<Long> backlog = new ArrayList<>();
        public final List<Double> produceLatencyMeanMicros = new ArrayList<>();
        public final List<Double> produceLatencyMinMicros = new ArrayList<>();
        public final List<Double> produceLatency50thMicros = new ArrayList<>();
        public final List<Double> produceLatency75thMicros = new ArrayList<>();
        public final List<Double> produceLatency90thMicros = new ArrayList<>();
        public final List<Double> produceLatency95thMicros = new ArrayList<>();
        public final List<Double> produceLatency99thMicros = new ArrayList<>();
        public final List<Double> produceLatency999thMicros = new ArrayList<>();
        public final List<Double> produceLatency9999thMicros = new ArrayList<>();
        public final List<Double> produceLatencyMaxMicros = new ArrayList<>();
        public Map<Double, Long> produceLatencyQuantilesMicros;
        public final List<Double> endToEndLatencyMeanMicros = new ArrayList<>();
        public final List<Double> endToEndLatencyMinMicros = new ArrayList<>();
        public final List<Double> endToEndLatency50thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency75thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency90thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency95thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency99thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency999thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency9999thMicros = new ArrayList<>();
        public final List<Double> endToEndLatencyMaxMicros = new ArrayList<>();
        public Map<Double, Long> endToEndLatencyQuantilesMicros;

        private Result(PerfConfig config) {
            this.config = config;
            this.startMs = System.currentTimeMillis();
        }

        private void update(PeriodResult periodResult, double elapsedTotal) {
            this.timeSecs.add(elapsedTotal);
            this.produceRate.add(periodResult.produceRate);
            this.produceThroughputBps.add(periodResult.produceThroughputBps);
            this.errorRate.add(periodResult.errorRate);
            this.consumeRate.add(periodResult.consumeRate);
            this.consumeThroughputBps.add(periodResult.consumeThroughputBps);
            this.backlog.add(periodResult.backlog);
            this.produceLatencyMeanMicros.add(periodResult.produceLatencyMeanMicros);
            this.produceLatencyMinMicros.add(periodResult.produceLatencyMinMicros);
            this.produceLatency50thMicros.add(periodResult.produceLatency50thMicros);
            this.produceLatency75thMicros.add(periodResult.produceLatency75thMicros);
            this.produceLatency90thMicros.add(periodResult.produceLatency90thMicros);
            this.produceLatency95thMicros.add(periodResult.produceLatency95thMicros);
            this.produceLatency99thMicros.add(periodResult.produceLatency99thMicros);
            this.produceLatency999thMicros.add(periodResult.produceLatency999thMicros);
            this.produceLatency9999thMicros.add(periodResult.produceLatency9999thMicros);
            this.produceLatencyMaxMicros.add(periodResult.produceLatencyMaxMicros);
            this.endToEndLatencyMeanMicros.add(periodResult.endToEndLatencyMeanMicros);
            this.endToEndLatencyMinMicros.add(periodResult.endToEndLatencyMinMicros);
            this.endToEndLatency50thMicros.add(periodResult.endToEndLatency50thMicros);
            this.endToEndLatency75thMicros.add(periodResult.endToEndLatency75thMicros);
            this.endToEndLatency90thMicros.add(periodResult.endToEndLatency90thMicros);
            this.endToEndLatency95thMicros.add(periodResult.endToEndLatency95thMicros);
            this.endToEndLatency99thMicros.add(periodResult.endToEndLatency99thMicros);
            this.endToEndLatency999thMicros.add(periodResult.endToEndLatency999thMicros);
            this.endToEndLatency9999thMicros.add(periodResult.endToEndLatency9999thMicros);
            this.endToEndLatencyMaxMicros.add(periodResult.endToEndLatencyMaxMicros);
        }

        private void update(CumulativeResult cumulativeResult) {
            this.produceRateTotal = cumulativeResult.produceRateTotal;
            this.produceThroughputTotalBps = cumulativeResult.produceThroughputTotalBps;
            this.produceCountTotal = cumulativeResult.produceCountTotal;
            this.produceSizeTotalBytes = cumulativeResult.produceSizeTotalBytes;
            this.produceErrorTotal = cumulativeResult.produceErrorTotal;
            this.consumeRateTotal = cumulativeResult.consumeRateTotal;
            this.consumeThroughputTotalBps = cumulativeResult.consumeThroughputTotalBps;
            this.consumeCountTotal = cumulativeResult.consumeCountTotal;
            this.consumeSizeTotalBytes = cumulativeResult.consumeSizeTotalBytes;
            this.produceLatencyMeanTotalMicros = cumulativeResult.produceLatencyMeanTotalMicros;
            this.produceLatency50thTotalMicros = cumulativeResult.produceLatency50thTotalMicros;
            this.produceLatency75thTotalMicros = cumulativeResult.produceLatency75thTotalMicros;
            this.produceLatency90thTotalMicros = cumulativeResult.produceLatency90thTotalMicros;
            this.produceLatency95thTotalMicros = cumulativeResult.produceLatency95thTotalMicros;
            this.produceLatency99thTotalMicros = cumulativeResult.produceLatency99thTotalMicros;
            this.produceLatency999thTotalMicros = cumulativeResult.produceLatency999thTotalMicros;
            this.produceLatency9999thTotalMicros = cumulativeResult.produceLatency9999thTotalMicros;
            this.produceLatencyMaxTotalMicros = cumulativeResult.produceLatencyMaxTotalMicros;
            this.produceLatencyQuantilesMicros = cumulativeResult.produceLatencyQuantilesMicros;
            this.endToEndLatencyMeanTotalMicros = cumulativeResult.endToEndLatencyMeanTotalMicros;
            this.endToEndLatency50thTotalMicros = cumulativeResult.endToEndLatency50thTotalMicros;
            this.endToEndLatency75thTotalMicros = cumulativeResult.endToEndLatency75thTotalMicros;
            this.endToEndLatency90thTotalMicros = cumulativeResult.endToEndLatency90thTotalMicros;
            this.endToEndLatency95thTotalMicros = cumulativeResult.endToEndLatency95thTotalMicros;
            this.endToEndLatency99thTotalMicros = cumulativeResult.endToEndLatency99thTotalMicros;
            this.endToEndLatency999thTotalMicros = cumulativeResult.endToEndLatency999thTotalMicros;
            this.endToEndLatency9999thTotalMicros = cumulativeResult.endToEndLatency9999thTotalMicros;
            this.endToEndLatencyMaxTotalMicros = cumulativeResult.endToEndLatencyMaxTotalMicros;
            this.endToEndLatencyQuantilesMicros = cumulativeResult.endToEndLatencyQuantilesMicros;
        }
    }

    private static class PeriodResult {
        private final double cpuUsage;
        private final long heapMemoryUsed;
        private final long directMemoryUsed;
        private final double produceRate;
        private final double produceThroughputBps;
        private final double errorRate;
        private final double consumeRate;
        private final double consumeThroughputBps;
        private final long backlog;
        private final double produceLatencyMeanMicros;
        private final double produceLatencyMinMicros;
        private final double produceLatency50thMicros;
        private final double produceLatency75thMicros;
        private final double produceLatency90thMicros;
        private final double produceLatency95thMicros;
        private final double produceLatency99thMicros;
        private final double produceLatency999thMicros;
        private final double produceLatency9999thMicros;
        private final double produceLatencyMaxMicros;
        private final double endToEndLatencyMeanMicros;
        private final double endToEndLatencyMinMicros;
        private final double endToEndLatency50thMicros;
        private final double endToEndLatency75thMicros;
        private final double endToEndLatency90thMicros;
        private final double endToEndLatency95thMicros;
        private final double endToEndLatency99thMicros;
        private final double endToEndLatency999thMicros;
        private final double endToEndLatency9999thMicros;
        private final double endToEndLatencyMaxMicros;

        private PeriodResult(CpuMonitor cpu, PeriodStats stats, double elapsed, int readWriteRatio) {
            this.cpuUsage = cpu.usage();
            this.heapMemoryUsed = MemoryMonitor.heapUsed();
            this.directMemoryUsed = MemoryMonitor.directUsed();
            this.produceRate = stats.messagesSent / elapsed;
            this.produceThroughputBps = stats.bytesSent / elapsed;
            this.errorRate = stats.messagesSendFailed / elapsed;
            this.consumeRate = stats.messagesReceived / elapsed;
            this.consumeThroughputBps = stats.bytesReceived / elapsed;
            this.backlog = Math.max(0, readWriteRatio * stats.totalMessagesSent - stats.totalMessagesReceived);
            this.produceLatencyMeanMicros = stats.sendLatencyMicros.getMean();
            this.produceLatencyMinMicros = stats.sendLatencyMicros.getMinValue();
            this.produceLatency50thMicros = stats.sendLatencyMicros.getValueAtPercentile(50);
            this.produceLatency75thMicros = stats.sendLatencyMicros.getValueAtPercentile(75);
            this.produceLatency90thMicros = stats.sendLatencyMicros.getValueAtPercentile(90);
            this.produceLatency95thMicros = stats.sendLatencyMicros.getValueAtPercentile(95);
            this.produceLatency99thMicros = stats.sendLatencyMicros.getValueAtPercentile(99);
            this.produceLatency999thMicros = stats.sendLatencyMicros.getValueAtPercentile(99.9);
            this.produceLatency9999thMicros = stats.sendLatencyMicros.getValueAtPercentile(99.99);
            this.produceLatencyMaxMicros = stats.sendLatencyMicros.getMaxValue();
            this.endToEndLatencyMeanMicros = stats.endToEndLatencyMicros.getMean();
            this.endToEndLatencyMinMicros = stats.endToEndLatencyMicros.getMinValue();
            this.endToEndLatency50thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(50);
            this.endToEndLatency75thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(75);
            this.endToEndLatency90thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(90);
            this.endToEndLatency95thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(95);
            this.endToEndLatency99thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(99);
            this.endToEndLatency999thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(99.9);
            this.endToEndLatency9999thMicros = stats.endToEndLatencyMicros.getValueAtPercentile(99.99);
            this.endToEndLatencyMaxMicros = stats.endToEndLatencyMicros.getMaxValue();
        }

        private void logIt(double elapsedTotal) {
            LOGGER.info(PERIOD_LOG_FORMAT,
                DURATION_FORMAT.format(elapsedTotal),
                PERCENT_FORMAT.format(cpuUsage * 100),
                MEMORY_FORMAT.format(heapMemoryUsed / BYTES_PER_MB),
                MEMORY_FORMAT.format(directMemoryUsed / BYTES_PER_MB),
                RATE_FORMAT.format(produceRate),
                THROUGHPUT_FORMAT.format(produceThroughputBps / BYTES_PER_MB),
                RATE_FORMAT.format(errorRate),
                RATE_FORMAT.format(consumeRate),
                THROUGHPUT_FORMAT.format(consumeThroughputBps / BYTES_PER_MB),
                COUNT_FORMAT.format(backlog / 1_000.0),
                LATENCY_FORMAT.format(produceLatencyMeanMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatencyMinMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency50thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency99thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency999thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatencyMaxMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMeanMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMinMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency50thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency99thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency999thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMaxMicros / MICROS_PER_MILLI)
            );
        }
    }

    private static class CumulativeResult {
        private final double produceRateTotal;
        private final double produceThroughputTotalBps;
        private final double produceCountTotal;
        private final double produceSizeTotalBytes;
        private final double produceErrorTotal;
        private final double consumeRateTotal;
        private final double consumeThroughputTotalBps;
        private final double consumeCountTotal;
        private final double consumeSizeTotalBytes;
        private final double produceLatencyMeanTotalMicros;
        private final double produceLatencyMinTotalMicros;
        private final double produceLatency50thTotalMicros;
        private final double produceLatency75thTotalMicros;
        private final double produceLatency90thTotalMicros;
        private final double produceLatency95thTotalMicros;
        private final double produceLatency99thTotalMicros;
        private final double produceLatency999thTotalMicros;
        private final double produceLatency9999thTotalMicros;
        private final double produceLatencyMaxTotalMicros;
        public final Map<Double, Long> produceLatencyQuantilesMicros = new TreeMap<>();
        private final double endToEndLatencyMeanTotalMicros;
        private final double endToEndLatencyMinTotalMicros;
        private final double endToEndLatency50thTotalMicros;
        private final double endToEndLatency75thTotalMicros;
        private final double endToEndLatency90thTotalMicros;
        private final double endToEndLatency95thTotalMicros;
        private final double endToEndLatency99thTotalMicros;
        private final double endToEndLatency999thTotalMicros;
        private final double endToEndLatency9999thTotalMicros;
        private final double endToEndLatencyMaxTotalMicros;
        public final Map<Double, Long> endToEndLatencyQuantilesMicros = new TreeMap<>();

        private CumulativeResult(CumulativeStats stats, double elapsedTotal) {
            produceRateTotal = stats.totalMessagesSent / elapsedTotal;
            produceThroughputTotalBps = stats.totalBytesSent / elapsedTotal;
            produceCountTotal = stats.totalMessagesSent;
            produceSizeTotalBytes = stats.totalBytesSent;
            produceErrorTotal = stats.totalMessagesSendFailed;
            consumeRateTotal = stats.totalMessagesReceived / elapsedTotal;
            consumeThroughputTotalBps = stats.totalBytesReceived / elapsedTotal;
            consumeCountTotal = stats.totalMessagesReceived;
            consumeSizeTotalBytes = stats.totalBytesReceived;
            produceLatencyMeanTotalMicros = stats.totalSendLatencyMicros.getMean();
            produceLatencyMinTotalMicros = stats.totalSendLatencyMicros.getMinValue();
            produceLatency50thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(50);
            produceLatency75thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(75);
            produceLatency90thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(90);
            produceLatency95thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(95);
            produceLatency99thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(99);
            produceLatency999thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(99.9);
            produceLatency9999thTotalMicros = stats.totalSendLatencyMicros.getValueAtPercentile(99.99);
            produceLatencyMaxTotalMicros = stats.totalSendLatencyMicros.getMaxValue();
            stats.totalSendLatencyMicros.percentiles(100).forEach(
                value -> produceLatencyQuantilesMicros.put(value.getPercentile(), value.getValueIteratedTo())
            );
            endToEndLatencyMeanTotalMicros = stats.totalEndToEndLatencyMicros.getMean();
            endToEndLatencyMinTotalMicros = stats.totalEndToEndLatencyMicros.getMinValue();
            endToEndLatency50thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(50);
            endToEndLatency75thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(75);
            endToEndLatency90thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(90);
            endToEndLatency95thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(95);
            endToEndLatency99thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(99);
            endToEndLatency999thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(99.9);
            endToEndLatency9999thTotalMicros = stats.totalEndToEndLatencyMicros.getValueAtPercentile(99.99);
            endToEndLatencyMaxTotalMicros = stats.totalEndToEndLatencyMicros.getMaxValue();
            stats.totalEndToEndLatencyMicros.percentiles(100).forEach(
                value -> endToEndLatencyQuantilesMicros.put(value.getPercentile(), value.getValueIteratedTo())
            );
        }

        private void logIt() {
            LOGGER.info(SUMMARY_LOG_FORMAT,
                RATE_FORMAT.format(produceRateTotal),
                THROUGHPUT_FORMAT.format(produceThroughputTotalBps / BYTES_PER_MB),
                COUNT_FORMAT.format(produceCountTotal / 1_000_000.0),
                COUNT_FORMAT.format(produceSizeTotalBytes / BYTES_PER_GB),
                COUNT_FORMAT.format(produceErrorTotal / 1_000.0),
                RATE_FORMAT.format(consumeRateTotal),
                THROUGHPUT_FORMAT.format(consumeThroughputTotalBps / BYTES_PER_MB),
                COUNT_FORMAT.format(consumeCountTotal / 1_000_000.0),
                COUNT_FORMAT.format(consumeSizeTotalBytes / BYTES_PER_GB),
                LATENCY_FORMAT.format(produceLatencyMeanTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatencyMinTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency50thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency75thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency90thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency95thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency99thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency999thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency9999thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatencyMaxTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMeanTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMinTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency50thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency75thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency90thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency95thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency99thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency999thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency9999thTotalMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMaxTotalMicros / MICROS_PER_MILLI)
            );
        }
    }
}
