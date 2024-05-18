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

package org.apache.kafka.tools.automq.perf;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.tools.automq.perf.Stats.CumulativeStats;
import org.apache.kafka.tools.automq.perf.Stats.PeriodStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsCollector {

    private static final double NANOS_PER_SEC = TimeUnit.SECONDS.toNanos(1);
    private static final double MICROS_PER_MILLI = TimeUnit.MILLISECONDS.toMicros(1);

    private static final double BYTES_PER_MB = 1 << 20;
    private static final double BYTES_PER_GB = 1 << 30;

    // max to 9999.9 (167 min)
    private static final DecimalFormat DURATION_FORMAT = new PaddingDecimalFormat("0.0", 6);
    // max to 999999.99 (1M msg/s)
    private static final DecimalFormat RATE_FORMAT = new PaddingDecimalFormat("0.00", 9);
    // max to 999.99 (1 GB/s)
    private static final DecimalFormat THROUGHPUT_FORMAT = new PaddingDecimalFormat("0.00", 6);
    // max to 99999.999 (100 s)
    private static final DecimalFormat LATENCY_FORMAT = new PaddingDecimalFormat("0.000", 9);
    private static final DecimalFormat COUNT_FORMAT = new PaddingDecimalFormat("0.00", 6);

    private static final String PERIOD_LOG_FORMAT = "{}s" +
        " | Prod rate {} msg/s / {} MiB/s | Prod err {} err/s" +
        " | Cons rate {} msg/s / {} MiB/s | Backlog: {} K msg" +
        " | Prod Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}" +
        " | E2E Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}";
    private static final String SUMMARY_LOG_FORMAT = "Summary" +
        " | Prod rate {} msg/s / {} MiB/s | Prod total {} M msg / {} GiB / {} K err" +
        " | Cons rate {} msg/s / {} MiB/s | Cons total {} M msg / {} GiB" +
        " | Prod Latency (ms) avg: {} - 50%: {} - 75%: {} - 90%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}" +
        " | E2E Latency (ms) avg: {} - 50%: {} - 75%: {} - 90%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}";

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsCollector.class);

    public static Result printAndCollectStats(Stats stats, StopCondition condition, long intervalNanos,
        PerfConfig config) {
        final long start = System.nanoTime();
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
            last = periodStats.nowNanos;

            double produceRate = periodStats.messagesSent / elapsed;
            double produceThroughputBps = periodStats.bytesSent / elapsed;
            double errorRate = periodStats.messagesSendFailed / elapsed;
            double consumeRate = periodStats.messagesReceived / elapsed;
            double consumeThroughputBps = periodStats.bytesReceived / elapsed;
            long backlog = Math.max(0, config.groupsPerTopic * periodStats.totalMessagesSent - periodStats.totalMessagesReceived);
            double produceLatencyMeanMicros = periodStats.sendLatencyMicros.getMean();
            double produceLatency50thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(50);
            double produceLatency75thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(75);
            double produceLatency90thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(90);
            double produceLatency95thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(95);
            double produceLatency99thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(99);
            double produceLatency999thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(99.9);
            double produceLatency9999thMicros = periodStats.sendLatencyMicros.getValueAtPercentile(99.99);
            double produceLatencyMaxMicros = periodStats.sendLatencyMicros.getMaxValue();
            double endToEndLatencyMeanMicros = periodStats.endToEndLatencyMicros.getMean();
            double endToEndLatency50thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(50);
            double endToEndLatency75thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(75);
            double endToEndLatency90thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(90);
            double endToEndLatency95thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(95);
            double endToEndLatency99thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(99);
            double endToEndLatency999thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(99.9);
            double endToEndLatency9999thMicros = periodStats.endToEndLatencyMicros.getValueAtPercentile(99.99);
            double endToEndLatencyMaxMicros = periodStats.endToEndLatencyMicros.getMaxValue();

            result.timeSecs.add(elapsedTotal);
            result.produceRate.add(produceRate);
            result.produceThroughputBps.add(produceThroughputBps);
            result.errorRate.add(errorRate);
            result.consumeRate.add(consumeRate);
            result.consumeThroughputBps.add(consumeThroughputBps);
            result.backlog.add(backlog);
            result.produceLatencyMeanMicros.add(produceLatencyMeanMicros);
            result.produceLatency50thMicros.add(produceLatency50thMicros);
            result.produceLatency75thMicros.add(produceLatency75thMicros);
            result.produceLatency90thMicros.add(produceLatency90thMicros);
            result.produceLatency95thMicros.add(produceLatency95thMicros);
            result.produceLatency99thMicros.add(produceLatency99thMicros);
            result.produceLatency999thMicros.add(produceLatency999thMicros);
            result.produceLatency9999thMicros.add(produceLatency9999thMicros);
            result.produceLatencyMaxMicros.add(produceLatencyMaxMicros);
            result.endToEndLatencyMeanMicros.add(endToEndLatencyMeanMicros);
            result.endToEndLatency50thMicros.add(endToEndLatency50thMicros);
            result.endToEndLatency75thMicros.add(endToEndLatency75thMicros);
            result.endToEndLatency90thMicros.add(endToEndLatency90thMicros);
            result.endToEndLatency95thMicros.add(endToEndLatency95thMicros);
            result.endToEndLatency99thMicros.add(endToEndLatency99thMicros);
            result.endToEndLatency999thMicros.add(endToEndLatency999thMicros);
            result.endToEndLatency9999thMicros.add(endToEndLatency9999thMicros);
            result.endToEndLatencyMaxMicros.add(endToEndLatencyMaxMicros);

            LOGGER.info(PERIOD_LOG_FORMAT,
                DURATION_FORMAT.format(elapsedTotal),
                RATE_FORMAT.format(produceRate),
                THROUGHPUT_FORMAT.format(produceThroughputBps / BYTES_PER_MB),
                RATE_FORMAT.format(errorRate),
                RATE_FORMAT.format(consumeRate),
                THROUGHPUT_FORMAT.format(consumeThroughputBps / BYTES_PER_MB),
                COUNT_FORMAT.format(backlog / 1_000.0),
                LATENCY_FORMAT.format(produceLatencyMeanMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency50thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency99thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatency999thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(produceLatencyMaxMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMeanMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency50thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency99thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatency999thMicros / MICROS_PER_MILLI),
                LATENCY_FORMAT.format(endToEndLatencyMaxMicros / MICROS_PER_MILLI)
            );

            if (condition.shouldStop(start, periodStats.nowNanos)) {
                CumulativeStats cumulativeStats = stats.toCumulativeStats();
                elapsedTotal = (cumulativeStats.nowNanos - start) / NANOS_PER_SEC;

                double produceRateTotal = cumulativeStats.totalMessagesSent / elapsedTotal;
                double produceThroughputTotalBps = cumulativeStats.totalBytesSent / elapsedTotal;
                double produceCountTotal = cumulativeStats.totalMessagesSent;
                double produceSizeTotalBytes = cumulativeStats.totalBytesSent;
                double produceErrorTotal = cumulativeStats.totalMessagesSendFailed;
                double consumeRateTotal = cumulativeStats.totalMessagesReceived / elapsedTotal;
                double consumeThroughputTotalBps = cumulativeStats.totalBytesReceived / elapsedTotal;
                double consumeCountTotal = cumulativeStats.totalMessagesReceived;
                double consumeSizeTotalBytes = cumulativeStats.totalBytesReceived;
                double produceLatencyMeanTotalMicros = cumulativeStats.totalSendLatencyMicros.getMean();
                double produceLatency50thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(50);
                double produceLatency75thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(75);
                double produceLatency90thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(90);
                double produceLatency95thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(95);
                double produceLatency99thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99);
                double produceLatency999thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99.9);
                double produceLatency9999thTotalMicros = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99.99);
                double produceLatencyMaxTotalMicros = cumulativeStats.totalSendLatencyMicros.getMaxValue();
                double endToEndLatencyMeanTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getMean();
                double endToEndLatency50thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(50);
                double endToEndLatency75thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(75);
                double endToEndLatency90thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(90);
                double endToEndLatency95thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(95);
                double endToEndLatency99thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99);
                double endToEndLatency999thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99.9);
                double endToEndLatency9999thTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99.99);
                double endToEndLatencyMaxTotalMicros = cumulativeStats.totalEndToEndLatencyMicros.getMaxValue();

                result.produceRateTotal = produceRateTotal;
                result.produceThroughputTotalBps = produceThroughputTotalBps;
                result.produceCountTotal = produceCountTotal;
                result.produceSizeTotalBytes = produceSizeTotalBytes;
                result.produceErrorTotal = produceErrorTotal;
                result.consumeRateTotal = consumeRateTotal;
                result.consumeThroughputTotalBps = consumeThroughputTotalBps;
                result.consumeCountTotal = consumeCountTotal;
                result.consumeSizeTotalBytes = consumeSizeTotalBytes;
                result.produceLatencyMeanTotalMicros = produceLatencyMeanTotalMicros;
                result.produceLatency50thTotalMicros = produceLatency50thTotalMicros;
                result.produceLatency75thTotalMicros = produceLatency75thTotalMicros;
                result.produceLatency90thTotalMicros = produceLatency90thTotalMicros;
                result.produceLatency95thTotalMicros = produceLatency95thTotalMicros;
                result.produceLatency99thTotalMicros = produceLatency99thTotalMicros;
                result.produceLatency999thTotalMicros = produceLatency999thTotalMicros;
                result.produceLatency9999thTotalMicros = produceLatency9999thTotalMicros;
                result.produceLatencyMaxTotalMicros = produceLatencyMaxTotalMicros;
                cumulativeStats.totalSendLatencyMicros.percentiles(100).forEach(
                    value -> result.produceLatencyQuantilesMicros.put(value.getPercentile(), value.getValueIteratedTo())
                );
                result.endToEndLatencyMeanTotalMicros = endToEndLatencyMeanTotalMicros;
                result.endToEndLatency50thTotalMicros = endToEndLatency50thTotalMicros;
                result.endToEndLatency75thTotalMicros = endToEndLatency75thTotalMicros;
                result.endToEndLatency90thTotalMicros = endToEndLatency90thTotalMicros;
                result.endToEndLatency95thTotalMicros = endToEndLatency95thTotalMicros;
                result.endToEndLatency99thTotalMicros = endToEndLatency99thTotalMicros;
                result.endToEndLatency999thTotalMicros = endToEndLatency999thTotalMicros;
                result.endToEndLatency9999thTotalMicros = endToEndLatency9999thTotalMicros;
                result.endToEndLatencyMaxTotalMicros = endToEndLatencyMaxTotalMicros;
                cumulativeStats.totalEndToEndLatencyMicros.percentiles(100).forEach(
                    value -> result.endToEndLatencyQuantilesMicros.put(value.getPercentile(), value.getValueIteratedTo())
                );

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
                    LATENCY_FORMAT.format(produceLatency50thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency75thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency90thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency95thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency99thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency999thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatency9999thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(produceLatencyMaxTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatencyMeanTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency50thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency75thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency90thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency95thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency99thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency999thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatency9999thTotalMicros / MICROS_PER_MILLI),
                    LATENCY_FORMAT.format(endToEndLatencyMaxTotalMicros / MICROS_PER_MILLI)
                );
                break;
            }
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
        public final List<Double> produceLatency50thMicros = new ArrayList<>();
        public final List<Double> produceLatency75thMicros = new ArrayList<>();
        public final List<Double> produceLatency90thMicros = new ArrayList<>();
        public final List<Double> produceLatency95thMicros = new ArrayList<>();
        public final List<Double> produceLatency99thMicros = new ArrayList<>();
        public final List<Double> produceLatency999thMicros = new ArrayList<>();
        public final List<Double> produceLatency9999thMicros = new ArrayList<>();
        public final List<Double> produceLatencyMaxMicros = new ArrayList<>();
        public final Map<Double, Long> produceLatencyQuantilesMicros = new TreeMap<>();
        public final List<Double> endToEndLatencyMeanMicros = new ArrayList<>();
        public final List<Double> endToEndLatency50thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency75thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency90thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency95thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency99thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency999thMicros = new ArrayList<>();
        public final List<Double> endToEndLatency9999thMicros = new ArrayList<>();
        public final List<Double> endToEndLatencyMaxMicros = new ArrayList<>();
        public final Map<Double, Long> endToEndLatencyQuantilesMicros = new TreeMap<>();

        public Result(PerfConfig config) {
            this.config = config;
            this.startMs = System.currentTimeMillis();
        }
    }
}
