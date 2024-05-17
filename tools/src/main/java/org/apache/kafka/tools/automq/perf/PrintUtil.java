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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.tools.automq.perf.Stats.PeriodStats;
import org.apache.kafka.tools.automq.perf.Stats.CumulativeStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintUtil {

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

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintUtil.class);

    public static String printAndCollectStats(Stats stats, StopCondition condition, long intervalNanos, int readWriteRatio) {
        final long start = System.nanoTime();

        long last = start;
        while (true) {
            try {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(intervalNanos));
            } catch (InterruptedException e) {
                break;
            }

            PeriodStats periodStats = stats.toPeriodStats();
            long now = System.nanoTime();

            double elapsed = (now - last) / NANOS_PER_SEC;
            double produceRate = periodStats.messagesSent / elapsed;
            double produceThroughput = periodStats.bytesSent / BYTES_PER_MB / elapsed;
            double errorRate = periodStats.messagesSendFailed / elapsed;
            double consumeRate = periodStats.messagesReceived / elapsed;
            double consumeThroughput = periodStats.bytesReceived / BYTES_PER_MB / elapsed;
            long backlogs = Math.max(0, readWriteRatio * periodStats.totalMessagesSent - periodStats.totalMessagesReceived);
            double produceLatencyMean = periodStats.sendLatencyMicros.getMean() / MICROS_PER_MILLI;
            double produceLatency50th = periodStats.sendLatencyMicros.getValueAtPercentile(50) / MICROS_PER_MILLI;
            double produceLatency75th = periodStats.sendLatencyMicros.getValueAtPercentile(75) / MICROS_PER_MILLI;
            double produceLatency90th = periodStats.sendLatencyMicros.getValueAtPercentile(90) / MICROS_PER_MILLI;
            double produceLatency95th = periodStats.sendLatencyMicros.getValueAtPercentile(95) / MICROS_PER_MILLI;
            double produceLatency99th = periodStats.sendLatencyMicros.getValueAtPercentile(99) / MICROS_PER_MILLI;
            double produceLatency999th = periodStats.sendLatencyMicros.getValueAtPercentile(99.9) / MICROS_PER_MILLI;
            double produceLatency9999th = periodStats.sendLatencyMicros.getValueAtPercentile(99.99) / MICROS_PER_MILLI;
            double produceLatencyMax = periodStats.sendLatencyMicros.getMaxValue() / MICROS_PER_MILLI;
            double endToEndLatencyMean = periodStats.endToEndLatencyMicros.getMean() / MICROS_PER_MILLI;
            double endToEndLatency50th = periodStats.endToEndLatencyMicros.getValueAtPercentile(50) / MICROS_PER_MILLI;
            double endToEndLatency75th = periodStats.endToEndLatencyMicros.getValueAtPercentile(75) / MICROS_PER_MILLI;
            double endToEndLatency90th = periodStats.endToEndLatencyMicros.getValueAtPercentile(90) / MICROS_PER_MILLI;
            double endToEndLatency95th = periodStats.endToEndLatencyMicros.getValueAtPercentile(95) / MICROS_PER_MILLI;
            double endToEndLatency99th = periodStats.endToEndLatencyMicros.getValueAtPercentile(99) / MICROS_PER_MILLI;
            double endToEndLatency999th = periodStats.endToEndLatencyMicros.getValueAtPercentile(99.9) / MICROS_PER_MILLI;
            double endToEndLatency9999th = periodStats.endToEndLatencyMicros.getValueAtPercentile(99.99) / MICROS_PER_MILLI;
            double endToEndLatencyMax = periodStats.endToEndLatencyMicros.getMaxValue() / MICROS_PER_MILLI;

            LOGGER.info(PERIOD_LOG_FORMAT,
                DURATION_FORMAT.format(elapsed),
                RATE_FORMAT.format(produceRate),
                THROUGHPUT_FORMAT.format(produceThroughput),
                RATE_FORMAT.format(errorRate),
                RATE_FORMAT.format(consumeRate),
                THROUGHPUT_FORMAT.format(consumeThroughput),
                COUNT_FORMAT.format(backlogs / 1_000.0),
                LATENCY_FORMAT.format(produceLatencyMean),
                LATENCY_FORMAT.format(produceLatency50th),
                LATENCY_FORMAT.format(produceLatency99th),
                LATENCY_FORMAT.format(produceLatency999th),
                LATENCY_FORMAT.format(produceLatencyMax),
                LATENCY_FORMAT.format(endToEndLatencyMean),
                LATENCY_FORMAT.format(endToEndLatency50th),
                LATENCY_FORMAT.format(endToEndLatency99th),
                LATENCY_FORMAT.format(endToEndLatency999th),
                LATENCY_FORMAT.format(endToEndLatencyMax)
            );

            if (condition.shouldStop(start, now)) {
                CumulativeStats cumulativeStats = stats.toCumulativeStats();
                now = System.nanoTime();

                double elapsedTotal = (now - start) / NANOS_PER_SEC;
                double produceRateTotal = cumulativeStats.totalMessagesSent / elapsedTotal;
                double produceThroughputTotal = cumulativeStats.totalBytesSent / BYTES_PER_MB / elapsedTotal;
                double produceCountTotal = cumulativeStats.totalMessagesSent / 1_000_000.0;
                double produceSizeTotal = cumulativeStats.totalBytesSent / BYTES_PER_GB;
                double produceErrorTotal = cumulativeStats.totalMessagesSendFailed / 1_000.0;
                double consumeRateTotal = cumulativeStats.totalMessagesReceived / elapsedTotal;
                double consumeThroughputTotal = cumulativeStats.totalBytesReceived / BYTES_PER_MB / elapsedTotal;
                double consumeCountTotal = cumulativeStats.totalMessagesReceived / 1_000_000.0;
                double consumeSizeTotal = cumulativeStats.totalBytesReceived / BYTES_PER_GB;
                double produceLatencyMeanTotal = cumulativeStats.totalSendLatencyMicros.getMean() / MICROS_PER_MILLI;
                double produceLatency50thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(50) / MICROS_PER_MILLI;
                double produceLatency75thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(75) / MICROS_PER_MILLI;
                double produceLatency90thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(90) / MICROS_PER_MILLI;
                double produceLatency95thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(95) / MICROS_PER_MILLI;
                double produceLatency99thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99) / MICROS_PER_MILLI;
                double produceLatency999thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99.9) / MICROS_PER_MILLI;
                double produceLatency9999thTotal = cumulativeStats.totalSendLatencyMicros.getValueAtPercentile(99.99) / MICROS_PER_MILLI;
                double produceLatencyMaxTotal = cumulativeStats.totalSendLatencyMicros.getMaxValue() / MICROS_PER_MILLI;
                double endToEndLatencyMeanTotal = cumulativeStats.totalEndToEndLatencyMicros.getMean() / MICROS_PER_MILLI;
                double endToEndLatency50thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(50) / MICROS_PER_MILLI;
                double endToEndLatency75thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(75) / MICROS_PER_MILLI;
                double endToEndLatency90thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(90) / MICROS_PER_MILLI;
                double endToEndLatency95thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(95) / MICROS_PER_MILLI;
                double endToEndLatency99thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99) / MICROS_PER_MILLI;
                double endToEndLatency999thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99.9) / MICROS_PER_MILLI;
                double endToEndLatency9999thTotal = cumulativeStats.totalEndToEndLatencyMicros.getValueAtPercentile(99.99) / MICROS_PER_MILLI;
                double endToEndLatencyMaxTotal = cumulativeStats.totalEndToEndLatencyMicros.getMaxValue() / MICROS_PER_MILLI;

                LOGGER.info(SUMMARY_LOG_FORMAT,
                    RATE_FORMAT.format(produceRateTotal),
                    THROUGHPUT_FORMAT.format(produceThroughputTotal),
                    COUNT_FORMAT.format(produceCountTotal),
                    COUNT_FORMAT.format(produceSizeTotal),
                    COUNT_FORMAT.format(produceErrorTotal),
                    RATE_FORMAT.format(consumeRateTotal),
                    THROUGHPUT_FORMAT.format(consumeThroughputTotal),
                    COUNT_FORMAT.format(consumeCountTotal),
                    COUNT_FORMAT.format(consumeSizeTotal),
                    LATENCY_FORMAT.format(produceLatencyMeanTotal),
                    LATENCY_FORMAT.format(produceLatency50thTotal),
                    LATENCY_FORMAT.format(produceLatency75thTotal),
                    LATENCY_FORMAT.format(produceLatency90thTotal),
                    LATENCY_FORMAT.format(produceLatency95thTotal),
                    LATENCY_FORMAT.format(produceLatency99thTotal),
                    LATENCY_FORMAT.format(produceLatency999thTotal),
                    LATENCY_FORMAT.format(produceLatency9999thTotal),
                    LATENCY_FORMAT.format(produceLatencyMaxTotal),
                    LATENCY_FORMAT.format(endToEndLatencyMeanTotal),
                    LATENCY_FORMAT.format(endToEndLatency50thTotal),
                    LATENCY_FORMAT.format(endToEndLatency75thTotal),
                    LATENCY_FORMAT.format(endToEndLatency90thTotal),
                    LATENCY_FORMAT.format(endToEndLatency95thTotal),
                    LATENCY_FORMAT.format(endToEndLatency99thTotal),
                    LATENCY_FORMAT.format(endToEndLatency999thTotal),
                    LATENCY_FORMAT.format(endToEndLatency9999thTotal),
                    LATENCY_FORMAT.format(endToEndLatencyMaxTotal)
                );

                break;
            }

            last = now;
        }
        // TODO return the summary
        return null;
    }

    @FunctionalInterface
    public interface StopCondition {
        /**
         * Check if the loop in {@link PrintUtil#printAndCollectStats} should stop.
         *
         * @param startNanos start time in nanoseconds
         * @param nowNanos   current time in nanoseconds
         * @return true if the loop should stop
         */
        boolean shouldStop(long startNanos, long nowNanos);
    }
}
