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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public class Stats {

    private static final long HIGHEST_TRACKABLE_VALUE_MICROS = TimeUnit.SECONDS.toMicros(180);
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 5;

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder messagesSendFailed = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private static final Recorder sendLatencyMicros = new Recorder(HIGHEST_TRACKABLE_VALUE_MICROS, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

    private static final LongAdder messagesReceived = new LongAdder();
    private static final LongAdder bytesReceived = new LongAdder();
    private static final Recorder endToEndLatencyMicros = new Recorder(HIGHEST_TRACKABLE_VALUE_MICROS, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalMessagesSendFailed = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();
    private static final LongAdder totalMessagesReceived = new LongAdder();
    private static final LongAdder totalBytesReceived = new LongAdder();
    private static final Histogram totalSendLatencyMicros = new Histogram(HIGHEST_TRACKABLE_VALUE_MICROS, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    private static final Histogram totalEndToEndLatencyMicros = new Histogram(HIGHEST_TRACKABLE_VALUE_MICROS, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

    public void messageSent(long bytes, long sendTimeNanos) {
        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos);
        messagesSent.increment();
        bytesSent.add(bytes);
        sendLatencyMicros.recordValue(latencyMicros);
        totalMessagesSent.increment();
        totalBytesSent.add(bytes);
        totalSendLatencyMicros.recordValue(latencyMicros);
    }

    public void messageFailed() {
        messagesSendFailed.increment();
        totalMessagesSendFailed.increment();
    }

    public void messageReceived(long bytes, long sendTimeNanos) {
        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos);
        messagesReceived.increment();
        bytesReceived.add(bytes);
        endToEndLatencyMicros.recordValue(latencyMicros);
        totalMessagesReceived.increment();
        totalBytesReceived.add(bytes);
        totalEndToEndLatencyMicros.recordValue(latencyMicros);
    }

    /**
     * Get period stats.
     * Note: This method resets the period counters.
     */
    public PeriodStats toPeriodStats() {
        PeriodStats periodStats = new PeriodStats();
        periodStats.messagesSent = messagesSent.sumThenReset();
        periodStats.messagesSendFailed = messagesSendFailed.sumThenReset();
        periodStats.bytesSent = bytesSent.sumThenReset();
        periodStats.sendLatencyMicros = sendLatencyMicros.getIntervalHistogram();
        periodStats.messagesReceived = messagesReceived.sumThenReset();
        periodStats.bytesReceived = bytesReceived.sumThenReset();
        periodStats.endToEndLatencyMicros = endToEndLatencyMicros.getIntervalHistogram();
        periodStats.totalMessagesSent = totalMessagesSent.sum();
        periodStats.totalMessagesSendFailed = totalMessagesSendFailed.sum();
        periodStats.totalBytesSent = totalBytesSent.sum();
        periodStats.totalMessagesReceived = totalMessagesReceived.sum();
        periodStats.totalBytesReceived = totalBytesReceived.sum();
        return periodStats;
    }

    /**
     * Get cumulative stats.
     * Note: There is no side effect on any counters.
     */
    public CumulativeStats toCumulativeStats() {
        CumulativeStats cumulativeStats = new CumulativeStats();
        cumulativeStats.totalMessagesSent = totalMessagesSent.sum();
        cumulativeStats.totalMessagesSendFailed = totalMessagesSendFailed.sum();
        cumulativeStats.totalBytesSent = totalBytesSent.sum();
        cumulativeStats.totalMessagesReceived = totalMessagesReceived.sum();
        cumulativeStats.totalBytesReceived = totalBytesReceived.sum();
        cumulativeStats.totalSendLatencyMicros = totalSendLatencyMicros.copy();
        cumulativeStats.totalEndToEndLatencyMicros = totalEndToEndLatencyMicros.copy();
        return cumulativeStats;
    }

    /**
     * Reset all counters and return the cumulative stats.
     */
    public CumulativeStats reset() {
        CumulativeStats cumulativeStats = new CumulativeStats();
        messagesSent.reset();
        messagesSendFailed.reset();
        bytesSent.reset();
        sendLatencyMicros.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        endToEndLatencyMicros.reset();
        cumulativeStats.totalMessagesSent = totalMessagesSent.sumThenReset();
        cumulativeStats.totalMessagesSendFailed = totalMessagesSendFailed.sumThenReset();
        cumulativeStats.totalBytesSent = totalBytesSent.sumThenReset();
        cumulativeStats.totalMessagesReceived = totalMessagesReceived.sumThenReset();
        cumulativeStats.totalBytesReceived = totalBytesReceived.sumThenReset();
        cumulativeStats.totalSendLatencyMicros = totalSendLatencyMicros.copy();
        totalSendLatencyMicros.reset();
        cumulativeStats.totalEndToEndLatencyMicros = totalEndToEndLatencyMicros.copy();
        totalEndToEndLatencyMicros.reset();
        return cumulativeStats;
    }

    public static class PeriodStats {
        public long messagesSent;
        public long messagesSendFailed;
        public long bytesSent;
        public Histogram sendLatencyMicros;

        public long messagesReceived;
        public long bytesReceived;
        public Histogram endToEndLatencyMicros;

        public long totalMessagesSent;
        public long totalMessagesSendFailed;
        public long totalBytesSent;
        public long totalMessagesReceived;
        public long totalBytesReceived;
    }

    public static class CumulativeStats {
        public long totalMessagesSent;
        public long totalMessagesSendFailed;
        public long totalBytesSent;
        public long totalMessagesReceived;
        public long totalBytesReceived;
        public Histogram totalSendLatencyMicros;
        public Histogram totalEndToEndLatencyMicros;
    }
}
