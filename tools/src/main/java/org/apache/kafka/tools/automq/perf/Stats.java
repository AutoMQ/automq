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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class Stats {

    private static final long MAX_SEND_LATENCY = TimeUnit.SECONDS.toMicros(180);
    private static final long MAX_END_TO_END_LATENCY = TimeUnit.HOURS.toMicros(1);
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 5;

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messagesSendFailed = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final Recorder sendLatencyMicros = new LimitedRecorder(MAX_SEND_LATENCY);

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final Recorder endToEndLatencyMicros = new LimitedRecorder(MAX_END_TO_END_LATENCY);

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessagesSendFailed = new LongAdder();
    private final LongAdder totalBytesSent = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalBytesReceived = new LongAdder();
    private final Histogram totalSendLatencyMicros = new LimitedHistogram(MAX_SEND_LATENCY);
    private final Histogram totalEndToEndLatencyMicros = new LimitedHistogram(MAX_END_TO_END_LATENCY);

    /**
     * The max send time of all messages received.
     * Used to determine whether any consumer catches up to a specific time.
     */
    public final AtomicLong maxSendTimeNanos = new AtomicLong(0);

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
        maxSendTimeNanos.updateAndGet(current -> Math.max(current, sendTimeNanos));
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
        periodStats.totalMessagesReceived = totalMessagesReceived.sum();
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
        public long nowNanos = System.nanoTime();
        public long messagesSent;
        public long messagesSendFailed;
        public long bytesSent;
        public Histogram sendLatencyMicros;

        public long messagesReceived;
        public long bytesReceived;
        public Histogram endToEndLatencyMicros;

        public long totalMessagesSent;
        public long totalMessagesReceived;
    }

    public static class CumulativeStats {
        public long nowNanos = System.nanoTime();
        public long totalMessagesSent;
        public long totalMessagesSendFailed;
        public long totalBytesSent;
        public long totalMessagesReceived;
        public long totalBytesReceived;
        public Histogram totalSendLatencyMicros;
        public Histogram totalEndToEndLatencyMicros;
    }

    private static class LimitedRecorder extends Recorder {
        private final long limit;

        public LimitedRecorder(long limit) {
            super(limit, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
            this.limit = limit;
        }

        @Override
        public void recordValue(long value) {
            if (value < 0) {
                return;
            }
            super.recordValue(Math.min(value, limit));
        }
    }

    private static class LimitedHistogram extends Histogram {
        private final long limit;

        public LimitedHistogram(long limit) {
            super(limit, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
            this.limit = limit;
        }

        @Override
        public void recordValue(long value) {
            if (value < 0) {
                return;
            }
            super.recordValue(Math.min(value, limit));
        }
    }
}
