/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.otel;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

public class DeltaHistogram {
    private final Summarize summarize;
    private long count;
    private double sum;
    private double deltaMean;

    public DeltaHistogram(Histogram histogram) {
        this.summarize = new Summarize(histogram);
    }

    public DeltaHistogram(Timer timer) {
        this.summarize = new Summarize(timer);
    }

    private void update() {
        if (count == 0) {
            updateState(summarize.count(), summarize.sum());
            deltaMean = summarize.mean();
        } else {
            long deltaCount = summarize.count() - count;
            if (deltaCount <= 0) {
                updateState(summarize.count(), summarize.sum());
                deltaMean = 0;
                return;
            }
            double deltaSum = summarize.sum() - sum;
            deltaMean = deltaSum / deltaCount;
            updateState(summarize.count(), summarize.sum());
        }
    }

    private void updateState(long count, double sum) {
        this.count = count;
        this.sum = sum;
    }

    public double getDeltaMean() {
        update();
        return deltaMean;
    }

    public static class Summarize {
        private final Histogram histogram;
        private final Timer timer;

        public Summarize(Histogram histogram) {
            this.histogram = histogram;
            this.timer = null;
        }

        public Summarize(Timer timer) {
            this.histogram = null;
            this.timer = timer;
        }

        public long count() {
            if (histogram != null) {
                return histogram.count();
            } else if (timer != null) {
                return timer.count();
            }
            return 0;
        }

        public double sum() {
            if (histogram != null) {
                return histogram.sum();
            } else if (timer != null) {
                return timer.sum();
            }
            return 0;
        }

        public double mean() {
            if (histogram != null) {
                return histogram.mean();
            } else if (timer != null) {
                return timer.mean();
            }
            return 0;
        }
    }
}
