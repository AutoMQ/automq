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

package com.automq.opentelemetry.yammer;

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
