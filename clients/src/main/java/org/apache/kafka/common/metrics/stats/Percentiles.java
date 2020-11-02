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
package org.apache.kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A compound stat that reports one or more percentiles
 */
public class Percentiles extends SampledStat implements CompoundStat {

    private final Logger log = LoggerFactory.getLogger(Percentiles.class);

    public enum BucketSizing {
        CONSTANT, LINEAR
    }

    private final int buckets;
    private final Percentile[] percentiles;
    private final BinScheme binScheme;
    private final double min;
    private final double max;

    public Percentiles(int sizeInBytes, double max, BucketSizing bucketing, Percentile... percentiles) {
        this(sizeInBytes, 0.0, max, bucketing, percentiles);
    }

    public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing, Percentile... percentiles) {
        super(0.0);
        this.percentiles = percentiles;
        this.buckets = sizeInBytes / 4;
        this.min = min;
        this.max = max;
        if (bucketing == BucketSizing.CONSTANT) {
            this.binScheme = new ConstantBinScheme(buckets, min, max);
        } else if (bucketing == BucketSizing.LINEAR) {
            if (min != 0.0d)
                throw new IllegalArgumentException("Linear bucket sizing requires min to be 0.0.");
            this.binScheme = new LinearBinScheme(buckets, max);
        } else {
            throw new IllegalArgumentException("Unknown bucket type: " + bucketing);
        }
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<>(this.percentiles.length);
        for (Percentile percentile : this.percentiles) {
            final double pct = percentile.percentile();
            ms.add(new NamedMeasurable(
                percentile.name(),
                (config, now) -> value(config, now, pct / 100.0))
            );
        }
        return ms;
    }

    public double value(MetricConfig config, long now, double quantile) {
        purgeObsoleteSamples(config, now);
        float count = 0.0f;
        for (Sample sample : this.samples)
            count += sample.eventCount;
        if (count == 0.0f)
            return Double.NaN;
        float sum = 0.0f;
        float quant = (float) quantile;
        for (int b = 0; b < buckets; b++) {
            for (Sample s : this.samples) {
                HistogramSample sample = (HistogramSample) s;
                float[] hist = sample.histogram.counts();
                sum += hist[b];
                if (sum / count > quant)
                    return binScheme.fromBin(b);
            }
        }
        return Double.POSITIVE_INFINITY;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        return value(config, now, 0.5);
    }

    @Override
    protected HistogramSample newSample(long timeMs) {
        return new HistogramSample(this.binScheme, timeMs);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
        final double boundedValue;
        if (value > max) {
            log.debug("Received value {} which is greater than max recordable value {}, will be pinned to the max value",
                     value, max);
            boundedValue = max;
        } else if (value < min) {
            log.debug("Received value {} which is less than min recordable value {}, will be pinned to the min value",
                     value, min);
            boundedValue = min;
        } else {
            boundedValue = value;
        }

        HistogramSample hist = (HistogramSample) sample;
        hist.histogram.record(boundedValue);
    }

    private static class HistogramSample extends SampledStat.Sample {
        private final Histogram histogram;

        private HistogramSample(BinScheme scheme, long now) {
            super(0.0, now);
            this.histogram = new Histogram(scheme);
        }

        @Override
        public void reset(long now) {
            super.reset(now);
            this.histogram.clear();
        }
    }

}
