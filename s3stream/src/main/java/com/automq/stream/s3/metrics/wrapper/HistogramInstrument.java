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

package com.automq.stream.s3.metrics.wrapper;

import com.automq.stream.s3.metrics.S3StreamMetricsConstant;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import java.util.List;
import java.util.function.Supplier;

public class HistogramInstrument {
    private final ObservableLongGauge count;
    private final ObservableLongGauge sum;
//    private final ObservableDoubleGauge histP50Value;
//    private final ObservableDoubleGauge histP99Value;
    private final ObservableDoubleGauge histMaxValue;

    public HistogramInstrument(Meter meter, String name, String desc, String unit, Supplier<List<HistogramMetric>> histogramsSupplier) {
        this.count = meter.gaugeBuilder(name + S3StreamMetricsConstant.COUNT_METRIC_NAME_SUFFIX)
            .setDescription(desc + " (count)")
            .ofLongs()
            .buildWithCallback(result -> {
                List<HistogramMetric> histograms = histogramsSupplier.get();
                histograms.forEach(histogram -> {
                    if (histogram.shouldRecord()) {
                        result.record(histogram.count(), histogram.attributes);
                    }
                });
            });
        this.sum = meter.gaugeBuilder(name + S3StreamMetricsConstant.SUM_METRIC_NAME_SUFFIX)
            .setDescription(desc + " (sum)")
            .ofLongs()
            .setUnit(unit)
            .buildWithCallback(result -> {
                List<HistogramMetric> histograms = histogramsSupplier.get();
                histograms.forEach(histogram -> {
                    if (histogram.shouldRecord()) {
                        result.record(histogram.sum(), histogram.attributes);
                    }
                });
            });
//        this.histP50Value = meter.gaugeBuilder(name + S3StreamMetricsConstant.P50_METRIC_NAME_SUFFIX)
//            .setDescription(desc + " (50th percentile)")
//            .setUnit(unit)
//            .buildWithCallback(result -> {
//                List<HistogramMetric> histograms = histogramsSupplier.get();
//                histograms.forEach(histogram -> {
//                    if (histogram.shouldRecord()) {
//                        result.record(histogram.p50(), histogram.attributes);
//                    }
//                });
//            });
//        this.histP99Value = meter.gaugeBuilder(name + S3StreamMetricsConstant.P99_METRIC_NAME_SUFFIX)
//            .setDescription(desc + " (99th percentile)")
//            .setUnit(unit)
//            .buildWithCallback(result -> {
//                List<HistogramMetric> histograms = histogramsSupplier.get();
//                histograms.forEach(histogram -> {
//                    if (histogram.shouldRecord()) {
//                        result.record(histogram.p99(), histogram.attributes);
//                    }
//                });
//            });
        this.histMaxValue = meter.gaugeBuilder(name + S3StreamMetricsConstant.MAX_METRIC_NAME_SUFFIX)
            .setDescription(desc + " (max)")
            .setUnit(unit)
            .buildWithCallback(result -> {
                List<HistogramMetric> histograms = histogramsSupplier.get();
                histograms.forEach(histogram -> {
                    if (histogram.shouldRecord()) {
                        result.record(histogram.max(), histogram.attributes);
                    }
                });
            });
    }
}
