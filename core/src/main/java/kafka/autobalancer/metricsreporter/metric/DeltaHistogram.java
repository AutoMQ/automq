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

package kafka.autobalancer.metricsreporter.metric;

import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;

public class DeltaHistogram {
    private long count = 0;
    private long sum = 0;

    public double deltaRate(YammerHistogramMetric metric) {
        long deltaCount = metric.count() - count;
        if (deltaCount == 0) {
            return 0;
        }
        double deltaRate = (double) (metric.sum() - sum) / deltaCount;
        count = metric.count();
        sum = metric.sum();
        return deltaRate;
    }
}
