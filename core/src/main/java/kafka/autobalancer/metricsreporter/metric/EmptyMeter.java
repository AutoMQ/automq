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

package kafka.autobalancer.metricsreporter.metric;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;

import java.util.concurrent.TimeUnit;

public class EmptyMeter implements Metered {
    @Override
    public TimeUnit rateUnit() {
        return null;
    }

    @Override
    public String eventType() {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public double fifteenMinuteRate() {
        return 0;
    }

    @Override
    public double fiveMinuteRate() {
        return 0;
    }

    @Override
    public double meanRate() {
        return 0;
    }

    @Override
    public double oneMinuteRate() {
        return 0;
    }

    @Override
    public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context) throws Exception {
        processor.processMeter(name, this, context);
    }
}
