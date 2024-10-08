/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.otel;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeltaHistogramTest {

    @Test
    public void testDeltaMean() {
        MetricsRegistry registry = new MetricsRegistry();
        Histogram histogram = registry.newHistogram(getClass(), "test-hist");
        DeltaHistogram deltaHistogram = new DeltaHistogram(histogram);
        for (int i = 0; i < 10; i++) {
            histogram.update(i);
        }
        Assertions.assertEquals(4.5, deltaHistogram.getDeltaMean());
        for (int i = 100; i < 200; i++) {
            histogram.update(i);
        }
        Assertions.assertEquals(149.5, deltaHistogram.getDeltaMean(), 0.0001);
        Assertions.assertEquals(136.31, histogram.mean(), 0.01);
    }
}
