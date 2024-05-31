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

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsWrapperTest {

    @Test
    public void testConfigurableMetrics() {
        CounterMetric metric = new CounterMetric(new MetricsConfig(), Attributes.builder().put("extra", "v").build(),
                () -> Mockito.mock(LongCounter.class));
        Assertions.assertEquals(MetricsLevel.INFO, metric.metricsLevel);

        metric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, Attributes.builder().put("base", "v2").build()));
        Assertions.assertEquals(MetricsLevel.DEBUG, metric.metricsLevel);
        Assertions.assertEquals(Attributes.builder().put("extra", "v").put("base", "v2").build(), metric.attributes);

        HistogramMetric histogramMetric = new HistogramMetric(MetricsLevel.INFO, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertEquals(MetricsLevel.INFO, histogramMetric.metricsLevel);

        histogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, Attributes.builder().put("base", "v2").build()));
        Assertions.assertEquals(MetricsLevel.DEBUG, histogramMetric.metricsLevel);
        Assertions.assertEquals(Attributes.builder().put("extra", "v").put("base", "v2").build(), histogramMetric.attributes);

        histogramMetric = new HistogramMetric(MetricsLevel.INFO, new MetricsConfig());
        Assertions.assertEquals(5000L, histogramMetric.getDeltaHistogram().getSnapshotInterval());
        histogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.INFO, Attributes.empty(), 30000L));
        Assertions.assertEquals(30000L, histogramMetric.getDeltaHistogram().getSnapshotInterval());
    }

    @Test
    public void testMetricsLevel() {
        CounterMetric metric = new CounterMetric(new MetricsConfig(MetricsLevel.INFO, null), () -> Mockito.mock(LongCounter.class));
        Assertions.assertTrue(metric.add(MetricsLevel.INFO, 1));
        Assertions.assertFalse(metric.add(MetricsLevel.DEBUG, 1));
        metric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(metric.add(MetricsLevel.INFO, 1));
        Assertions.assertTrue(metric.add(MetricsLevel.DEBUG, 1));

        HistogramMetric histogramMetric = new HistogramMetric(MetricsLevel.INFO, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertTrue(histogramMetric.shouldRecord());
        histogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(histogramMetric.shouldRecord());
        histogramMetric = new HistogramMetric(MetricsLevel.DEBUG, new MetricsConfig(),
            Attributes.builder().put("extra", "v").build());
        Assertions.assertFalse(histogramMetric.shouldRecord());
        histogramMetric.onConfigChange(new MetricsConfig(MetricsLevel.DEBUG, null));
        Assertions.assertTrue(histogramMetric.shouldRecord());
    }

    @Test
    @Timeout(10)
    public void testDeltaHistogram() throws InterruptedException {
        DeltaHistogram histogram = new DeltaHistogram(1000);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger init = new AtomicInteger(1);
        int steps = 10000;
        mockLinearDataDist(histogram, init.get(), steps);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger(0);
        init.set(init.get() + count.get() * steps);
        count.incrementAndGet();
        executorService.schedule(() -> {
            init.set(init.get() + count.get() * steps);
            mockLinearDataDist(histogram, init.get(), steps);
            count.incrementAndGet();
            latch.countDown();
        }, 3000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(10000, histogram.cumulativeCount());
        // sum from 1 to 10000
        Assertions.assertEquals(50005000, histogram.cumulativeSum());
        Assertions.assertEquals(1, histogram.min());
        Assertions.assertEquals(10000, histogram.max());
        double p99 = histogram.p99();
        // estimated 99th percentile from 1 to 10000, expected: 9900, allowed precision: 500
        Assertions.assertTrue(p99 > 9400 && p99 < 10000);
        // estimated 50th percentile from 1 to 10000, expected: 5000, allowed precision: 500
        double p50 = histogram.p50();
        Assertions.assertTrue(p50 > 4500 && p50 < 5500);
        // check if snapshot is reused
        Assertions.assertEquals(p99, histogram.p99());
        Assertions.assertEquals(p50, histogram.p50());

        // snapshot interval < recoding interval
        Thread.sleep(1100);
        Assertions.assertEquals(0, histogram.p99());
        Assertions.assertEquals(0, histogram.p50());

        // next round
        latch.await();
        Assertions.assertEquals(20000, histogram.cumulativeCount());
        // sum from 1 to 20000
        Assertions.assertEquals(200010000, histogram.cumulativeSum());
        Assertions.assertEquals(0, histogram.min());
        Assertions.assertEquals(20000, histogram.max());
        p99 = histogram.p99();
        // estimated 99th percentile from 10001 to 20000, expected: 19900, allowed precision: 1000
        Assertions.assertEquals(19900, p99, 1000);
        // estimated 50th percentile from 10001 to 20000, expected: 15000, allowed precision: 1000
        p50 = histogram.p50();
        Assertions.assertEquals(15000, p50, 1000);
    }

    private void mockLinearDataDist(DeltaHistogram histogram, int init, int steps) {
        for (int i = init; i < init + steps; i++) {
            histogram.record(i);
        }
    }
}
