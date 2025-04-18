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
