/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.operations.S3ReadPurpose;
import com.automq.stream.s3.metrics.operations.S3RequestSource;
import com.automq.stream.s3.metrics.stats.OperationLatencyMetrics;
import com.automq.stream.s3.metrics.wrapper.DeltaHistogram;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Tag("S3Unit")
class S3RequestMetricsTest {
    /** Disabled dimensions share one series; all flag combinations retain GET/PUT totals, status and size bands. */
    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void registersOnlyEnabledDimensions(boolean sourceEnabled, boolean purposeEnabled) throws Exception {
        for (S3Operation operation : List.of(S3Operation.GET_OBJECT, S3Operation.PUT_OBJECT)) {
            S3RequestMetrics metrics = new S3RequestMetrics(operation, sourceEnabled, purposeEnabled);
            Map<Attributes, DeltaHistogram> uniqueHistograms = new HashMap<>();
            int observations = 0;
            for (boolean success : List.of(false, true)) {
                for (long size : List.of(64L, 32768L)) {
                    for (S3RequestSource source : S3RequestSource.values()) {
                        for (S3ReadPurpose purpose : S3ReadPurpose.values()) {
                            DeltaHistogram histogram = metrics.histogram(source, purpose, size, success);
                            AttributesBuilder expected = Attributes.builder()
                                .put("operation_type", "S3Request")
                                .put("operation_name", operation.getName())
                                .put("status", success ? "success" : "failed")
                                .put("size", size == 64 ? "16KB" : "32KB");
                            if (sourceEnabled) {
                                expected.put("source", source.name());
                                if (purposeEnabled && operation == S3Operation.GET_OBJECT && source == S3RequestSource.ROUTER_WAL) {
                                    expected.put("read_purpose", purpose.name());
                                }
                            }
                            Attributes attributes = attributes(histogram);
                            assertEquals(expected.build(), attributes);
                            DeltaHistogram previous = uniqueHistograms.putIfAbsent(attributes, histogram);
                            if (previous != null) {
                                assertSame(previous, histogram, "Identical exported labels must reuse one histogram");
                            }
                            histogram.record(10);
                            observations++;
                        }
                    }
                }
            }
            assertEquals(observations, uniqueHistograms.values().stream().mapToLong(DeltaHistogram::cumulativeCount).sum());
            assertEquals(observations * 10L, uniqueHistograms.values().stream().mapToLong(DeltaHistogram::cumulativeSum).sum());
        }
    }

    // Inspect the actual registration shared by count, sum, p50, p99 and max export callbacks.
    private static Attributes attributes(DeltaHistogram histogram) throws Exception {
        Field field = OperationLatencyMetrics.class.getDeclaredField("OPERATION_LATENCY");
        field.setAccessible(true);
        Object bundle = field.get(null);
        field = bundle.getClass().getDeclaredField("histograms");
        field.setAccessible(true);
        for (Object registration : (Collection<?>) field.get(bundle)) {
            Field value = registration.getClass().getDeclaredField("histogram");
            value.setAccessible(true);
            if (value.get(registration) == histogram) {
                Field attrs = registration.getClass().getDeclaredField("histogramAttrs");
                attrs.setAccessible(true);
                return (Attributes) attrs.get(registration);
            }
        }
        throw new AssertionError("Histogram was not registered");
    }
}
