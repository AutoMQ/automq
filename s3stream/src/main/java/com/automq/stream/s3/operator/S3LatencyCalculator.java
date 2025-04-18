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

package com.automq.stream.s3.operator;

import org.HdrHistogram.ConcurrentHistogram;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class S3LatencyCalculator {
    private final long highestValue;
    private final ConcurrentSkipListMap<Long, ConcurrentHistogram> histogramMap = new ConcurrentSkipListMap<>();

    public S3LatencyCalculator(long[] buckets, long highestValue) {
        this.highestValue = highestValue;

        if (buckets == null || buckets.length == 0) {
            throw new IllegalArgumentException("Buckets should not be null or empty");
        }

        if (buckets[0] != 0) {
            histogramMap.put(0L, new ConcurrentHistogram(highestValue, 3));
        }

        for (long bucket : buckets) {
            if (bucket < 0) {
                throw new IllegalArgumentException("Bucket should be non-negative");
            }

            histogramMap.put(bucket, new ConcurrentHistogram(highestValue, 3));
        }
    }

    private ConcurrentHistogram findHistogram(long dataSizeInBytes) {
        Map.Entry<Long, ConcurrentHistogram> floorEntry = histogramMap.floorEntry(dataSizeInBytes);
        Map.Entry<Long, ConcurrentHistogram> ceilingEntry = histogramMap.ceilingEntry(dataSizeInBytes);
        if (ceilingEntry == null) {
            return floorEntry.getValue();
        }

        if (dataSizeInBytes - floorEntry.getKey() < ceilingEntry.getKey() - dataSizeInBytes) {
            return floorEntry.getValue();
        } else {
            return ceilingEntry.getValue();
        }
    }

    public void record(long dataSizeInBytes, long latency) {
        if (dataSizeInBytes < 0) {
            throw new IllegalArgumentException("Data size should be non-negative");
        }

        // Ignore the huge latency.
        if (latency > highestValue) {
            return;
        }

        findHistogram(dataSizeInBytes).recordValue(latency);
    }

    public long valueAtPercentile(long dataSizeInBytes, long percentile) {
        Map.Entry<Long, ConcurrentHistogram> floorEntry = histogramMap.floorEntry(dataSizeInBytes);
        Map.Entry<Long, ConcurrentHistogram> ceilingEntry = histogramMap.ceilingEntry(dataSizeInBytes);
        if (ceilingEntry == null) {
            return floorEntry.getValue().getValueAtPercentile(percentile);
        }

        long floorValue = floorEntry.getValue().getValueAtPercentile(percentile);
        long ceilingValue = ceilingEntry.getValue().getValueAtPercentile(percentile);
        float ratio = (dataSizeInBytes - floorEntry.getKey()) / (float) (ceilingEntry.getKey() - floorEntry.getKey());
        return (long) (floorValue + (ceilingValue - floorValue) * ratio);
    }
}
