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

package com.automq.stream.s3.operator;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.HdrHistogram.ConcurrentHistogram;

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
