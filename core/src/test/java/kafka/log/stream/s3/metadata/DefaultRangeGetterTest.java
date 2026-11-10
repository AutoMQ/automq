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
package kafka.log.stream.s3.metadata;

import com.automq.stream.s3.metadata.StreamOffsetRange;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link DefaultRangeGetter.StreamIdBloomFilter} sizes its Guava-backed entries with a
 * reimplementation of {@code com.google.common.hash.BloomFilter#optimalNumOfBits} (package-
 * private there, so it cannot be called directly) — the same formula Guava's own
 * {@code BloomFilter.create} uses internally to size its bit array. This test asserts the
 * computed size against Guava's REAL method via reflection, not a second hand copy of the
 * formula, so a future Guava version — or a typo in the reimplementation — that changes the
 * actual allocation shows up here rather than silently drifting {@code StreamIdBloomFilter}'s
 * {@code maxBloomFilterSize}-based LRU eviction away from what is really allocated.
 */
@Tag("S3Unit")
public class DefaultRangeGetterTest {

    /**
     * Reflects into {@code com.google.common.hash.BloomFilter.optimalNumOfBits(long, double)}
     * (package-private, {@code @VisibleForTesting}) — the ground truth this test compares
     * against, not a duplicate of the formula under test.
     */
    private static long guavaOptimalNumOfBits(long n, double p) throws ReflectiveOperationException {
        Method method = Class.forName("com.google.common.hash.BloomFilter")
            .getDeclaredMethod("optimalNumOfBits", long.class, double.class);
        method.setAccessible(true);
        return (long) method.invoke(null, n, p);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 137, 1000, 10_000})
    void sizeInBytesMatchesGuavasOwnOptimalNumOfBits(int expectedInsertions) throws ReflectiveOperationException {
        DefaultRangeGetter.StreamIdBloomFilter streamIdBloomFilter =
            new DefaultRangeGetter.StreamIdBloomFilter(Long.MAX_VALUE /* no eviction pressure in this test */);

        List<StreamOffsetRange> streamOffsetRanges = new ArrayList<>();
        for (int i = 0; i < expectedInsertions; i++) {
            streamOffsetRanges.add(new StreamOffsetRange(i, 0, 100));
        }
        streamIdBloomFilter.update(1L /* objectId */, streamOffsetRanges);

        long realNumBits = guavaOptimalNumOfBits(expectedInsertions, DefaultRangeGetter.StreamIdBloomFilter.DEFAULT_FPP);
        long expectedBloomFilterBytes = (realNumBits + 7) / 8;
        // StreamIdBloomFilter charges Long.BYTES per cache entry on top of the filter's own
        // size — the objectId key's accounted weight, not part of the bit-array math itself.
        assertEquals(Long.BYTES + expectedBloomFilterBytes, streamIdBloomFilter.sizeInBytes());
    }

    @Test
    void zeroExpectedInsertionsIsTreatedAsOne() throws ReflectiveOperationException {
        // Guava's own create() clamps expectedInsertions == 0 to 1 (BloomFilter.java:428) rather
        // than reject it; StreamIdBloomFilter.update() can be called with an empty range list
        // (an SSO with no index entries), so the size estimate must apply the same clamp instead
        // of computing optimalNumOfBits(0, p), which is not a meaningful bit count.
        DefaultRangeGetter.StreamIdBloomFilter streamIdBloomFilter =
            new DefaultRangeGetter.StreamIdBloomFilter(Long.MAX_VALUE);

        streamIdBloomFilter.update(1L, List.of());

        long realNumBits = guavaOptimalNumOfBits(1, DefaultRangeGetter.StreamIdBloomFilter.DEFAULT_FPP);
        long expectedBloomFilterBytes = (realNumBits + 7) / 8;
        assertEquals(Long.BYTES + expectedBloomFilterBytes, streamIdBloomFilter.sizeInBytes());
    }
}
