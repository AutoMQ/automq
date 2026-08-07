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
package kafka.log.streamaspect.reassignment;

import org.apache.kafka.common.Uuid;

import com.google.common.base.Ticker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies bounded, expiring, get-once partition handoff staging. */
@Tag("S3Unit")
public class PartitionHandoffCacheTest {
    private static final long MIB = 1L << 20;
    private static final long GIB = 1L << 30;

    /** The default cache floor applies to small heaps and scales in 2-GiB steps above it. */
    @Test
    public void testDefaultMaximumWeightScalesWithHeap() {
        assertEquals(192L * MIB, PartitionHandoffCache.defaultMaximumWeight(1L * GIB));
        assertEquals(192L * MIB, PartitionHandoffCache.defaultMaximumWeight(8L * GIB));
        assertEquals(256L * MIB, PartitionHandoffCache.defaultMaximumWeight(16L * GIB));
    }

    /**
     * Given an admitted handoff, lookup consumes the exact identity once.
     */
    @Test
    public void testTakeConsumesExactIdentityOnce() {
        PartitionHandoffCache cache = cache(1024, Ticker.systemTicker());
        PartitionHandoff handoff = handoff(0, 10, 8);

        cache.putAll(List.of(handoff));
        assertEquals(Optional.of(handoff), cache.take(handoff.key()));
        assertEquals(Optional.empty(), cache.take(handoff.key()));
        assertEquals(Optional.empty(), cache.take(new PartitionHandoff.Key(
            handoff.topicId(), handoff.partitionId(), handoff.endOffset() + 1)));
    }

    /**
     * Given an entry older than thirty seconds, lookup misses and consumes no stale state.
     */
    @Test
    public void testEntryExpiresAfterWrite() {
        TestTicker ticker = new TestTicker();
        PartitionHandoffCache cache = cache(1024, ticker);
        PartitionHandoff handoff = handoff(0, 10, 8);
        cache.putAll(List.of(handoff));

        ticker.advance(30, TimeUnit.SECONDS);

        assertEquals(Optional.empty(), cache.take(handoff.key()));
    }

    /**
     * Given admitted handoffs exceed the cache weight over time, the older entry is evicted.
     */
    @Test
    public void testWeightedCacheEvictsOlderEntry() {
        PartitionHandoff first = handoff(0, 10, 40);
        PartitionHandoff second = handoff(1, 20, 40);
        PartitionHandoffCache cache = cache(first.encodedSize() + second.encodedSize() - 1, Ticker.systemTicker());

        cache.putAll(List.of(first));
        cache.putAll(List.of(second));

        assertEquals(Optional.empty(), cache.take(first.key()));
        assertEquals(Optional.of(second), cache.take(second.key()));
    }

    /**
     * Given two decoded handoffs have the same identity, the later hint replaces the earlier one.
     */
    @Test
    public void testLaterDuplicateReplacesEarlierHandoff() {
        PartitionHandoff first = handoff(0, 10, 8);
        PartitionHandoff replacement = handoff(0, 10, 16);
        PartitionHandoffCache cache = cache(1024, Ticker.systemTicker());

        cache.putAll(List.of(first, replacement));

        assertEquals(Optional.of(replacement), cache.take(first.key()));
    }

    private static PartitionHandoffCache cache(long maximumWeight, Ticker ticker) {
        return new PartitionHandoffCache(maximumWeight, Duration.ofSeconds(30), ticker);
    }

    private static PartitionHandoff handoff(int partitionId, long endOffset, int valueSize) {
        return new PartitionHandoff(
            Uuid.fromString("FbrrdcfRQbqRKTp9h7B1YQ"),
            partitionId,
            new MetaStreamHandoff(endOffset, List.of(
                new MetaStreamHandoffRecord(3, ByteBuffer.wrap(new byte[valueSize])))));
    }

    private static final class TestTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        private void advance(long duration, TimeUnit unit) {
            nanos.addAndGet(unit.toNanos(duration));
        }
    }
}
