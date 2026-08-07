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

import com.automq.stream.utils.Systems;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A bounded, expiring target-side staging cache for optional partition handoff hints.
 * Received hints are always inserted; weight or expiry eviction is observed later as a normal cache miss. Each broker
 * lifecycle owns one cache, and {@link #clear()} discards its process-local contents.
 */
public final class PartitionHandoffCache {
    private static final long MIB = 1L << 20;
    private static final long HEAP_BYTES_PER_WEIGHT_STEP = 2L << 30;
    private static final long WEIGHT_PER_HEAP_STEP = 32L << 20;
    public static final long DEFAULT_MAXIMUM_WEIGHT = defaultMaximumWeight(Systems.HEAP_MEMORY_SIZE);
    public static final Duration DEFAULT_EXPIRY = Duration.ofSeconds(30);

    private final Cache<PartitionHandoff.Key, PartitionHandoff> cache;

    /**
     * Creates a cache with a heap-scaled default weight and 30-second expiry after write.
     */
    public PartitionHandoffCache() {
        this(DEFAULT_MAXIMUM_WEIGHT, DEFAULT_EXPIRY, Ticker.systemTicker());
    }

    /**
     * Creates a cache with explicit bounds and clock, primarily for embedding and deterministic expiry tests.
     *
     * @param maximumWeight maximum encoded handoff bytes
     * @param expiry expiry after write
     * @param ticker cache clock
     */
    public PartitionHandoffCache(long maximumWeight, Duration expiry, Ticker ticker) {
        if (maximumWeight <= 0) {
            throw new IllegalArgumentException("maximumWeight must be positive");
        }
        if (expiry.isZero() || expiry.isNegative()) {
            throw new IllegalArgumentException("expiry must be positive");
        }
        this.cache = CacheBuilder.newBuilder()
            .concurrencyLevel(1)
            .maximumWeight(maximumWeight)
            .weigher((Weigher<PartitionHandoff.Key, PartitionHandoff>)
                (key, handoff) -> handoff.encodedSize())
            .expireAfterWrite(expiry.toNanos(), TimeUnit.NANOSECONDS)
            .ticker(ticker)
            .build();
    }

    /**
     * Inserts every decoded handoff. Weight enforcement may immediately evict any entry.
     *
     * @param handoffs received handoffs
     */
    public void putAll(Collection<PartitionHandoff> handoffs) {
        for (PartitionHandoff handoff : handoffs) {
            cache.put(handoff.key(), handoff);
        }
    }

    /**
     * Removes and returns the exact handoff entry, if it is still present.
     *
     * @param key exact topic, partition, and handoff-end identity
     * @return the consumed handoff or empty on miss, expiry, or eviction
     */
    public Optional<PartitionHandoff> take(PartitionHandoff.Key key) {
        return Optional.ofNullable(cache.asMap().remove(key));
    }

    /**
     * Discards all process-local hints when the owning broker lifecycle stops.
     */
    public void clear() {
        cache.invalidateAll();
    }

    static long defaultMaximumWeight(long heapMemorySize) {
        return Math.max(192L * MIB,
            heapMemorySize / HEAP_BYTES_PER_WEIGHT_STEP * WEIGHT_PER_HEAP_STEP);
    }
}
