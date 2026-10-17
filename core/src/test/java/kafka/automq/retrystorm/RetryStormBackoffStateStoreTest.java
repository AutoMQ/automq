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

package kafka.automq.retrystorm;

import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers per-resource retry storm state transitions and logical-time eviction.
 */
@Tag("S3Unit")
public class RetryStormBackoffStateStoreTest {

    private static final RetryStormBackoffStateStore.BackoffKey KEY =
        new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-0", "connection-1");

    /**
     * Given repeated delayable-transient errors, the first failure is immediate and the second delays.
     */
    @Test
    public void testDelayableTransientThreshold() {
        RetryStormBackoffStateStore store = newStore();

        RetryStormBackoffStateStore.StateDecision first =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L);
        assertFalse(first.delayed());

        RetryStormBackoffStateStore.StateDecision second =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1001L);
        assertTrue(second.delayed());
        assertEquals(1000L, second.delayMs());
        assertTrue((second.reasonMask() & RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT) != 0);
    }

    /**
     * Given a store-level default delay, the three-argument record call returns that delay when delaying.
     */
    @Test
    public void testDefaultDelayComesFromStoreConfiguration() {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(1000L, 1000L, 321L, 100000);

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L);
        RetryStormBackoffStateStore.StateDecision delayed =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1001L);

        assertTrue(delayed.delayed());
        assertEquals(321L, delayed.delayMs());
    }

    /**
     * Given protective-only errors, the sixth error in the sliding window delays and expired windows reset.
     */
    @Test
    public void testProtectiveThresholdAndWindowExpiration() {
        RetryStormBackoffStateStore store = newStore();

        for (int i = 0; i < 5; i++) {
            assertFalse(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.protectiveOnlyError(), 1000L + i).delayed());
        }
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.protectiveOnlyError(), 1005L).delayed());

        RetryStormBackoffStateStore.BackoffKey nextKey =
            new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-1", "connection-1");
        for (int i = 0; i < 5; i++) {
            assertFalse(store.recordAndDecide(nextKey, RetryStormBackoffStateStore.ErrorClassSet.protectiveOnlyError(), 1000L + i).delayed());
        }
        assertFalse(store.recordAndDecide(nextKey, RetryStormBackoffStateStore.ErrorClassSet.protectiveOnlyError(), 2101L).delayed());
    }

    /**
     * Given a delaying resource has been quiet past recovery timeout, the next error restarts candidate mode.
     */
    @Test
    public void testQuietTimeoutRecoversToCandidate() {
        RetryStormBackoffStateStore store = newStore();

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L);
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1001L).delayed());

        RetryStormBackoffStateStore.StateDecision recovered =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 3102L);
        assertFalse(recovered.delayed());
    }

    /**
     * Given a delayed response has not had quiet time after its send time, state remains delaying.
     */
    @Test
    public void testDelayingStateSurvivesDelayBeforeQuietTimeout() throws Exception {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(10L, 20L, 30L, 100000);

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 30L);
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1L, 30L).delayed());

        Thread.sleep(25L);
        RetryStormBackoffStateStore.StateDecision stillDelaying =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 40L, 30L);
        assertTrue(stillDelaying.delayed());
    }

    /**
     * Given dynamic max delay exceeds store defaults, logical eviction does not remove active delaying state.
     */
    @Test
    public void testDelayingStateSurvivesDynamicDelayLargerThanStoreDefault() {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(10L, 20L, 30L, 100000);

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 100L);
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1L, 100L).delayed());

        RetryStormBackoffStateStore.BackoffKey nextKey =
            new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-next", "connection-1");
        store.recordAndDecide(nextKey, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 80L, 100L);

        RetryStormBackoffStateStore.StateDecision stillDelaying =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 90L, 100L);
        assertTrue(stillDelaying.delayed());
    }

    /**
     * Given a quiet state exceeds cache retention, cache cleanup removes it without scanning on insertion.
     */
    @Test
    public void testCacheEvictsQuietStateAfterRetention() throws Exception {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(10L, 20L, 30L, 100000, 5L);

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 30L);
        assertEquals(1, store.trackedDimensions());

        Thread.sleep(80L);
        store.evictIfNeeded();

        assertEquals(0, store.trackedDimensions());
    }

    /**
     * Given more resources than configured capacity, cache maximum size bounds tracked dimensions.
     */
    @Test
    public void testCacheMaximumSizeBoundsTrackedDimensions() {
        int maxTrackedDimensions = 4;
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(1000L, 1000L, 1000L, maxTrackedDimensions);

        for (int i = 0; i < 16; i++) {
            RetryStormBackoffStateStore.BackoffKey key = new RetryStormBackoffStateStore.BackoffKey(
                ApiKeys.PRODUCE.id,
                "topic-" + i,
                "connection-1"
            );
            store.recordAndDecide(key, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L + i);
        }
        store.evictIfNeeded();

        assertTrue(store.trackedDimensions() <= maxTrackedDimensions);
    }

    /**
     * Given dimensions in candidate and delaying mode, only old-enough delaying states are returned for periodic logs.
     */
    @Test
    public void testDelayedSnapshotsReturnOnlyStatesDelayingLongerThanMinimumAge() {
        RetryStormBackoffStateStore store = newStore();
        RetryStormBackoffStateStore.BackoffKey oldDelaying =
            new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-old", "connection-1");
        RetryStormBackoffStateStore.BackoffKey youngDelaying =
            new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-young", "connection-1");
        RetryStormBackoffStateStore.BackoffKey candidate =
            new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-candidate", "connection-1");

        store.recordAndDecide(oldDelaying, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 1000L);
        store.recordAndDecide(oldDelaying, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1L, 1000L);
        store.recordAndDecide(youngDelaying, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 9000L, 1000L);
        store.recordAndDecide(youngDelaying, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 9001L, 1000L);
        store.recordAndDecide(candidate, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 1000L);

        List<RetryStormBackoffStateStore.DelayedStateSnapshot> snapshots =
            store.delayedSnapshots(11002L, 10000L, 10);

        assertEquals(1, snapshots.size());
        assertEquals(oldDelaying, snapshots.get(0).key());
        assertEquals(1L, snapshots.get(0).delayingSinceMs());
        assertTrue((snapshots.get(0).reasonMask() & RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT) != 0);
    }

    /**
     * Given an idle delaying state near cache expiration, snapshot scanning does not refresh cache access time.
     */
    @Test
    public void testDelayedSnapshotDoesNotTouchCacheAccessTime() throws Exception {
        RetryStormBackoffStateStore store = new RetryStormBackoffStateStore(10L, 0L, 30L, 100000, 200L);

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 0L, 30L);
        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1L, 30L);
        Thread.sleep(50L);
        assertEquals(1, store.delayedSnapshots(11002L, 10000L, 10).size());
        Thread.sleep(250L);
        store.evictIfNeeded();

        assertEquals(0, store.trackedDimensions());
    }

    private static RetryStormBackoffStateStore newStore() {
        return new RetryStormBackoffStateStore(1000L, 1000L, 1000L, 100000);
    }
}
