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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class RetryStormBackoffStateStoreTest {

    private static final RetryStormBackoffStateStore.BackoffKey KEY =
        new RetryStormBackoffStateStore.BackoffKey(ApiKeys.PRODUCE.id, "topic-0", "connection-1");

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
        assertTrue(second.reason().contains("delayable-transient"));
    }

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

    @Test
    public void testClearResetsState() {
        RetryStormBackoffStateStore store = newStore();

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L);
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1001L).delayed());

        store.clear(KEY);
        assertFalse(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1002L).delayed());
    }

    @Test
    public void testQuietTimeoutRecoversToCandidate() {
        RetryStormBackoffStateStore store = newStore();

        store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1000L);
        assertTrue(store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 1001L).delayed());

        RetryStormBackoffStateStore.StateDecision recovered =
            store.recordAndDecide(KEY, RetryStormBackoffStateStore.ErrorClassSet.delayableTransientError(), 3102L);
        assertFalse(recovered.delayed());
    }

    private static RetryStormBackoffStateStore newStore() {
        return new RetryStormBackoffStateStore(1000L, 1000L, 1000L, 100000);
    }
}
