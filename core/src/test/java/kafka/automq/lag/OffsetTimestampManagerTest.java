package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetTimestampManagerTest {

    private static final long HOUR = 3600_000L;
    private OffsetTimestampIndex index;

    @BeforeEach
    void setUp() {
        index = new OffsetTimestampIndex(
            20, 10, 10,
            HOUR, 6 * HOUR,
            5, 20,
            100L, 5000L
        );
    }

    @Test
    void testLookupHitsIndex_NoBackfill() {
        long now = 100 * HOUR;
        index.addPointToGlobal(500, now - 500_000L, now);

        int[] backfillCallCount = {0};
        Function<Long, Long> backfillFn = offset -> {
            backfillCallCount[0]++;
            return now - offset * 1000L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);
        OffsetTimestampIndex.LookupResult result = manager.lookup(500);

        assertEquals(now - 500_000L, result.timestamp());
        assertTrue(result.precise());
        assertEquals(0, backfillCallCount[0], "Backfill should not be called on index hit");
    }

    @Test
    void testLookupMissTriggersBackfill() {
        long now = 100 * HOUR;

        Function<Long, Long> backfillFn = offset -> now - offset * 1000L;

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);
        OffsetTimestampIndex.LookupResult result = manager.lookup(500);

        assertEquals(now - 500_000L, result.timestamp());
        assertTrue(result.precise());
    }

    @Test
    void testBackfillResultCachedInIndex() {
        long now = 100 * HOUR;
        int[] backfillCallCount = {0};
        Function<Long, Long> backfillFn = offset -> {
            backfillCallCount[0]++;
            return now - offset * 1000L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);

        manager.lookup(500);
        assertEquals(1, backfillCallCount[0]);

        manager.lookup(500);
        assertEquals(1, backfillCallCount[0], "Second lookup should hit index cache");
    }

    @Test
    void testLookupBatchRespectsMaxBackfills() {
        long now = 100 * HOUR;
        int[] backfillCallCount = {0};
        Function<Long, Long> backfillFn = offset -> {
            backfillCallCount[0]++;
            return now - offset * 1000L;
        };

        int maxBackfills = 2;
        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, maxBackfills);

        Set<Long> offsets = Set.of(100L, 200L, 300L, 400L);
        Map<Long, OffsetTimestampIndex.LookupResult> results = manager.lookupBatch(offsets);

        long preciseCount = results.values().stream()
            .filter(OffsetTimestampIndex.LookupResult::precise)
            .count();
        assertTrue(backfillCallCount[0] <= maxBackfills, "Backfill call count should be capped");
        assertEquals(backfillCallCount[0], preciseCount, "Only backfilled points should be precise");
    }

    @Test
    void testNullBackfillFnSkipsBackfill() {
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null, 10);
        OffsetTimestampIndex.LookupResult result = manager.lookup(500);

        assertEquals(-1L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testBackfillReturnsNegative_TreatedAsMiss() {
        Function<Long, Long> backfillFn = offset -> -1L;

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);
        OffsetTimestampIndex.LookupResult result = manager.lookup(500);

        assertEquals(-1L, result.timestamp());
    }

    @Test
    void testDelegatesOnFetchToIndex() {
        long now = 100 * HOUR;
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null, 10);

        manager.onFetch(1, 500, now - 500_000L, now);
        manager.onFetch(1, 600, now - 400_000L, now + 1000L);

        OffsetTimestampIndex.LookupResult result = manager.lookup(550);
        assertTrue(result.timestamp() >= 0, "Should find interpolated result from session");
    }

    @Test
    void testDelegatesUpdateLeoToIndex() {
        long now = 100 * HOUR;
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null, 10);

        manager.updateLeo(1000, now, 0, now);

        OffsetTimestampIndex.LookupResult result = manager.lookup(1000);
        assertEquals(now, result.timestamp());
        assertTrue(result.precise());
    }

    @Test
    void testBackfillFnThrowsException_ReturnsMiss() {
        Function<Long, Long> backfillFn = offset -> {
            throw new RuntimeException("boom");
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);
        OffsetTimestampIndex.LookupResult result = assertDoesNotThrow(() -> manager.lookup(500L));

        assertEquals(-1L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testLookupBatchContinuesAfterBackfillException() {
        long now = 100 * HOUR;
        Function<Long, Long> backfillFn = offset -> {
            if (offset == 1000L) {
                throw new RuntimeException("boom");
            }
            return now - offset * 1000L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn, 10);

        Set<Long> offsets = new LinkedHashSet<>(Arrays.asList(1000L, 100L, 50L));
        Map<Long, OffsetTimestampIndex.LookupResult> results =
            assertDoesNotThrow(() -> manager.lookupBatch(offsets));

        assertEquals(-1L, results.get(1000L).timestamp());
        assertFalse(results.get(1000L).precise());
        assertEquals(now - 100_000L, results.get(100L).timestamp());
        assertTrue(results.get(100L).precise());
        assertEquals(now - 50_000L, results.get(50L).timestamp());
        assertTrue(results.get(50L).precise());
    }
}
