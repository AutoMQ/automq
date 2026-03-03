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

public class OffsetTimestampManagerTest {

    private OffsetTimestampIndex index;

    @BeforeEach
    void setUp() {
        index = new OffsetTimestampIndex(
            new OffsetTimestampIndex.IndexConfig(
                new OffsetTimestampIndex.SparseIndexConfig(java.util.List.of(
                    new OffsetTimestampIndex.LayerConfig(100L, 10),
                    new OffsetTimestampIndex.LayerConfig(500L, 10)
                )),
                new OffsetTimestampIndex.SparseIndexConfig(java.util.List.of(
                    new OffsetTimestampIndex.LayerConfig(50L, 10),
                    new OffsetTimestampIndex.LayerConfig(200L, 10)
                )),
                5
            )
        );
    }

    @Test
    void testLookupHitsIndexNoBackfill() {
        index.updateLeo(500, 5000L);

        int[] callCount = {0};
        Function<Long, Long> backfillFn = offset -> {
            callCount[0]++;
            return 9999L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);
        long ts = manager.lookup(500);

        assertEquals(5000L, ts);
        assertEquals(0, callCount[0], "Backfill should not be called on index hit");
    }

    @Test
    void testLookupMissTriggersBackfill() {
        Function<Long, Long> backfillFn = offset -> 42000L;

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);
        long ts = manager.lookup(500);
        assertEquals(42000L, ts);
    }

    @Test
    void testBackfillReturnsNegativeTreatedAsMiss() {
        Function<Long, Long> backfillFn = offset -> -1L;

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);
        long ts = manager.lookup(500);
        assertEquals(-1L, ts);
    }

    @Test
    void testBackfillFnThrowsExceptionReturnsMiss() {
        Function<Long, Long> backfillFn = offset -> {
            throw new RuntimeException("boom");
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);
        long ts = assertDoesNotThrow(() -> manager.lookup(500L));
        assertEquals(-1L, ts);
    }

    @Test
    void testNullBackfillFnReturnsMiss() {
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null);
        long ts = manager.lookup(500);
        assertEquals(-1L, ts);
    }

    @Test
    void testLookupBatchBackfillsAllMisses() {
        int[] callCount = {0};
        Function<Long, Long> backfillFn = offset -> {
            callCount[0]++;
            return offset * 10L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);

        Set<Long> offsets = new LinkedHashSet<>(Arrays.asList(100L, 200L, 300L, 400L));
        Map<Long, Long> results = manager.lookupBatch(offsets);

        assertEquals(4, results.size());
        assertEquals(4, callCount[0], "All misses should trigger backfill without per-batch limit");
        assertEquals(1000L, results.get(100L));
        assertEquals(2000L, results.get(200L));
        assertEquals(3000L, results.get(300L));
        assertEquals(4000L, results.get(400L));
    }

    @Test
    void testLookupBatchContinuesAfterBackfillException() {
        Function<Long, Long> backfillFn = offset -> {
            if (offset == 100L) {
                throw new RuntimeException("boom");
            }
            return offset * 10L;
        };

        OffsetTimestampManager manager = new OffsetTimestampManager(index, backfillFn);
        Set<Long> offsets = new LinkedHashSet<>(Arrays.asList(100L, 200L));
        Map<Long, Long> results = assertDoesNotThrow(() -> manager.lookupBatch(offsets));

        assertEquals(-1L, results.get(100L), "Failed backfill should be miss");
        assertEquals(2000L, results.get(200L), "Successful backfill should return timestamp");
    }

    @Test
    void testDelegatesUpdateLeoToIndex() {
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null);
        manager.updateLeo(1000, 50000L);
        long ts = manager.lookup(1000);
        assertEquals(50000L, ts);
    }

    @Test
    void testDelegatesOnFetchToIndex() {
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null);
        manager.onFetch(1, 500, 5000L, 0L);
        long ts = manager.lookup(500);
        assertEquals(5000L, ts);
    }

    @Test
    void testDelegatesOnSessionClosedToIndex() {
        OffsetTimestampManager manager = new OffsetTimestampManager(index, null);
        manager.onFetch(1, 500, 5000L, 0L);
        manager.onSessionClosed(1);
        long ts = manager.lookup(500);
        assertEquals(-1L, ts);
    }
}
