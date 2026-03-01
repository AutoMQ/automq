package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetTimestampIndexTest {

    private OffsetTimestampIndex index;

    private static final long HOUR = 3600_000L;

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
    void testFetchHitFromSessionTracker() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);
        index.onFetch(1, 500, now - 500_000L, now);
        index.onFetch(1, 600, now - 400_000L, now + 1000L);

        OffsetTimestampIndex.LookupResult r = index.lookup(550);
        assertNotEquals(-1L, r.timestamp());
        assertFalse(r.precise());
    }

    @Test
    void testFallbackToGlobalIndex() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);
        index.addPointToGlobal(200, now - 200_000L, now);
        index.addPointToGlobal(400, now - 100_000L, now);

        OffsetTimestampIndex.LookupResult r = index.lookup(300);
        assertNotEquals(-1L, r.timestamp());
    }

    @Test
    void testLeoSampleGoesToGlobal() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);

        OffsetTimestampIndex.LookupResult r = index.lookup(1000);
        assertEquals(now, r.timestamp());
        assertTrue(r.precise());
    }

    @Test
    void testSessionClosedEvictsData() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);
        index.onFetch(1, 500, now - 500_000L, now);
        index.onFetch(1, 600, now - 400_000L, now + 1000L);

        index.onSessionClosed(1);

        OffsetTimestampIndex.LookupResult r = index.lookup(550);
        assertNotNull(r);
    }

    @Test
    void testPeriodicMaintenance() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);
        index.onFetch(1, 500, now - 500_000L, now);
        index.onFetch(1, 600, now - 400_000L, now + 1000L);

        index.periodicMaintenance(now + 20000L);

        assertTrue(index.globalTotalSize() > 0);
    }

    @Test
    void testBackfillAddsToGlobal() {
        long now = 100 * HOUR;
        index.updateLeo(1000, now, 0, now);
        index.addPointToGlobal(300, now - 300_000L, now);

        OffsetTimestampIndex.LookupResult r = index.lookup(300);
        assertEquals(now - 300_000L, r.timestamp());
        assertTrue(r.precise());
    }
}
