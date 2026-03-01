package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlobalSparseIndexTest {

    private GlobalSparseIndex index;

    private static final long HOUR = 3600_000L;
    private static final long BASE_TIME = 100 * HOUR;

    @BeforeEach
    void setUp() {
        index = new GlobalSparseIndex(10, 6, 6, HOUR, 6 * HOUR);
    }

    @Test
    void testAddPointAndLookupExact() {
        index.addPoint(500, 50000L);
        GlobalSparseIndex.LookupResult result = index.lookup(500);
        assertEquals(50000L, result.timestamp());
        assertTrue(result.precise());
    }

    @Test
    void testLookupInterpolation() {
        index.addPoint(400, 40000L);
        index.addPoint(600, 60000L);
        GlobalSparseIndex.LookupResult result = index.lookup(500);
        assertEquals(50000L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testLookupMiss() {
        GlobalSparseIndex.LookupResult result = index.lookup(500);
        assertEquals(-1L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testL0AsPool_AllPointsEnterL0() {
        long now = BASE_TIME;
        index.addPoint(100, now - 30 * HOUR);
        index.addPoint(500, now - 3 * HOUR);
        index.addPoint(900, now - 10_000L);

        assertEquals(3, index.l0Size());
        assertEquals(0, index.l1Size());
        assertEquals(0, index.l2Size());
    }

    @Test
    void testCompactDemotesByTimestampAge() {
        long now = BASE_TIME;
        for (int i = 0; i <= 10; i++) {
            long age = i * HOUR;
            index.addPoint(i * 100L, now - age);
        }

        assertEquals(2, index.l0Size(), "Only <=1h points should remain in L0 after age demotion");
        assertTrue(index.l1Size() + index.l2Size() > 0, "Aged points should be demoted to L1/L2");
        assertEquals(11, index.l0Size() + index.l1Size() + index.l2Size(),
            "Total points should be preserved");

        GlobalSparseIndex.LookupResult r = index.lookup(500);
        assertNotEquals(-1L, r.timestamp());
    }

    @Test
    void testCompactDemotesL1ToL2WhenAged() {
        long now = BASE_TIME;
        for (int i = 0; i < 8; i++) {
            index.addPoint(i * 100L, now - 3 * HOUR - i * 1000L);
        }
        for (int i = 8; i <= 11; i++) {
            index.addPoint(i * 100L, now - 10_000L);
        }

        assertTrue(index.l1Size() > 0, "L1 should have demoted points before future compact");

        long futureNow = now + 4 * HOUR;
        index.compact(futureNow);
        assertTrue(index.l2Size() > 0, "L2 should receive aged L1 points after future compact");
        assertEquals(7, index.totalSize(), "Sampling should cap retained points across layers");
    }

    @Test
    void testDiscardBelowLogStartOffset() {
        index.addPoint(100, 10000L);
        index.addPoint(500, 50000L);
        index.addPoint(900, 90000L);

        index.setLogStartOffset(200);
        index.compact(BASE_TIME);

        GlobalSparseIndex.LookupResult r2 = index.lookup(500);
        assertEquals(50000L, r2.timestamp());
    }

    @Test
    void testHalveSamplePreservesFirstAndLast() {
        long now = BASE_TIME;
        for (int i = 0; i <= 20; i++) {
            index.addPoint(i * 10L, now - i * 1000L);
        }

        GlobalSparseIndex.LookupResult first = index.lookup(0);
        assertNotEquals(-1L, first.timestamp());
        GlobalSparseIndex.LookupResult last = index.lookup(200);
        assertNotEquals(-1L, last.timestamp());
    }
}
