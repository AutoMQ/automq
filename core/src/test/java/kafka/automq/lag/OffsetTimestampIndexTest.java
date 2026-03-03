package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetTimestampIndexTest {

    private OffsetTimestampIndex index;

    @BeforeEach
    void setUp() {
        index = new OffsetTimestampIndex(
            5,
            new long[]{100L, 500L}, new int[]{4, 4},
            new long[]{50L, 200L}, new int[]{4, 4}
        );
    }

    @Test
    void testLeoIndexExactHit() {
        index.updateLeo(1000, 50000L);
        long ts = index.lookup(1000);
        assertEquals(50000L, ts);
    }

    @Test
    void testLeoIndexInterpolation() {
        index.updateLeo(100, 1000L);
        index.updateLeo(200, 1200L);
        long ts = index.lookup(150);
        assertEquals(1100L, ts);
    }

    @Test
    void testSessionExactHit() {
        index.onFetch(1, 500, 5000L, 0L);
        long ts = index.lookup(500);
        assertEquals(5000L, ts);
    }

    @Test
    void testSessionInterpolation() {
        index.onFetch(1, 100, 1000L, 0L);
        index.onFetch(1, 200, 2000L, 100L);
        long ts = index.lookup(150);
        assertEquals(1500L, ts);
    }

    @Test
    void testExactHitFromSessionReturnsEarly() {
        index.onFetch(1, 500, 5000L, 0L);
        index.updateLeo(1000, 10000L);

        long ts = index.lookup(500);
        assertEquals(5000L, ts, "Should return session exact hit");
    }

    @Test
    void testInterpolationPicksTightestBounds() {
        index.updateLeo(100, 1000L);
        index.updateLeo(400, 4200L);

        index.onFetch(1, 200, 2000L, 0L);
        index.onFetch(1, 300, 3000L, 100L);

        long ts = index.lookup(250);
        assertEquals(2500L, ts);
    }

    @Test
    void testFallbackFromSessionToLeo() {
        index.onFetch(1, 100, 1000L, 0L);
        index.onFetch(1, 200, 2000L, 100L);

        index.updateLeo(800, 8000L);
        index.updateLeo(1000, 10000L);

        long ts = index.lookup(900);
        assertEquals(9000L, ts);
    }

    @Test
    void testSessionClosed() {
        index.onFetch(1, 500, 5000L, 0L);
        index.onSessionClosed(1);
        long ts = index.lookup(500);
        assertEquals(-1L, ts, "Should miss after session closed");
    }

    @Test
    void testSessionSeekBackwardResetsSession() {
        index.onFetch(1, 500, 5000L, 0L);
        index.onFetch(1, 600, 6000L, 100L);

        index.onFetch(1, 200, 2000L, 200L);

        long ts = index.lookup(500);
        assertEquals(-1L, ts);
        ts = index.lookup(200);
        assertEquals(2000L, ts);
    }

    @Test
    void testMaxSessionsEvictsOldest() {
        for (int i = 1; i <= 6; i++) {
            index.onFetch(i, i * 100L, i * 1000L, i * 100L);
        }
        assertEquals(5, index.sessionCount(), "Should evict to maxSessions");
    }

    @Test
    void testLookupMissOnEmpty() {
        long ts = index.lookup(500);
        assertEquals(-1L, ts);
    }

    @Test
    void testNegativeLeoTimestampIgnored() {
        index.updateLeo(1000, -1L);
        long ts = index.lookup(1000);
        assertEquals(-1L, ts, "Negative timestamp should be ignored");
    }

    @Test
    void testOffsetRangePruning() {
        index.onFetch(1, 100, 1000L, 0L);
        index.onFetch(1, 200, 2000L, 100L);
        index.onFetch(2, 500, 5000L, 0L);
        index.onFetch(2, 600, 6000L, 100L);

        long ts = index.lookup(550);
        assertEquals(5500L, ts);
    }

    @Test
    void testLookupSkipsOutOfRangeSessionsInInterpolationPhase() {
        index.onFetch(1, 100, 1000L, 0L);
        index.onFetch(1, 200, 2000L, 100L);
        index.onFetch(2, 300, 3000L, 0L);
        index.onFetch(2, 400, 4000L, 100L);

        long ts = index.lookup(250);
        assertEquals(-1L, ts);
    }
}
