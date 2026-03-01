package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SessionTrackerTest {

    private SessionTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new SessionTracker(5, 20, 100L, 5000L);
    }

    @Test
    void testOnFetchCreatesSession() {
        tracker.onFetch(1, 100, 10000L, 0L);
        assertEquals(1, tracker.sessionCount());
    }

    @Test
    void testOnFetchMultipleSessions() {
        tracker.onFetch(1, 100, 10000L, 0L);
        tracker.onFetch(2, 200, 20000L, 0L);
        assertEquals(2, tracker.sessionCount());
    }

    @Test
    void testLookupFromActiveSession() {
        tracker.onFetch(1, 100, 10000L, 0L);
        tracker.onFetch(1, 300, 30000L, 1000L);

        SessionTracker.LookupResult result = tracker.lookup(200);
        assertEquals(20000L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testLookupSkipsIrrelevantSessions() {
        tracker.onFetch(1, 100, 10000L, 0L);
        tracker.onFetch(1, 200, 20000L, 1000L);
        tracker.onFetch(2, 500, 50000L, 0L);
        tracker.onFetch(2, 600, 60000L, 1000L);

        SessionTracker.LookupResult result = tracker.lookup(150);
        assertEquals(15000L, result.timestamp());
    }

    @Test
    void testOnSessionClosedEvictsWithoutArchive() {
        tracker.onFetch(1, 100, 10000L, 0L);
        tracker.onFetch(1, 200, 20000L, 1000L);
        assertEquals(1, tracker.sessionCount());

        tracker.onSessionClosed(1);
        assertEquals(0, tracker.sessionCount());

        SessionTracker.LookupResult result = tracker.lookup(150);
        assertEquals(-1, result.timestamp());
    }

    @Test
    void testMaxSessionsEvictsOldest() {
        for (int i = 1; i <= 6; i++) {
            tracker.onFetch(i, i * 100L, i * 10000L, i * 1000L);
        }
        assertTrue(tracker.sessionCount() <= 5);
    }

    @Test
    void testFlushAgedPoints() {
        tracker.onFetch(1, 100, 10000L, 0L);
        tracker.onFetch(1, 200, 20000L, 1000L);
        tracker.onFetch(1, 300, 30000L, 2000L);

        List<SessionBuffer.DataPoint> points = tracker.flushAgedPoints(20000L);
        assertFalse(points.isEmpty());
    }

    @Test
    void testLookupMiss() {
        SessionTracker.LookupResult result = tracker.lookup(100);
        assertEquals(-1, result.timestamp());
    }
}
