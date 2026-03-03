package kafka.automq.lag;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-partition offset to timestamp index combining LEO sampling and fetch sessions.
 */
public class OffsetTimestampIndex {

    public static final long MISS = -1L;

    static final long[] DEFAULT_LEO_INTERVALS = {60_000L, 600_000L, 14_400_000L};
    static final int[] DEFAULT_LEO_CAPACITIES = {10, 18, 18};
    static final long[] DEFAULT_SESSION_INTERVALS = {10_000L, 60_000L};
    static final int[] DEFAULT_SESSION_CAPACITIES = {12, 10};
    static final int DEFAULT_MAX_SESSIONS = 20;

    private final LayeredSparseIndex leoIndex;
    private final Map<Integer, SessionEntry> sessions = new HashMap<>();
    private final int maxSessions;
    private final long[] sessionIntervals;
    private final int[] sessionCapacities;

    public OffsetTimestampIndex() {
        this(
            DEFAULT_MAX_SESSIONS,
            DEFAULT_LEO_INTERVALS,
            DEFAULT_LEO_CAPACITIES,
            DEFAULT_SESSION_INTERVALS,
            DEFAULT_SESSION_CAPACITIES
        );
    }

    public OffsetTimestampIndex(int maxSessions,
                                long[] leoIntervals,
                                int[] leoCapacities,
                                long[] sessionIntervals,
                                int[] sessionCapacities) {
        this.maxSessions = maxSessions;
        this.leoIndex = new LayeredSparseIndex(leoIntervals, leoCapacities);
        this.sessionIntervals = sessionIntervals.clone();
        this.sessionCapacities = sessionCapacities.clone();
    }

    /**
     * Kept for compatibility while Scala call sites are migrated.
     */
    @Deprecated
    public OffsetTimestampIndex(int l0Max, int l1Max, int l2Max,
                                long l0BoundaryMs, long l1BoundaryMs,
                                int maxSessions, int bufferMaxSize,
                                long minTimeGapMs, long archiveAgeMs) {
        this();
    }

    public synchronized void updateLeo(long leo, long leoTimestamp) {
        if (leoTimestamp >= 0) {
            leoIndex.insert(leo, leoTimestamp);
        }
    }

    /**
     * Kept for compatibility while Scala call sites are migrated.
     */
    @Deprecated
    public synchronized void updateLeo(long leo, long leoTimestamp, long logStartOffset, long nowMs) {
        updateLeo(leo, leoTimestamp);
    }

    public synchronized void onFetch(int sessionId, long offset, long timestamp, long currentTimeMs) {
        SessionEntry entry = sessions.get(sessionId);
        if (entry == null) {
            entry = new SessionEntry(new LayeredSparseIndex(sessionIntervals, sessionCapacities), currentTimeMs);
            sessions.put(sessionId, entry);
        }
        entry.lastFetchTimeMs = currentTimeMs;

        LayeredSparseIndex sessionIndex = entry.index;
        if (!sessionIndex.isEmpty() && offset <= sessionIndex.maxOffset()) {
            sessionIndex.reset();
        }
        sessionIndex.insert(offset, timestamp);

        if (sessions.size() > maxSessions) {
            evictOldestSession();
        }
    }

    public synchronized void onSessionClosed(int sessionId) {
        sessions.remove(sessionId);
    }

    public synchronized long lookup(long targetOffset) {
        for (SessionEntry entry : sessions.values()) {
            LayeredSparseIndex sessionIndex = entry.index;
            if (sessionIndex.isEmpty()) {
                continue;
            }
            if (targetOffset < sessionIndex.minOffset() || targetOffset > sessionIndex.maxOffset()) {
                continue;
            }
            LayeredSparseIndex.LookupBounds bounds = sessionIndex.lookupBounds(targetOffset);
            if (isExactHit(bounds, targetOffset)) {
                return bounds.floor().timestamp();
            }
        }

        if (!leoIndex.isEmpty() && targetOffset >= leoIndex.minOffset() && targetOffset <= leoIndex.maxOffset()) {
            LayeredSparseIndex.LookupBounds bounds = leoIndex.lookupBounds(targetOffset);
            if (isExactHit(bounds, targetOffset)) {
                return bounds.floor().timestamp();
            }
        }

        LayeredSparseIndex.DataPoint bestFloor = null;
        LayeredSparseIndex.DataPoint bestCeil = null;
        for (SessionEntry entry : sessions.values()) {
            LayeredSparseIndex.LookupBounds bounds = entry.index.lookupBounds(targetOffset);
            bestFloor = tighterFloor(bestFloor, bounds.floor());
            bestCeil = tighterCeil(bestCeil, bounds.ceil());
        }
        LayeredSparseIndex.LookupBounds leoBounds = leoIndex.lookupBounds(targetOffset);
        bestFloor = tighterFloor(bestFloor, leoBounds.floor());
        bestCeil = tighterCeil(bestCeil, leoBounds.ceil());

        if (bestFloor != null && bestCeil != null) {
            return interpolate(bestFloor, bestCeil, targetOffset);
        }
        return MISS;
    }

    public synchronized int sessionCount() {
        return sessions.size();
    }

    /**
     * Kept for compatibility while manager and tests are migrated.
     */
    @Deprecated
    public synchronized void addPointToGlobal(long offset, long timestamp, long nowMs) {
        leoIndex.insert(offset, timestamp);
    }

    /**
     * Kept for compatibility while manager and tests are migrated.
     */
    @Deprecated
    public synchronized void periodicMaintenance(long currentTimeMs) {
    }

    /**
     * Kept for compatibility while manager and tests are migrated.
     */
    @Deprecated
    public synchronized int globalTotalSize() {
        return leoIndex.totalSize();
    }

    private static boolean isExactHit(LayeredSparseIndex.LookupBounds bounds, long targetOffset) {
        return bounds.floor() != null && bounds.floor().offset() == targetOffset;
    }

    private static LayeredSparseIndex.DataPoint tighterFloor(LayeredSparseIndex.DataPoint current,
                                                             LayeredSparseIndex.DataPoint candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null || candidate.offset() > current.offset()) {
            return candidate;
        }
        return current;
    }

    private static LayeredSparseIndex.DataPoint tighterCeil(LayeredSparseIndex.DataPoint current,
                                                            LayeredSparseIndex.DataPoint candidate) {
        if (candidate == null) {
            return current;
        }
        if (current == null || candidate.offset() < current.offset()) {
            return candidate;
        }
        return current;
    }

    private static long interpolate(LayeredSparseIndex.DataPoint floor,
                                    LayeredSparseIndex.DataPoint ceil,
                                    long targetOffset) {
        if (floor.offset() == ceil.offset()) {
            return floor.timestamp();
        }
        double ratio = (double) (targetOffset - floor.offset()) / (ceil.offset() - floor.offset());
        return floor.timestamp() + (long) (ratio * (ceil.timestamp() - floor.timestamp()));
    }

    private void evictOldestSession() {
        Integer oldestSessionId = null;
        long oldestTime = Long.MAX_VALUE;
        for (Map.Entry<Integer, SessionEntry> entry : sessions.entrySet()) {
            if (entry.getValue().lastFetchTimeMs < oldestTime) {
                oldestTime = entry.getValue().lastFetchTimeMs;
                oldestSessionId = entry.getKey();
            }
        }
        if (oldestSessionId != null) {
            sessions.remove(oldestSessionId);
        }
    }

    private static final class SessionEntry {
        final LayeredSparseIndex index;
        long lastFetchTimeMs;

        SessionEntry(LayeredSparseIndex index, long lastFetchTimeMs) {
            this.index = index;
            this.lastFetchTimeMs = lastFetchTimeMs;
        }
    }
}
