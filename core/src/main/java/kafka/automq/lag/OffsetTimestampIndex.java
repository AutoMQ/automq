package kafka.automq.lag;

import java.util.List;

/**
 * Per-partition offset->timestamp index that combines session and global sparse data.
 */
public class OffsetTimestampIndex {

    public record LookupResult(long timestamp, boolean precise) {
        public static final LookupResult MISS = new LookupResult(-1L, false);
    }

    private static final long COMPACT_INTERVAL_MS = 5 * 60 * 1000L;

    private final SessionTracker sessionTracker;
    private final GlobalSparseIndex globalIndex;

    private long lastCompactTimeMs = 0L;

    public OffsetTimestampIndex(int l0Max, int l1Max, int l2Max,
                                long l0BoundaryMs, long l1BoundaryMs,
                                int maxSessions, int bufferMaxSize,
                                long minTimeGapMs, long archiveAgeMs) {
        this.globalIndex = new GlobalSparseIndex(l0Max, l1Max, l2Max, l0BoundaryMs, l1BoundaryMs);
        this.sessionTracker = new SessionTracker(maxSessions, bufferMaxSize, minTimeGapMs, archiveAgeMs);
    }

    public synchronized void updateLeo(long leo, long leoTimestamp, long logStartOffset, long nowMs) {
        globalIndex.setLogStartOffset(logStartOffset);
        if (leoTimestamp >= 0) {
            globalIndex.addPoint(leo, leoTimestamp, nowMs);
        }
    }

    public synchronized void onFetch(int sessionId, long offset, long timestamp, long currentTimeMs) {
        sessionTracker.onFetch(sessionId, offset, timestamp, currentTimeMs);
    }

    public synchronized void onSessionClosed(int sessionId) {
        sessionTracker.onSessionClosed(sessionId);
    }

    public synchronized LookupResult lookup(long targetOffset) {
        SessionTracker.LookupResult sessionResult = sessionTracker.lookup(targetOffset);
        if (sessionResult.timestamp() >= 0) {
            return new LookupResult(sessionResult.timestamp(), sessionResult.precise());
        }

        GlobalSparseIndex.LookupResult globalResult = globalIndex.lookup(targetOffset);
        if (globalResult.timestamp() >= 0) {
            return new LookupResult(globalResult.timestamp(), globalResult.precise());
        }

        return LookupResult.MISS;
    }

    public synchronized void addPointToGlobal(long offset, long timestamp, long nowMs) {
        globalIndex.addPoint(offset, timestamp, nowMs);
    }

    public synchronized void periodicMaintenance(long currentTimeMs) {
        List<SessionBuffer.DataPoint> flushed = sessionTracker.flushAgedPoints(currentTimeMs);
        for (SessionBuffer.DataPoint point : flushed) {
            globalIndex.addPoint(point.offset(), point.timestamp(), currentTimeMs);
        }

        if (currentTimeMs - lastCompactTimeMs >= COMPACT_INTERVAL_MS) {
            globalIndex.compact(currentTimeMs);
            lastCompactTimeMs = currentTimeMs;
        }
    }

    public synchronized int globalTotalSize() {
        return globalIndex.totalSize();
    }
}
