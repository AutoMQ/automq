package kafka.automq.lag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks SessionBuffer instances for one partition.
 */
public class SessionTracker {

    public record LookupResult(long timestamp, boolean precise) {
        public static final LookupResult MISS = new LookupResult(-1L, false);
    }

    private final ConcurrentHashMap<Integer, SessionBuffer> sessions = new ConcurrentHashMap<>();
    private final int maxSessions;
    private final int bufferMaxSize;
    private final long minTimeGapMs;
    private final long archiveAgeMs;
    private final long expireAgeMs;

    public SessionTracker(int maxSessions, int bufferMaxSize, long minTimeGapMs,
                          long archiveAgeMs, long expireAgeMs) {
        this.maxSessions = maxSessions;
        this.bufferMaxSize = bufferMaxSize;
        this.minTimeGapMs = minTimeGapMs;
        this.archiveAgeMs = archiveAgeMs;
        this.expireAgeMs = expireAgeMs;
    }

    public void onFetch(int sessionId, long offset, long timestamp, long currentTimeMs) {
        SessionBuffer buffer = sessions.computeIfAbsent(
            sessionId,
            key -> new SessionBuffer(bufferMaxSize, minTimeGapMs)
        );
        buffer.append(offset, timestamp, currentTimeMs);

        if (sessions.size() > maxSessions) {
            evictOldestSession();
        }
    }

    public LookupResult lookup(long targetOffset) {
        List<Map.Entry<Integer, SessionBuffer>> ordered = new ArrayList<>(sessions.entrySet());
        ordered.sort(Comparator.comparingInt(Map.Entry::getKey));

        for (Map.Entry<Integer, SessionBuffer> entry : ordered) {
            SessionBuffer buffer = entry.getValue();
            if (buffer.isEmpty()) {
                continue;
            }
            if (targetOffset < buffer.minOffset() || targetOffset > buffer.maxOffset()) {
                continue;
            }
            SessionBuffer.LookupResult result = buffer.lookup(targetOffset);
            if (result.timestamp() >= 0) {
                return new LookupResult(result.timestamp(), result.precise());
            }
        }

        return LookupResult.MISS;
    }

    public List<SessionBuffer.DataPoint> flushAgedPoints(long currentTimeMs) {
        List<SessionBuffer.DataPoint> flushed = new ArrayList<>();
        for (Map.Entry<Integer, SessionBuffer> entry : new ArrayList<>(sessions.entrySet())) {
            SessionBuffer buffer = entry.getValue();
            long idleMs = currentTimeMs - buffer.lastUpdateTimeMs();
            if (idleMs >= archiveAgeMs) {
                flushed.addAll(buffer.getRepresentativePoints(5));
                sessions.remove(entry.getKey());
            }
        }
        return flushed;
    }

    public void onSessionClosed(int sessionId) {
        sessions.remove(sessionId);
    }

    private void evictOldestSession() {
        sessions.entrySet().stream()
            .min(Comparator.comparingLong(e -> e.getValue().lastUpdateTimeMs()))
            .ifPresent(e -> sessions.remove(e.getKey()));
    }

    public int sessionCount() {
        return sessions.size();
    }
}
