package kafka.automq.lag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-partition offset to timestamp index combining LEO sampling and fetch sessions.
 */
public class OffsetTimestampIndex {

    public static final long MISS = -1L;

    public record LayerConfig(long intervalMs, int capacity) {
        public LayerConfig {
            if (capacity <= 0) {
                throw new IllegalArgumentException("capacity must be positive");
            }
        }
    }

    public record SparseIndexConfig(List<LayerConfig> levels) {
        public SparseIndexConfig {
            levels = List.copyOf(levels);
            if (levels.isEmpty()) {
                throw new IllegalArgumentException("levels must be non-empty");
            }
        }
    }

    public record IndexConfig(SparseIndexConfig leo, SparseIndexConfig session, int maxSessions) {
        public IndexConfig {
            if (maxSessions <= 0) {
                throw new IllegalArgumentException("maxSessions must be positive");
            }
        }
    }

    static final IndexConfig DEFAULT_CONFIG = new IndexConfig(
        new SparseIndexConfig(List.of(
            new LayerConfig(60_000L, 10),
            new LayerConfig(600_000L, 18),
            new LayerConfig(14_400_000L, 18)
        )),
        new SparseIndexConfig(List.of(
            new LayerConfig(10_000L, 12),
            new LayerConfig(60_000L, 10)
        )),
        20
    );

    private final LayeredSparseIndex latestAppendSampleIndex;
    private final Map<Integer, SessionEntry> sessions = new HashMap<>();
    private final int maxSessions;
    private final long[] sessionIntervals;
    private final int[] sessionCapacities;

    public OffsetTimestampIndex() {
        this(DEFAULT_CONFIG);
    }

    public OffsetTimestampIndex(IndexConfig config) {
        this(
            config.maxSessions(),
            config.leo(),
            config.session()
        );
    }

    private OffsetTimestampIndex(int maxSessions,
                                 SparseIndexConfig leoConfig,
                                 SparseIndexConfig sessionConfig) {
        this.maxSessions = maxSessions;
        this.latestAppendSampleIndex = new LayeredSparseIndex(intervals(leoConfig), capacities(leoConfig));
        this.sessionIntervals = intervals(sessionConfig);
        this.sessionCapacities = capacities(sessionConfig);
    }

    public synchronized void updateLatestAppendSample(long offset, long timestamp) {
        if (timestamp >= 0) {
            latestAppendSampleIndex.insert(offset, timestamp);
        }
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

        if (!latestAppendSampleIndex.isEmpty()
            && targetOffset >= latestAppendSampleIndex.minOffset()
            && targetOffset <= latestAppendSampleIndex.maxOffset()) {
            LayeredSparseIndex.LookupBounds bounds = latestAppendSampleIndex.lookupBounds(targetOffset);
            if (isExactHit(bounds, targetOffset)) {
                return bounds.floor().timestamp();
            }
        }

        LayeredSparseIndex.DataPoint bestFloor = null;
        LayeredSparseIndex.DataPoint bestCeil = null;
        for (SessionEntry entry : sessions.values()) {
            LayeredSparseIndex sessionIndex = entry.index;
            if (sessionIndex.isEmpty()) {
                continue;
            }
            if (targetOffset < sessionIndex.minOffset() || targetOffset > sessionIndex.maxOffset()) {
                continue;
            }
            LayeredSparseIndex.LookupBounds bounds = sessionIndex.lookupBounds(targetOffset);
            bestFloor = tighterFloor(bestFloor, bounds.floor());
            bestCeil = tighterCeil(bestCeil, bounds.ceil());
        }
        if (!latestAppendSampleIndex.isEmpty()
            && targetOffset >= latestAppendSampleIndex.minOffset()
            && targetOffset <= latestAppendSampleIndex.maxOffset()) {
            LayeredSparseIndex.LookupBounds latestSampleBounds = latestAppendSampleIndex.lookupBounds(targetOffset);
            bestFloor = tighterFloor(bestFloor, latestSampleBounds.floor());
            bestCeil = tighterCeil(bestCeil, latestSampleBounds.ceil());
        }

        if (bestFloor != null && bestCeil != null) {
            return interpolate(bestFloor, bestCeil, targetOffset);
        }
        return MISS;
    }

    public synchronized int sessionCount() {
        return sessions.size();
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

    private static long[] intervals(SparseIndexConfig config) {
        long[] values = new long[config.levels().size()];
        for (int i = 0; i < config.levels().size(); i++) {
            values[i] = config.levels().get(i).intervalMs();
        }
        return values;
    }

    private static int[] capacities(SparseIndexConfig config) {
        int[] values = new int[config.levels().size()];
        for (int i = 0; i < config.levels().size(); i++) {
            values[i] = config.levels().get(i).capacity();
        }
        return values;
    }
}
