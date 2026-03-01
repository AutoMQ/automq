package kafka.automq.lag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Three-layer sparse index mapping offset -> timestamp.
 * Layers are defined by message timestamp age (now - dataPoint.timestamp).
 * All writes enter L0 first, then compaction demotes old points to L1/L2.
 */
public class GlobalSparseIndex {

    public record LookupResult(long timestamp, boolean precise) {
        public static final LookupResult MISS = new LookupResult(-1L, false);
    }

    private final TreeMap<Long, Long> l0 = new TreeMap<>();
    private final TreeMap<Long, Long> l1 = new TreeMap<>();
    private final TreeMap<Long, Long> l2 = new TreeMap<>();

    private final int l0Max;
    private final int l1Max;
    private final int l2Max;

    private final long l0BoundaryMs;
    private final long l1BoundaryMs;

    private long logStartOffset = 0L;
    private long lastCompactTimeMs = 0L;
    private long latestTimestamp = Long.MIN_VALUE;

    public GlobalSparseIndex(int l0Max, int l1Max, int l2Max, long l0BoundaryMs, long l1BoundaryMs) {
        this.l0Max = l0Max;
        this.l1Max = l1Max;
        this.l2Max = l2Max;
        this.l0BoundaryMs = l0BoundaryMs;
        this.l1BoundaryMs = l1BoundaryMs;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public void addPoint(long offset, long timestamp) {
        if (timestamp > latestTimestamp) {
            latestTimestamp = timestamp;
        }
        long nowMs = latestTimestamp == Long.MIN_VALUE ? timestamp : latestTimestamp;
        addPoint(offset, timestamp, nowMs);
    }

    public void addPoint(long offset, long timestamp, long nowMs) {
        if (offset < logStartOffset) {
            return;
        }
        l0.put(offset, timestamp);
        if (l0.size() > l0Max) {
            compact(nowMs);
        }
    }

    public LookupResult lookup(long targetOffset) {
        LookupResult exact = exactLookup(targetOffset);
        if (exact != null) {
            return exact;
        }
        Bounds bounds = findBounds(targetOffset);
        if (bounds.floor != null && bounds.ceil != null) {
            return new LookupResult(interpolate(bounds.floor, bounds.ceil, targetOffset), false);
        }
        if (bounds.floor != null) {
            return new LookupResult(bounds.floor.getValue(), false);
        }

        return LookupResult.MISS;
    }

    public void compact(long nowMs) {
        demoteAged(l0, l1, l2, nowMs);

        if (l0.size() > l0Max) {
            halveSample(l0, Math.max(2, l0Max / 2));
        }

        demoteL1Aged(nowMs);

        if (l1.size() > l1Max) {
            halveSample(l1, Math.max(2, l1Max / 2));
        }

        while (!l2.isEmpty() && l2.firstKey() < logStartOffset) {
            l2.pollFirstEntry();
        }

        if (l2.size() > l2Max) {
            halveSample(l2, Math.max(2, l2Max / 2));
        }

        lastCompactTimeMs = nowMs;
    }

    private void demoteAged(TreeMap<Long, Long> source, TreeMap<Long, Long> mid, TreeMap<Long, Long> old, long nowMs) {
        List<Long> toRemove = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : source.entrySet()) {
            long age = nowMs - entry.getValue();
            if (age > l0BoundaryMs) {
                if (age <= l1BoundaryMs) {
                    mid.put(entry.getKey(), entry.getValue());
                } else {
                    old.put(entry.getKey(), entry.getValue());
                }
                toRemove.add(entry.getKey());
            }
        }
        toRemove.forEach(source::remove);
    }

    private LookupResult exactLookup(long targetOffset) {
        Long ts = l0.get(targetOffset);
        if (ts != null) {
            return new LookupResult(ts, true);
        }
        ts = l1.get(targetOffset);
        if (ts != null) {
            return new LookupResult(ts, true);
        }
        ts = l2.get(targetOffset);
        if (ts != null) {
            return new LookupResult(ts, true);
        }
        return null;
    }

    private Bounds findBounds(long targetOffset) {
        Map.Entry<Long, Long> bestFloor = null;
        Map.Entry<Long, Long> bestCeil = null;
        for (TreeMap<Long, Long> layer : List.of(l0, l1, l2)) {
            Map.Entry<Long, Long> floor = layer.floorEntry(targetOffset);
            Map.Entry<Long, Long> ceil = layer.ceilingEntry(targetOffset);
            if (floor != null && (bestFloor == null || floor.getKey() > bestFloor.getKey())) {
                bestFloor = floor;
            }
            if (ceil != null && (bestCeil == null || ceil.getKey() < bestCeil.getKey())) {
                bestCeil = ceil;
            }
        }
        return new Bounds(bestFloor, bestCeil);
    }

    private void demoteL1Aged(long nowMs) {
        List<Long> toRemove = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : l1.entrySet()) {
            long age = nowMs - entry.getValue();
            if (age > l1BoundaryMs) {
                l2.put(entry.getKey(), entry.getValue());
                toRemove.add(entry.getKey());
            }
        }
        toRemove.forEach(l1::remove);
    }

    private void halveSample(TreeMap<Long, Long> tree, int targetSize) {
        if (tree.size() <= targetSize) {
            return;
        }

        List<Map.Entry<Long, Long>> entries = new ArrayList<>(tree.entrySet());
        tree.clear();

        tree.put(entries.get(0).getKey(), entries.get(0).getValue());
        tree.put(entries.get(entries.size() - 1).getKey(), entries.get(entries.size() - 1).getValue());

        double step = (double) (entries.size() - 1) / (targetSize - 1);
        for (int i = 1; i < targetSize - 1; i++) {
            int idx = (int) Math.round(i * step);
            if (idx > 0 && idx < entries.size() - 1) {
                tree.put(entries.get(idx).getKey(), entries.get(idx).getValue());
            }
        }
    }

    private long interpolate(Map.Entry<Long, Long> floor, Map.Entry<Long, Long> ceil, long targetOffset) {
        if (floor.getKey().equals(ceil.getKey())) {
            return floor.getValue();
        }
        double ratio = (double) (targetOffset - floor.getKey()) / (ceil.getKey() - floor.getKey());
        return floor.getValue() + (long) (ratio * (ceil.getValue() - floor.getValue()));
    }

    private static final class Bounds {
        private final Map.Entry<Long, Long> floor;
        private final Map.Entry<Long, Long> ceil;

        private Bounds(Map.Entry<Long, Long> floor, Map.Entry<Long, Long> ceil) {
            this.floor = floor;
            this.ceil = ceil;
        }
    }

    public int l0Size() {
        return l0.size();
    }

    public int l1Size() {
        return l1.size();
    }

    public int l2Size() {
        return l2.size();
    }

    public int totalSize() {
        return l0.size() + l1.size() + l2.size();
    }

    public long lastCompactTimeMs() {
        return lastCompactTimeMs;
    }
}
