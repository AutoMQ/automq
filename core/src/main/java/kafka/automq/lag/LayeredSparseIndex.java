package kafka.automq.lag;

/**
 * Multi-level sparse index mapping offset to timestamp using compacted arrays.
 *
 * <p>Each level has a configurable timestamp interval and capacity. Points flow
 * from L0 (finest) to L(N-1) (coarsest) via overflow demotion. Offsets are
 * globally monotonic; inserts with offset <= lastInsertedOffset are ignored.
 *
 * <p>Not thread-safe. Callers should synchronize externally.
 */
public class LayeredSparseIndex {

    public record DataPoint(long offset, long timestamp) {
    }

    public record LookupBounds(DataPoint floor, DataPoint ceil) {
    }

    static final class Level {
        final long intervalMs;
        final int capacity;
        final DataPoint[] points;
        int size;

        Level(long intervalMs, int capacity) {
            this.intervalMs = intervalMs;
            this.capacity = capacity;
            this.points = new DataPoint[Math.max(2, capacity + 1)];
        }

        void append(DataPoint point) {
            points[size++] = point;
        }

        DataPoint last() {
            return size > 0 ? points[size - 1] : null;
        }

        DataPoint[] removeFirstN(int n) {
            int toRemove = Math.min(n, size);
            DataPoint[] removed = new DataPoint[toRemove];
            System.arraycopy(points, 0, removed, 0, toRemove);

            int remaining = size - toRemove;
            if (remaining > 0) {
                System.arraycopy(points, toRemove, points, 0, remaining);
            }
            for (int i = remaining; i < size; i++) {
                points[i] = null;
            }
            size = remaining;
            return removed;
        }

        LookupBounds binarySearch(long targetOffset) {
            if (size == 0) {
                return new LookupBounds(null, null);
            }

            int lo = 0;
            int hi = size - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                long midOffset = points[mid].offset();
                if (midOffset == targetOffset) {
                    return new LookupBounds(points[mid], points[mid]);
                }
                if (midOffset < targetOffset) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }

            DataPoint floor = hi >= 0 ? points[hi] : null;
            DataPoint ceil = lo < size ? points[lo] : null;
            return new LookupBounds(floor, ceil);
        }

        void clear() {
            for (int i = 0; i < size; i++) {
                points[i] = null;
            }
            size = 0;
        }

        long minOffset() {
            return size > 0 ? points[0].offset() : Long.MAX_VALUE;
        }

        long maxOffset() {
            return size > 0 ? points[size - 1].offset() : Long.MIN_VALUE;
        }
    }

    private final Level[] levels;
    private long lastInsertedOffset = Long.MIN_VALUE;

    public LayeredSparseIndex(long[] intervals, int[] capacities) {
        if (intervals.length != capacities.length || intervals.length == 0) {
            throw new IllegalArgumentException("intervals and capacities must have the same non-zero length");
        }
        this.levels = new Level[intervals.length];
        for (int i = 0; i < intervals.length; i++) {
            if (capacities[i] <= 0) {
                throw new IllegalArgumentException("capacity must be positive");
            }
            levels[i] = new Level(intervals[i], capacities[i]);
        }
    }

    public void insert(long offset, long timestamp) {
        if (offset <= lastInsertedOffset) {
            return;
        }
        lastInsertedOffset = offset;

        Level l0 = levels[0];
        if (l0.size == 0 || Math.abs(timestamp - l0.last().timestamp()) >= l0.intervalMs) {
            l0.append(new DataPoint(offset, timestamp));
            if (l0.size > l0.capacity) {
                overflow(0);
            }
        }
    }

    private void overflow(int levelIdx) {
        Level current = levels[levelIdx];
        int halfSize = Math.max(1, current.capacity / 2);
        DataPoint[] removed = current.removeFirstN(halfSize);

        int nextIdx = levelIdx + 1;
        if (nextIdx >= levels.length || removed.length == 0) {
            return;
        }

        Level next = levels[nextIdx];
        int start = 0;
        long anchor;
        if (next.size == 0) {
            next.append(removed[0]);
            anchor = removed[0].timestamp();
            start = 1;
        } else {
            anchor = next.last().timestamp();
        }
        for (int i = start; i < removed.length; i++) {
            DataPoint point = removed[i];
            if (Math.abs(point.timestamp() - anchor) >= next.intervalMs) {
                next.append(point);
                anchor = point.timestamp();
            }
        }
        if (next.size > next.capacity) {
            overflow(nextIdx);
        }
    }

    public LookupBounds lookupBounds(long targetOffset) {
        DataPoint bestFloor = null;
        DataPoint bestCeil = null;

        for (Level level : levels) {
            if (level.size == 0) {
                continue;
            }

            LookupBounds bounds = level.binarySearch(targetOffset);
            if (bounds.floor() != null && bounds.ceil() != null && bounds.floor().offset() == targetOffset) {
                return bounds;
            }

            if (bounds.floor() != null && (bestFloor == null || bounds.floor().offset() > bestFloor.offset())) {
                bestFloor = bounds.floor();
            }
            if (bounds.ceil() != null && (bestCeil == null || bounds.ceil().offset() < bestCeil.offset())) {
                bestCeil = bounds.ceil();
            }
        }

        return new LookupBounds(bestFloor, bestCeil);
    }

    public void reset() {
        for (Level level : levels) {
            level.clear();
        }
        lastInsertedOffset = Long.MIN_VALUE;
    }

    public boolean isEmpty() {
        for (Level level : levels) {
            if (level.size > 0) {
                return false;
            }
        }
        return true;
    }

    public long minOffset() {
        long min = Long.MAX_VALUE;
        for (Level level : levels) {
            if (level.size > 0 && level.minOffset() < min) {
                min = level.minOffset();
            }
        }
        return min;
    }

    public long maxOffset() {
        long max = Long.MIN_VALUE;
        for (Level level : levels) {
            if (level.size > 0 && level.maxOffset() > max) {
                max = level.maxOffset();
            }
        }
        return max;
    }

    public int levelSize(int level) {
        return levels[level].size;
    }

    public int levelCount() {
        return levels.length;
    }

    public int totalSize() {
        int total = 0;
        for (Level level : levels) {
            total += level.size;
        }
        return total;
    }
}
