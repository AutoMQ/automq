package kafka.automq.lag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Append-only per-session offset/timestamp buffer.
 */
public class SessionBuffer {

    public record DataPoint(long offset, long timestamp) {
    }

    public record LookupResult(long timestamp, boolean precise) {
        public static final LookupResult MISS = new LookupResult(-1L, false);
    }

    private final ArrayList<DataPoint> buffer = new ArrayList<>();
    private final int maxSize;
    private final long minTimeGapMs;

    private long lastRecordTimeMs = Long.MIN_VALUE;
    private long lastUpdateTimeMs = 0L;
    private long minOffset = Long.MAX_VALUE;
    private long maxOffset = Long.MIN_VALUE;

    public SessionBuffer(int maxSize, long minTimeGapMs) {
        this.maxSize = maxSize;
        this.minTimeGapMs = minTimeGapMs;
    }

    public void append(long offset, long timestamp, long currentTimeMs) {
        lastUpdateTimeMs = currentTimeMs;

        if (lastRecordTimeMs != Long.MIN_VALUE && currentTimeMs - lastRecordTimeMs < minTimeGapMs) {
            return;
        }

        buffer.add(new DataPoint(offset, timestamp));
        lastRecordTimeMs = currentTimeMs;

        if (offset < minOffset) {
            minOffset = offset;
        }
        if (offset > maxOffset) {
            maxOffset = offset;
        }

        if (buffer.size() > maxSize) {
            compactInPlace();
            recalculateMinMax();
        }
    }

    public LookupResult lookup(long targetOffset) {
        if (buffer.isEmpty()) {
            return LookupResult.MISS;
        }

        int idx = Collections.binarySearch(buffer, new DataPoint(targetOffset, 0L),
            Comparator.comparingLong(DataPoint::offset));

        if (idx >= 0) {
            return new LookupResult(buffer.get(idx).timestamp(), true);
        }

        int insertionPoint = -idx - 1;
        int floorIdx = insertionPoint - 1;
        int ceilIdx = insertionPoint;

        if (floorIdx >= 0 && ceilIdx < buffer.size()) {
            DataPoint floor = buffer.get(floorIdx);
            DataPoint ceil = buffer.get(ceilIdx);
            return new LookupResult(interpolate(floor, ceil, targetOffset), false);
        }
        if (floorIdx >= 0) {
            return new LookupResult(buffer.get(floorIdx).timestamp(), false);
        }

        return LookupResult.MISS;
    }

    public List<DataPoint> getRepresentativePoints(int count) {
        if (buffer.isEmpty()) {
            return List.of();
        }
        if (count <= 1 || buffer.size() <= count) {
            return new ArrayList<>(buffer);
        }

        List<DataPoint> result = new ArrayList<>();
        result.add(buffer.get(0));

        double step = (double) (buffer.size() - 1) / (count - 1);
        for (int i = 1; i < count - 1; i++) {
            int idx = (int) Math.round(i * step);
            if (idx > 0 && idx < buffer.size() - 1) {
                result.add(buffer.get(idx));
            }
        }

        result.add(buffer.get(buffer.size() - 1));
        return result;
    }

    private void compactInPlace() {
        int targetSize = Math.max(2, maxSize / 2);
        List<DataPoint> sampled = getSampledPoints(targetSize);
        buffer.clear();
        buffer.addAll(sampled);
    }

    private List<DataPoint> getSampledPoints(int targetSize) {
        if (buffer.size() <= targetSize) {
            return new ArrayList<>(buffer);
        }
        List<DataPoint> sampled = new ArrayList<>();
        sampled.add(buffer.get(0));

        double step = (double) (buffer.size() - 1) / (targetSize - 1);
        for (int i = 1; i < targetSize - 1; i++) {
            int idx = (int) Math.round(i * step);
            if (idx > 0 && idx < buffer.size() - 1) {
                sampled.add(buffer.get(idx));
            }
        }

        sampled.add(buffer.get(buffer.size() - 1));
        return sampled;
    }

    private void recalculateMinMax() {
        if (buffer.isEmpty()) {
            minOffset = Long.MAX_VALUE;
            maxOffset = Long.MIN_VALUE;
            return;
        }
        minOffset = buffer.get(0).offset();
        maxOffset = buffer.get(0).offset();
        for (DataPoint point : buffer) {
            if (point.offset() < minOffset) {
                minOffset = point.offset();
            }
            if (point.offset() > maxOffset) {
                maxOffset = point.offset();
            }
        }
    }

    private long interpolate(DataPoint floor, DataPoint ceil, long targetOffset) {
        if (floor.offset() == ceil.offset()) {
            return floor.timestamp();
        }
        double ratio = (double) (targetOffset - floor.offset()) / (ceil.offset() - floor.offset());
        return floor.timestamp() + (long) (ratio * (ceil.timestamp() - floor.timestamp()));
    }

    public int size() {
        return buffer.size();
    }

    public long minOffset() {
        return minOffset;
    }

    public long maxOffset() {
        return maxOffset;
    }

    public long lastUpdateTimeMs() {
        return lastUpdateTimeMs;
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }
}
