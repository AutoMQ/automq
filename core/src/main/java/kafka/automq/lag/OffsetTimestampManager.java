package kafka.automq.lag;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Strategy layer over OffsetTimestampIndex.
 */
public class OffsetTimestampManager {

    private final OffsetTimestampIndex index;
    private final Function<Long, Long> backfillFn;
    private final int maxBackfillsPerBatch;

    public OffsetTimestampManager(OffsetTimestampIndex index,
                                  Function<Long, Long> backfillFn,
                                  int maxBackfillsPerBatch) {
        this.index = index;
        this.backfillFn = backfillFn;
        this.maxBackfillsPerBatch = maxBackfillsPerBatch;
    }

    public OffsetTimestampIndex.LookupResult lookup(long targetOffset) {
        OffsetTimestampIndex.LookupResult result = index.lookup(targetOffset);
        if (result.timestamp() >= 0) {
            return result;
        }

        if (backfillFn != null) {
            long ts = backfillFn.apply(targetOffset);
            if (ts >= 0) {
                index.addPointToGlobal(targetOffset, ts, System.currentTimeMillis());
                return new OffsetTimestampIndex.LookupResult(ts, true);
            }
        }

        return OffsetTimestampIndex.LookupResult.MISS;
    }

    public Map<Long, OffsetTimestampIndex.LookupResult> lookupBatch(Set<Long> targetOffsets) {
        Map<Long, OffsetTimestampIndex.LookupResult> results = new HashMap<>();
        int backfillCount = 0;

        for (long offset : targetOffsets) {
            OffsetTimestampIndex.LookupResult result = index.lookup(offset);
            if (result.timestamp() >= 0) {
                results.put(offset, result);
                continue;
            }

            if (backfillFn != null && backfillCount < maxBackfillsPerBatch) {
                long ts = backfillFn.apply(offset);
                if (ts >= 0) {
                    index.addPointToGlobal(offset, ts, System.currentTimeMillis());
                    results.put(offset, new OffsetTimestampIndex.LookupResult(ts, true));
                    backfillCount++;
                    continue;
                }
            }

            results.put(offset, OffsetTimestampIndex.LookupResult.MISS);
        }

        return results;
    }

    public void updateLeo(long leo, long leoTimestamp, long logStartOffset, long nowMs) {
        index.updateLeo(leo, leoTimestamp, logStartOffset, nowMs);
    }

    public void onFetch(int sessionId, long offset, long timestamp, long currentTimeMs) {
        index.onFetch(sessionId, offset, timestamp, currentTimeMs);
    }

    public void onSessionClosed(int sessionId) {
        index.onSessionClosed(sessionId);
    }

    public void periodicMaintenance(long currentTimeMs) {
        index.periodicMaintenance(currentTimeMs);
    }

    public OffsetTimestampIndex getIndex() {
        return index;
    }
}
