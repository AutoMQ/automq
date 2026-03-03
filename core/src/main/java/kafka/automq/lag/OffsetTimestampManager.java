package kafka.automq.lag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Strategy layer over OffsetTimestampIndex with backfill-on-miss.
 */
public class OffsetTimestampManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetTimestampManager.class);

    private final OffsetTimestampIndex index;
    private final Function<Long, Long> backfillFn;

    public OffsetTimestampManager(OffsetTimestampIndex index,
                                  Function<Long, Long> backfillFn) {
        this.index = index;
        this.backfillFn = backfillFn;
    }

    public long lookup(long targetOffset) {
        long timestamp = index.lookup(targetOffset);
        if (timestamp >= 0) {
            return timestamp;
        }

        if (backfillFn != null) {
            try {
                long backfillTs = backfillFn.apply(targetOffset);
                if (backfillTs >= 0) {
                    return backfillTs;
                }
            } catch (Exception e) {
                LOGGER.warn("Backfill failed for offset {}", targetOffset, e);
            }
        }

        LOGGER.debug("Timestamp lookup miss for offset {}", targetOffset);
        return -1L;
    }

    public Map<Long, Long> lookupBatch(Set<Long> targetOffsets) {
        Map<Long, Long> results = new HashMap<>();

        for (long offset : targetOffsets) {
            long timestamp = index.lookup(offset);
            if (timestamp >= 0) {
                results.put(offset, timestamp);
                continue;
            }

            if (backfillFn != null) {
                try {
                    long backfillTs = backfillFn.apply(offset);
                    if (backfillTs >= 0) {
                        results.put(offset, backfillTs);
                        continue;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Backfill failed for offset {}", offset, e);
                }
            }

            results.put(offset, -1L);
        }
        return results;
    }

    public void updateLeo(long leo, long leoTimestamp) {
        index.updateLeo(leo, leoTimestamp);
    }

    public void onFetch(int sessionId, long offset, long timestamp, long currentTimeMs) {
        index.onFetch(sessionId, offset, timestamp, currentTimeMs);
    }

    public void onSessionClosed(int sessionId) {
        index.onSessionClosed(sessionId);
    }
}
