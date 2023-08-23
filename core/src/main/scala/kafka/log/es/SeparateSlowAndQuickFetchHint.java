package kafka.log.es;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to mark if it is needed to diff quick fetch from slow fetch in current thread.
 * If marked, data should be fetched within a short time, otherwise, the request should be satisfied in a separated slow-fetch thread pool.
 */
public class SeparateSlowAndQuickFetchHint {
    private static final FastThreadLocal<AtomicBoolean> MANUAL_RELEASE = new FastThreadLocal<>() {
        @Override
        protected AtomicBoolean initialValue() {
            return new AtomicBoolean(false);
        }
    };

    public static boolean isMarked() {
        return MANUAL_RELEASE.get().get();
    }

    public static void mark() {
        MANUAL_RELEASE.get().set(true);
    }

    public static void reset() {
        MANUAL_RELEASE.get().set(false);
    }
}
