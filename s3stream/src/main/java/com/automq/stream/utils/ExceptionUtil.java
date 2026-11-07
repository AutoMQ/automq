package com.automq.stream.utils;

import java.util.Collection;
import java.util.Iterator;

public class ExceptionUtil {

    /**
     * Combine multiple exceptions into one, where the first exception is the primary one and the rest are suppressed.
     * It returns null if the input collection is null or empty.
     */
    public static <E extends Exception> E combine(Collection<E> exceptions) {
        if (null == exceptions || exceptions.isEmpty()) {
            return null;
        }
        Iterator<E> it = exceptions.iterator();
        E primary = it.next();
        while (it.hasNext()) {
            primary.addSuppressed(it.next());
        }
        return primary;
    }
}
