package org.apache.kafka.common.es;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ElasticStreamSwitch {
    private static AtomicBoolean switchEnabled = new AtomicBoolean(false);

    public static void setSwitch(boolean enabled) {
        switchEnabled.set(enabled);
    }
    public static boolean isEnabled() {
        return switchEnabled.get();
    }
}
