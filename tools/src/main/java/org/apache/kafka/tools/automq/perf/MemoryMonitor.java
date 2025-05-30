package org.apache.kafka.tools.automq.perf;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public class MemoryMonitor {

    /**
     * Returns the amount of heap memory used by the JVM in bytes.
     */
    public static long heapUsed() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    /**
     * Returns the amount of direct memory used by the JVM in bytes.
     */
    public static long directUsed() {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        return pools.stream()
            .filter(p -> "direct".equals(p.getName()))
            .mapToLong(BufferPoolMXBean::getMemoryUsed)
            .sum();
    }
}
