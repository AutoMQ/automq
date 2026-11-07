package org.apache.kafka.tools.automq.perf;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public class CpuMonitor {
    private final CentralProcessor processor;
    private long[] prevTicks;

    public CpuMonitor() {
        this.processor = new SystemInfo().getHardware().getProcessor();
        this.prevTicks = processor.getSystemCpuLoadTicks();
    }

    /**
     * Returns the CPU usage between the last call of this method and now.
     * It returns -1.0 if an error occurs.
     *
     * @return CPU load between 0 and 1 (100%)
     */
    public synchronized double usage() {
        try {
            return usage0();
        } catch (Exception e) {
            return -1.0;
        }
    }

    private double usage0() {
        long[] currTicks = processor.getSystemCpuLoadTicks();
        double usage = processor.getSystemCpuLoadBetweenTicks(prevTicks);
        prevTicks = currTicks;
        return usage;
    }
}
