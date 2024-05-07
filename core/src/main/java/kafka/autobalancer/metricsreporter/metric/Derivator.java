/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.metricsreporter.metric;

// NotThreadSafe: The caller should ensure thread safety.
public class Derivator {
    private long lastY = 0;
    private long lastX = 0;

    public double derive(long x, long y) {
        return derive(x, y, true);
    }

    public double derive(long x, long y, boolean ignoreZero) {
        if (lastX == 0 && ignoreZero) {
            lastX = x;
            lastY = y;
            return 0.0;
        }
        double deltaX = x - lastX;
        double result;
        if (deltaX == 0) {
            result = 0.0;
        } else {
            result = (y - lastY) / deltaX;
        }
        lastX = x;
        lastY = y;
        return result;
    }

    public void reset() {
        lastX = 0;
        lastY = 0;
    }
}
