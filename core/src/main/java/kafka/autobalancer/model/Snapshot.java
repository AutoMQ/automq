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

package kafka.autobalancer.model;

import java.util.Arrays;
import java.util.Collection;

public class Snapshot {
    private final Snapshot prev;
    private final double[] values;
    private Double latest;
    private Double percentile90th;

    public Snapshot(Snapshot prev, Collection<Double> values) {
        this.prev = prev;
        this.values = values.stream().mapToDouble(Double::doubleValue).toArray();
        Arrays.sort(this.values);
    }

    public double getValue(double quantile) {
        if (!(quantile < 0.0) && !(quantile > 1.0)) {
            if (this.values.length == 0) {
                return 0.0;
            } else {
                double pos = quantile * (double) (this.values.length + 1);
                if (pos < 1.0) {
                    return this.values[0];
                } else if (pos >= (double) this.values.length) {
                    return this.values[this.values.length - 1];
                } else {
                    double lower = this.values[(int) pos - 1];
                    double upper = this.values[(int) pos];
                    return lower + (pos - Math.floor(pos)) * (upper - lower);
                }
            }
        } else {
            throw new IllegalArgumentException(quantile + " is not in [0..1]");
        }
    }

    public double get90thPercentile() {
        if (this.percentile90th == null) {
            this.percentile90th = this.getValue(0.9);
        }
        return this.percentile90th;
    }

    public double getLatest() {
        if (this.latest == null) {
            this.latest = this.values[this.values.length - 1];
        }
        return latest;
    }

    public Snapshot getPrev() {
        return this.prev;
    }

    public int size() {
        return this.values.length;
    }

    public double[] getValues() {
        return Arrays.copyOf(this.values, this.values.length);
    }
}
