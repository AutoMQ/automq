/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model;

import java.util.Arrays;
import java.util.Collection;

public class Snapshot {
    public static final double INVALID = -1;
    private final double[] values;
    private final double latest;
    private Snapshot prev;

    public Snapshot(Snapshot prev, Collection<Double> values) {
        this.prev = prev;
        this.values = values.stream().mapToDouble(Double::doubleValue).toArray();
        this.latest = this.values.length > 0 ? this.values[this.values.length - 1] : INVALID;
        Arrays.sort(this.values);
    }

    public double getValue(double quantile) {
        if (quantile < 0.0 || quantile > 1.0) {
            throw new IllegalArgumentException(quantile + " out of range. Should be in [0, 1]");
        }
        if (values.length < getMinValidLength(quantile)) {
            return INVALID;
        }
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

    private int getMinValidLength(double quantile) {
        if (quantile == 0.0 || quantile == 1.0) {
            return 1;
        }
        if (quantile < 0.5) {
            return (int) Math.ceil(1.0 / quantile);
        }
        return (int) Math.ceil(1.0 / (1 - quantile));
    }

    public double get90thPercentile() {
        return getValue(0.9);
    }

    public double getLatest() {
        return latest;
    }

    public Snapshot getPrev() {
        return this.prev;
    }

    public void setPrev(Snapshot prev) {
        this.prev = prev;
    }

    public int size() {
        return this.values.length;
    }

    public double[] getValues() {
        return Arrays.copyOf(this.values, this.values.length);
    }
}
