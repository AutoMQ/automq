/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
