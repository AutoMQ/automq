/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common.normalizer;

/**
 * Linear normalizer that normalize the value to [0, 1]
 */
public class LinearNormalizer implements Normalizer {
    private final double min;
    private final double max;

    public LinearNormalizer(double min, double max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public double normalize(double value) {
        return (value - min) / (max - min);
    }
}
