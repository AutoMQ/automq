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

public interface Normalizer {

    /**
     * Normalize the value to [0, 1]
     *
     * @param value the value to normalize
     * @return the normalized value
     */
    double normalize(double value);

    default double normalize(double value, boolean reverse) {
        double normalizedValue = normalize(value);
        return reverse ? (1 - normalizedValue) : normalizedValue;
    }
}
