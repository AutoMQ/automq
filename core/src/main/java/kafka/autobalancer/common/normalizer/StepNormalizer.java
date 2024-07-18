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
 * Step normalizer that normalize the value to [0, 1], when value is less than stepVar, it will be normalized with
 * LinearNormalizer, otherwise it will be normalized with a logarithmic function which approaches 1 while the value
 * approaches infinity.
 */
public class StepNormalizer implements Normalizer {
    private final double stepValue;
    private final double step;
    private final double stepOffset;
    private final Normalizer linearNormalizer;

    public StepNormalizer(double min, double step, double stepValue) {
        this(min, step, 0, stepValue);
    }

    public StepNormalizer(double min, double step, double stepOffset, double stepValue) {
        if (stepValue < 0 || stepValue > 1) {
            throw new IllegalArgumentException("Step value must be in [0, 1]");
        }
        this.step = step;
        this.stepOffset = stepOffset;
        this.stepValue = stepValue;
        this.linearNormalizer = new LinearNormalizer(min, this.step);
    }

    @Override
    public double normalize(double value) {
        if (value <= step) {
            return stepValue * linearNormalizer.normalize(value);
        }
        return stepValue + delta(value + stepOffset);
    }

    private double delta(double value) {
        return (1 - this.stepValue) * (1 - 1 / (Math.log(value) / Math.log(step + stepOffset)));
    }
}
