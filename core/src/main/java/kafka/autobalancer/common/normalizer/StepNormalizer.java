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

package kafka.autobalancer.common.normalizer;

/**
 * Step normalizer that normalize the value to [0, 1], when value is less than stepVar, it will be normalized with
 * LinearNormalizer, otherwise it will be normalized with a logarithmic function which approaches 1 while the value
 * approaches infinity.
 */
public class StepNormalizer implements Normalizer {
    private final double stepValue;
    private final double stepVar;
    private final Normalizer linearNormalizer;

    public StepNormalizer(double min, double stepVar, double stepValue) {
        if (stepValue < 0 || stepValue > 1) {
            throw new IllegalArgumentException("Step value must be in [0, 1]");
        }
        this.stepVar = stepVar;
        this.stepValue = stepValue;
        this.linearNormalizer = new LinearNormalizer(min, this.stepVar);
    }

    @Override
    public double normalize(double value) {
        if (value <= stepVar) {
            return stepValue * linearNormalizer.normalize(value);
        }
        return stepValue + delta(value);
    }

    private double delta(double value) {
        return (1 - this.stepValue) * (1 - 1 / (Math.log(value) / Math.log(stepVar)));
    }
}
