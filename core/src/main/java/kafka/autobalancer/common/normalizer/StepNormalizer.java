/*
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
