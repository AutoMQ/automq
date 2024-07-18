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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizerTest {
    private static final double EPSILON = 0.0001;

    @Test
    public void testLinearNormalize() {
        Normalizer normalizer = new LinearNormalizer(0, 100);
        Assertions.assertEquals(-0.1, normalizer.normalize(-10), EPSILON);
        Assertions.assertEquals(0.0, normalizer.normalize(0), EPSILON);
        Assertions.assertEquals(0.4, normalizer.normalize(40), EPSILON);
        Assertions.assertEquals(1.0, normalizer.normalize(100), EPSILON);
        Assertions.assertEquals(1.2, normalizer.normalize(120), EPSILON);

        Assertions.assertEquals(1.1, normalizer.normalize(-10, true), EPSILON);
        Assertions.assertEquals(1.0, normalizer.normalize(0, true), EPSILON);
        Assertions.assertEquals(0.6, normalizer.normalize(40, true), EPSILON);
        Assertions.assertEquals(0.0, normalizer.normalize(100, true), EPSILON);
        Assertions.assertEquals(-0.2, normalizer.normalize(120, true), EPSILON);
    }

    @Test
    public void testStepNormalizer() {
        Normalizer normalizer = new StepNormalizer(0, 100, 0.9);
        Assertions.assertEquals(-0.09, normalizer.normalize(-10), EPSILON);
        Assertions.assertEquals(0, normalizer.normalize(0), EPSILON);
        Assertions.assertEquals(0.36, normalizer.normalize(40), EPSILON);
        Assertions.assertEquals(0.9, normalizer.normalize(100), EPSILON);
        double v1 = normalizer.normalize(120);
        double v2 = normalizer.normalize(Double.MAX_VALUE);
        Assertions.assertTrue(v1 > 0.9 && v1 < 1.0);
        Assertions.assertTrue(v2 > 0.9 && v2 < 1.0);
        Assertions.assertTrue(v1 < v2);
    }
}
