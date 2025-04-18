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
