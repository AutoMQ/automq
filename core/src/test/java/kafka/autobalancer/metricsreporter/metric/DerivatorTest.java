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

package kafka.autobalancer.metricsreporter.metric;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DerivatorTest {

    @Test
    public void testDerive() {
        Derivator derivator = new Derivator();
        Assertions.assertEquals(0.0, derivator.derive(1, 2, true));
        derivator.reset();
        Assertions.assertEquals(2.0, derivator.derive(1, 2, false));
        derivator.reset();
        Assertions.assertEquals(0,  derivator.derive(0, 2, true));
        derivator.reset();
        Assertions.assertEquals(0,  derivator.derive(0, 2, false));
        Assertions.assertEquals(0,  derivator.derive(0, 2));
        Assertions.assertEquals(0,  derivator.derive(2, 2));
        Assertions.assertEquals(0,  derivator.derive(2, 4));
        Assertions.assertEquals(4,  derivator.derive(3, 8));
    }
}
