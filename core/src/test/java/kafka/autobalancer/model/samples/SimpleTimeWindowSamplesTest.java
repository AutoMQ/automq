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

package kafka.autobalancer.model.samples;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleTimeWindowSamplesTest {
    @Test
    public void testWindow() {
        AbstractTimeWindowSamples.Window window = new AbstractTimeWindowSamples.Window(5);
        for (int i = 0; i < 1000; i++) {
            if (i < 5) {
                Assertions.assertTrue(window.append(i));
            } else {
                Assertions.assertFalse(window.append(i));
            }
        }
        Assertions.assertEquals(4, window.latest());
        Assertions.assertEquals(10, window.sum());
    }

    @Test
    public void testAppend() {
        AbstractTimeWindowSamples timeWindowValueSequence = new SimpleTimeWindowSamples(5, 10, 10);
        Assertions.assertFalse(timeWindowValueSequence.isTrusted());

        for (int i = 0; i < 20; i++) {
            timeWindowValueSequence.append(0);
        }
        Assertions.assertEquals(2, timeWindowValueSequence.size());
        Assertions.assertFalse(timeWindowValueSequence.isTrusted());

        for (int i = 0; i < 10000; i++) {
            timeWindowValueSequence.append(0);
        }
        Assertions.assertEquals(10, timeWindowValueSequence.size());
        Assertions.assertTrue(timeWindowValueSequence.isTrusted());
        for (int i = 0; i < 50; i++) {
            timeWindowValueSequence.append(i);
        }
        Assertions.assertEquals(10, timeWindowValueSequence.size());
        Assertions.assertTrue(timeWindowValueSequence.isTrusted());
        Assertions.assertEquals(49, timeWindowValueSequence.getLatest());
    }
}
