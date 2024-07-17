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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
@Tag("S3Unit")
public class LifecycleDeadlineCheckerTest {
    @Test
    void testChecker() {
        MockTime time = new MockTime(0, 0, 0);
        long now = 0;
        time.setCurrentTimeMs(now);

        S3ObjectControlManager.LifecycleDeadlineChecker checker =
            new S3ObjectControlManager.LifecycleDeadlineChecker(time, 1000);

        assertEquals(time.milliseconds() + 1000, checker.getCurrentRunDeadLineMs());

        time.setCurrentTimeMs(now + 999);
        assertFalse(checker.maxRuntimeExceeded());

        time.setCurrentTimeMs(now + 1001);
        assertTrue(checker.maxRuntimeExceeded());


        now = 2000;
        time.setCurrentTimeMs(now);
        checker.reset();

        assertEquals(time.milliseconds() + 1000, checker.getCurrentRunDeadLineMs());

        time.setCurrentTimeMs(now + 6);
        assertFalse(checker.maxRuntimeExceeded());

        time.setCurrentTimeMs(now + 1001);
        assertTrue(checker.maxRuntimeExceeded());

    }
}
