/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model.samples;

import kafka.autobalancer.model.Snapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapshottableSamplesTest {
    @Test
    public void testAppendAndSnapshot() {
        SnapshottableSamples sequence = new SnapshottableSamples(512);
        for (int i = 0; i < 1000; i++) {
            sequence.append(i, System.currentTimeMillis());
        }
        Assertions.assertEquals(512, sequence.size());
        Snapshot snapshot = sequence.snapshot();
        Assertions.assertNotNull(snapshot);
        Assertions.assertNull(snapshot.getPrev());
        Assertions.assertEquals(512, snapshot.size());
        Assertions.assertEquals(948, snapshot.get90thPercentile(), 1);
        Assertions.assertEquals(744, snapshot.getValue(0.5), 1);

        for (int i = 1000; i < 2000; i++) {
            sequence.append(i, System.currentTimeMillis());
        }
        snapshot = sequence.snapshot();
        Assertions.assertNotNull(snapshot);
        Assertions.assertNotNull(snapshot.getPrev());
        Assertions.assertEquals(512, snapshot.size());
        Assertions.assertEquals(948, snapshot.getPrev().get90thPercentile(), 1);
        Assertions.assertEquals(744, snapshot.getPrev().getValue(0.5), 1);
        Assertions.assertEquals(1948, snapshot.get90thPercentile(), 1);
        Assertions.assertEquals(1744, snapshot.getValue(0.5), 1);
    }

    @Test
    public void testSnapshot() {
        SnapshottableSamples sequence = new SnapshottableSamples(512);
        for (int i = 0; i < 1000; i++) {
            sequence.append(i, System.currentTimeMillis());
        }
        for (int i = 0; i < 100; i++) {
            sequence.snapshot();
        }
        Snapshot snapshot = sequence.snapshot();
        Assertions.assertNotNull(snapshot);
        Assertions.assertNotNull(snapshot.getPrev());
        Assertions.assertNull(snapshot.getPrev().getPrev());
    }
}
