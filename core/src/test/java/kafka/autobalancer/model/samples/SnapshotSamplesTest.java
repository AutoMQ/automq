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

import kafka.autobalancer.model.Snapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnapshotSamplesTest {
    @Test
    public void testAppendAndSnapshot() {
        SnapshotSamples sequence = new SnapshotSamples(512);
        for (int i = 0; i < 1000; i++) {
            sequence.append(i);
        }
        Assertions.assertEquals(512, sequence.size());
        Snapshot snapshot = sequence.snapshot();
        Assertions.assertNotNull(snapshot);
        Assertions.assertNull(snapshot.getPrev());
        Assertions.assertEquals(512, snapshot.size());
        Assertions.assertEquals(948, snapshot.get90thPercentile(), 1);
        Assertions.assertEquals(744, snapshot.getValue(0.5), 1);

        for (int i = 1000; i < 2000; i++) {
            sequence.append(i);
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
}
