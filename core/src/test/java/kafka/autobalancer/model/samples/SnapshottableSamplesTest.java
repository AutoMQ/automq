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
