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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class PartitionRegistrationTest {
    @Test
    public void testElectionWasClean() {
        assertTrue(PartitionRegistration.electionWasClean(1, new int[] {1, 2}));
        assertFalse(PartitionRegistration.electionWasClean(1, new int[] {0, 2}));
        assertFalse(PartitionRegistration.electionWasClean(1, new int[] {}));
        assertTrue(PartitionRegistration.electionWasClean(3, new int[] {1, 2, 3, 4, 5, 6}));
    }

    @Test
    public void testPartitionControlInfoMergeAndDiff() {
        PartitionRegistration a = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1, 2}, null, null, 1, 0, 0);
        PartitionRegistration b = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{3}, null, null, 3, 1, 1);
        PartitionRegistration c = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1}, null, null, 1, 0, 1);
        assertEquals(b, a.merge(new PartitionChangeRecord().
            setLeader(3).setIsr(Arrays.asList(3))));
        assertEquals("isr: [1, 2] -> [3], leader: 1 -> 3, leaderEpoch: 0 -> 1, partitionEpoch: 0 -> 1",
            b.diff(a));
        assertEquals("isr: [1, 2] -> [1], partitionEpoch: 0 -> 1",
            c.diff(a));
    }

    @Test
    public void testRecordRoundTrip() {
        PartitionRegistration registrationA = new PartitionRegistration(
            new int[]{1, 2, 3}, new int[]{1, 2}, new int[]{1}, null, 1, 0, 0);
        Uuid topicId = Uuid.fromString("OGdAI5nxT_m-ds3rJMqPLA");
        int partitionId = 4;
        ApiMessageAndVersion record = registrationA.toRecord(topicId, partitionId);
        PartitionRegistration registrationB =
            new PartitionRegistration((PartitionRecord) record.message());
        assertEquals(registrationA, registrationB);
    }
}
