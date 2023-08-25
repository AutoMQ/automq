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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.es.ElasticStreamSwitch;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("esUnit")
public class PartitionChangeBuilderESTest {
    @BeforeEach
    public void setUp() {
        ElasticStreamSwitch.setSwitch(true);
    }

    @AfterEach
    public void tearDown() {
        ElasticStreamSwitch.setSwitch(false);
    }

    @Test
    public void testElectLeader() {
        // elect the targetNode regardless of the election type
        assertElectLeaderEquals(createFooBuilder().setElection(PartitionChangeBuilder.Election.PREFERRED).setTargetNode(100), 100, false);
        assertElectLeaderEquals(createFooBuilder().setTargetNode(101), 101, false);
        assertElectLeaderEquals(createFooBuilder().setElection(PartitionChangeBuilder.Election.UNCLEAN).setTargetNode(102), 102, false);

        // There should not be recovering state for leaders since unclean elections will never be touched. However, we
        // still test these cases in case of odd situations.
        assertElectLeaderEquals(createRecoveringFOOBuilder().setElection(PartitionChangeBuilder.Election.PREFERRED).setTargetNode(100), 100, false);
        assertElectLeaderEquals(createRecoveringFOOBuilder().setTargetNode(101), 101, false);
        assertElectLeaderEquals(createRecoveringFOOBuilder().setElection(PartitionChangeBuilder.Election.UNCLEAN).setTargetNode(102), 102, false);
    }

    private final static PartitionRegistration FOO = new PartitionRegistration(
        new int[] {2, 1, 3}, new int[] {2, 1, 3}, Replicas.NONE, Replicas.NONE,
        1, LeaderRecoveryState.RECOVERED, 100, 200);

    private final static Uuid FOO_ID = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");

    private static PartitionChangeBuilder createFooBuilder() {
        return new PartitionChangeBuilder(FOO, FOO_ID, 0, r -> r != 3, true);
    }

    private final static PartitionRegistration RECOVERING_FOO = new PartitionRegistration(
        new int[] {2, 1, 3}, new int[] {2, 1, 3}, Replicas.NONE, Replicas.NONE,
        1, LeaderRecoveryState.RECOVERING, 100, 200);

    private final static Uuid RECOVERING_FOO_ID = Uuid.fromString("KbrrdcfiR-KC2CPSTHaJrh");

    private static PartitionChangeBuilder createRecoveringFOOBuilder() {
        return new PartitionChangeBuilder(RECOVERING_FOO, RECOVERING_FOO_ID, 0, r -> r != 3, true);
    }

    private static void assertElectLeaderEquals(PartitionChangeBuilder builder,
        int expectedNode,
        boolean expectedUnclean) {
        PartitionChangeBuilder.ElectionResult electionResult = builder.electLeader();
        assertEquals(expectedNode, electionResult.node);
        assertEquals(expectedUnclean, electionResult.unclean);
    }
}
