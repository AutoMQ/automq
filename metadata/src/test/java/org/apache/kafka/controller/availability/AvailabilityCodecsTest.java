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

package org.apache.kafka.controller.availability;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(40)
public class AvailabilityCodecsTest {
    @Test
    public void testSignalCodecPreservesSchemaAndFreshnessFields() {
        BrokerAvailabilitySnapshot snapshot = new BrokerAvailabilitySnapshot(1, 9L, 100L, 70L, 100L,
            List.of(new AvailabilitySignal(AvailabilitySignalType.LOG_READ_FAIL,
                AvailabilityTarget.topicPartitionOffset("topic", 0, 42L))));

        BrokerAvailabilitySnapshot decoded = AvailabilityCodecs.decodeSignal(AvailabilityCodecs.encodeSignal(snapshot));

        assertEquals(AvailabilityConstants.SCHEMA_VERSION, decoded.getSchemaVersion());
        assertEquals(snapshot, decoded);
    }

    @Test
    public void testActionResponseAndControllerActionCodecsPreserveUuidCorrelation() {
        Uuid actionUuid = Uuid.randomUuid();
        AvailabilityTarget target = AvailabilityTarget.topicPartition("topic", 1);
        RecoveryAction action = new RecoveryAction(actionUuid, AvailabilityActionType.PARTITION_REASSIGNMENT, target,
            -1, 100L, 200L, "test", false);
        ActionResponse response = new ActionResponse(actionUuid, AvailabilityActionType.PARTITION_REASSIGNMENT, target,
            ActionExecutionStatus.SUCCEEDED, 110L, 150L, null);
        ControllerActionState state = new ControllerActionState(action, response);

        assertEquals(action, AvailabilityCodecs.decodeAction(AvailabilityCodecs.encodeAction(action)));
        assertEquals(response, AvailabilityCodecs.decodeResponse(AvailabilityCodecs.encodeResponse(response)));
        assertEquals(state, AvailabilityCodecs.decodeControllerAction(AvailabilityCodecs.encodeControllerAction(state)));
    }

    @Test
    public void testNamespaceKeysAndCleanupClassification() {
        Uuid actionUuid = Uuid.randomUuid();
        assertEquals("__automq_avl_v1_s", AvailabilityConstants.SIGNAL_NAMESPACE);
        assertEquals("3", AvailabilityKvKeys.signalKey(3));
        assertEquals(actionUuid.toString(), AvailabilityKvKeys.actionKey(actionUuid));
        assertEquals(actionUuid.toString(), AvailabilityKvKeys.responseKey(actionUuid));
        assertEquals(actionUuid.toString(), AvailabilityKvKeys.controllerActionKey(actionUuid));

        RecoveryAction action = new RecoveryAction(actionUuid, AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(3, 5L), 3, 100L, 200L, "test", false);
        ActionResponse response = new ActionResponse(actionUuid, AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(3, 5L), ActionExecutionStatus.SUCCEEDED, 120L, 150L, null);

        assertEquals(AvailabilityKvKeys.CleanupClass.EXPIRED_ACTION,
            AvailabilityKvKeys.cleanupClass(AvailabilityConstants.ACTION_NAMESPACE, 801L, action, null, null, 600L, 600L));
        assertEquals(AvailabilityKvKeys.CleanupClass.EXPIRED_RESPONSE,
            AvailabilityKvKeys.cleanupClass(AvailabilityConstants.RESPONSE_NAMESPACE, 751L, null, response, null, 600L, 600L));
        assertEquals(AvailabilityKvKeys.CleanupClass.RETAIN,
            AvailabilityKvKeys.cleanupClass(AvailabilityConstants.SIGNAL_NAMESPACE, 1000L, null, null, null, 600L, 600L));
    }

    @Test
    public void testEmptySignalSetCanRepresentHealthySnapshot() {
        BrokerAvailabilitySnapshot snapshot = new BrokerAvailabilitySnapshot(1, 1L, 100L, 70L, 100L,
            Collections.emptyList());

        BrokerAvailabilitySnapshot decoded = AvailabilityCodecs.decodeSignal(AvailabilityCodecs.encodeSignal(snapshot));

        assertEquals(Collections.emptyList(), decoded.getSignals());
    }
}
