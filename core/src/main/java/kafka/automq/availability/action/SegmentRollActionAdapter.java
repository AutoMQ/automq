/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.availability.action;

import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.RecoveryAction;

/**
 * Executes segment roll through a Partition-layer owner supplied by Broker runtime wiring.
 */
public class SegmentRollActionAdapter implements AvailabilityActionAdapter {
    private final SegmentRoller segmentRoller;

    public SegmentRollActionAdapter(SegmentRoller segmentRoller) {
        this.segmentRoller = segmentRoller;
    }

    @Override
    public ActionResponse execute(RecoveryAction action, long nowMs) throws Exception {
        segmentRoller.forceRoll(action);
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.SUCCEEDED, nowMs, nowMs, null);
    }

    public interface SegmentRoller {
        void forceRoll(RecoveryAction action) throws Exception;
    }
}
