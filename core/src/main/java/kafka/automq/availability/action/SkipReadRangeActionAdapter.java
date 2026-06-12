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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkipReadRangeActionAdapter implements AvailabilityActionAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SkipReadRangeActionAdapter.class);

    private final SkippedReadRangeRegistry registry;
    private final SegmentExclusiveEndOffsetResolver segmentEndOffsetResolver;

    public SkipReadRangeActionAdapter(SkippedReadRangeRegistry registry,
                                      SegmentExclusiveEndOffsetResolver segmentEndOffsetResolver) {
        this.registry = registry;
        this.segmentEndOffsetResolver = segmentEndOffsetResolver;
    }

    @Override
    public ActionResponse execute(RecoveryAction action, long nowMs) {
        AvailabilityTarget target = action.getTarget();
        if (target.getKind() != AvailabilityTarget.Kind.TOPIC_PARTITION_OFFSET || target.getTopic() == null ||
            target.getPartition() < 0 || target.getOffset() < 0) {
            return failed(action, nowMs, "SKIP_READ_RANGE requires topic-partition-offset target");
        }
        TopicPartition topicPartition = new TopicPartition(target.getTopic(), target.getPartition());
        long exclusiveEndOffset;
        try {
            exclusiveEndOffset = segmentEndOffsetResolver.exclusiveEndOffset(topicPartition, target.getOffset());
        } catch (Exception e) {
            return failed(action, nowMs, e.getClass().getName() + ": " + e.getMessage());
        }
        if (exclusiveEndOffset <= target.getOffset()) {
            return failed(action, nowMs, "segment exclusive end offset is not after target offset");
        }
        registry.skip(topicPartition, target.getOffset(), exclusiveEndOffset);
        LOGGER.info("Registered skipped read range actionUuid={} topicPartition={} startOffset={} exclusiveEndOffset={}",
            action.getActionUuid(), topicPartition, target.getOffset(), exclusiveEndOffset);
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.SUCCEEDED, nowMs, nowMs, null);
    }

    private ActionResponse failed(RecoveryAction action, long nowMs, String reason) {
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.FAILED, nowMs, nowMs, reason);
    }

    public interface SegmentExclusiveEndOffsetResolver {
        long exclusiveEndOffset(TopicPartition topicPartition, long offset) throws Exception;
    }
}
