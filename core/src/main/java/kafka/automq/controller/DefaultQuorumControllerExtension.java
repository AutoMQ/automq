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

package kafka.automq.controller;

import kafka.automq.failover.FailoverControlManager;

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.metadata.KVRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.controller.QuorumControllerExtension;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class DefaultQuorumControllerExtension implements QuorumControllerExtension {
    private final FailoverControlManager failoverControlManager;

    public DefaultQuorumControllerExtension(QuorumController controller) {
        this.failoverControlManager = new FailoverControlManager(
            controller.snapshotRegistry(),
            controller,
            controller.clusterControl(),
            controller.nodeControlManager(),
            controller.streamControlManager()
        );
    }

    @Override
    public boolean replay(MetadataRecordType type, ApiMessage message, Optional<OffsetAndEpoch> snapshotId,
        long batchLastOffset) {
        if (Objects.requireNonNull(type) == MetadataRecordType.KVRECORD) {
            failoverControlManager.replay((KVRecord) message);
        } else {
            return false;
        }
        return true;
    }

    @Override
    public CompletableFuture<AbstractResponse> handleExtensionRequest(ControllerRequestContext context, ApiKeys apiKey, Object requestData,
                                                                      ReadEventAppender readEventAppender, WriteEventAppender writeEventAppender) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            String.format("ApiKey %s is not supported.", apiKey.name())));
    }

    @Override
    public Optional<ControllerResult<BrokerHeartbeatReply>> maybeHandleBlockedBroker(
            BrokerHeartbeatRequestData request, long registerBrokerRecordOffset) {
        return Optional.empty();
    }
}
