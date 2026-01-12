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

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.controller.QuorumController.ControllerWriteOperation;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface QuorumControllerExtension {
    QuorumControllerExtension NOOP = new QuorumControllerExtension() {
        @Override
        public boolean replay(MetadataRecordType type, ApiMessage message, Optional<OffsetAndEpoch> snapshotId,
                long batchLastOffset) {
            return false;
        }

        @Override
        public CompletableFuture<AbstractResponse> handleExtensionRequest(
                ControllerRequestContext context,
                ApiKeys apiKey,
                Object requestData,
                ReadEventAppender readEventAppender,
                WriteEventAppender writeEventAppender) {
            return null;
        }
    };

    boolean replay(MetadataRecordType type, ApiMessage message, Optional<OffsetAndEpoch> snapshotId,
            long batchLastOffset);

    CompletableFuture<AbstractResponse> handleExtensionRequest(
            ControllerRequestContext context,
            ApiKeys apiKey,
            Object requestData,
            ReadEventAppender readEventAppender,
            WriteEventAppender writeEventAppender);

    @FunctionalInterface
    interface ReadEventAppender {
        <T> CompletableFuture<T> appendReadEvent(String name, OptionalLong deadlineNs, Supplier<T> handler);
    }

    @FunctionalInterface
    interface WriteEventAppender {
        <T> CompletableFuture<T> appendWriteEvent(String name, OptionalLong deadlineNs,
                ControllerWriteOperation<T> op);
    }
}
