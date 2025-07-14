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

package kafka.log.stream.s3.node;

import org.apache.kafka.controller.stream.NodeMetadata;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class NoopNodeManager implements NodeManager {
    private final int nodeId;
    private final long nodeEpoch;

    public NoopNodeManager(int nodeId, long nodeEpoch) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
    }

    @Override
    public CompletableFuture<Void> update(Function<NodeMetadata, Optional<NodeMetadata>> updater) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<NodeMetadata> getNodeMetadata() {
        return CompletableFuture.completedFuture(new NodeMetadata(nodeId, nodeEpoch, "", Collections.emptyMap()));
    }
}
