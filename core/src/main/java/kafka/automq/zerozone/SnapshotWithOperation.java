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

package kafka.automq.zerozone;

import kafka.automq.partition.snapshot.SnapshotOperation;
import kafka.cluster.PartitionSnapshot;

import org.apache.kafka.common.TopicIdPartition;

import java.util.concurrent.CompletableFuture;

public class SnapshotWithOperation {
    final TopicIdPartition topicIdPartition;
    final PartitionSnapshot snapshot;
    final SnapshotOperation operation;
    final CompletableFuture<Void> snapshotCf;

    public SnapshotWithOperation(TopicIdPartition topicIdPartition, PartitionSnapshot snapshot,
        SnapshotOperation operation) {
        this(topicIdPartition, snapshot, operation, null);
    }

    public SnapshotWithOperation(TopicIdPartition topicIdPartition, PartitionSnapshot snapshot,
        SnapshotOperation operation, CompletableFuture<Void> snapshotCf) {
        this.topicIdPartition = topicIdPartition;
        this.snapshot = snapshot;
        this.operation = operation;
        this.snapshotCf = snapshotCf;
    }

    public static SnapshotWithOperation snapshotMark(CompletableFuture<Void> cf) {
        return new SnapshotWithOperation(null, null, null, cf);
    }

    public boolean isSnapshotMark() {
        return snapshotCf != null;
    }

    @Override
    public String toString() {
        return "SnapshotWithOperation{" +
            "topicIdPartition=" + topicIdPartition +
            ", snapshot=" + snapshot +
            ", operation=" + operation +
            '}';
    }
}
