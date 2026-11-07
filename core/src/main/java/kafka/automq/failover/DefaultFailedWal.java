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

package kafka.automq.failover;

import java.util.Objects;

public final class DefaultFailedWal implements FailedWal {
    private final NodeRuntimeMetadata nodeMetadata;

    public DefaultFailedWal(NodeRuntimeMetadata nodeMetadata) {
        this.nodeMetadata = nodeMetadata;
    }

    public NodeRuntimeMetadata nodeMetadata() {
        return nodeMetadata;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (DefaultFailedWal) obj;
        return Objects.equals(this.nodeMetadata, that.nodeMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeMetadata);
    }

    @Override
    public String toString() {
        return "FailedWalV1[" +
            "nodeMetadata=" + nodeMetadata + ']';
    }

    @Override
    public FailoverContext toFailoverContext(int target) {
        return new FailoverContext(nodeMetadata.id(), nodeMetadata.epoch(), target, nodeMetadata.walConfigs());
    }

    @Override
    public FailedNode node() {
        return FailedNode.from(nodeMetadata);
    }

    public static FailedWal from(NodeRuntimeMetadata failedNode) {
        return new DefaultFailedWal(failedNode);
    }
}
