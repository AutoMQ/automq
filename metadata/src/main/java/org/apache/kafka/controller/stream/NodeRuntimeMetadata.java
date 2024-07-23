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
package org.apache.kafka.controller.stream;

import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;

public class NodeRuntimeMetadata {

    private final int nodeId;
    private final TimelineLong nodeEpoch;
    private final TimelineObject<Boolean> failoverMode;
    private final TimelineHashMap<Long/*objectId*/, S3StreamSetObject> streamSetObjects;

    public NodeRuntimeMetadata(int nodeId, long nodeEpoch, boolean failoverMode, SnapshotRegistry registry) {
        this.nodeId = nodeId;
        this.nodeEpoch = new TimelineLong(registry);
        this.nodeEpoch.set(nodeEpoch);
        this.failoverMode = new TimelineObject<>(registry, failoverMode);
        this.streamSetObjects = new TimelineHashMap<>(registry, 0);
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch.get();
    }

    public void setNodeEpoch(long nodeEpoch) {
        this.nodeEpoch.set(nodeEpoch);
    }

    public boolean getFailoverMode() {
        return failoverMode.get();
    }

    public void setFailoverMode(boolean failoverMode) {
        this.failoverMode.set(failoverMode);
    }

    public TimelineHashMap<Long, S3StreamSetObject> streamSetObjects() {
        return streamSetObjects;
    }

    @Override
    public String toString() {
        return "NodeS3StreamSetObjectMetadata{" +
                "nodeId=" + nodeId +
                ", nodeEpoch=" + nodeEpoch +
                ", streamSetObjects=" + streamSetObjects +
                '}';
    }
}
