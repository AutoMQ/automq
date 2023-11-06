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


package org.apache.kafka.image;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.automq.stream.s3.metadata.S3StreamConstant;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.metadata.stream.S3SSTObject;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class NodeS3SSTMetadataImage {

    public static final NodeS3SSTMetadataImage EMPTY = new NodeS3SSTMetadataImage(S3StreamConstant.INVALID_BROKER_ID,
        S3StreamConstant.INVALID_BROKER_EPOCH, Collections.emptyMap());
    private final int nodeId;
    private final long nodeEpoch;
    private final Map<Long/*objectId*/, S3SSTObject> s3SSTObjects;
    private final SortedMap<Long/*orderId*/, S3SSTObject> orderIndex;

    public NodeS3SSTMetadataImage(int nodeId, long nodeEpoch, Map<Long, S3SSTObject> sstObjects) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.s3SSTObjects = new HashMap<>(sstObjects);
        // build order index
        if (s3SSTObjects.isEmpty()) {
            this.orderIndex = Collections.emptySortedMap();
        } else {
            this.orderIndex = new TreeMap<>();
            s3SSTObjects.values().forEach(s3SSTObject -> orderIndex.put(s3SSTObject.orderId(), s3SSTObject));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeS3SSTMetadataImage that = (NodeS3SSTMetadataImage) o;
        return nodeId == that.nodeId && nodeEpoch == that.nodeEpoch && Objects.equals(s3SSTObjects, that.s3SSTObjects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeEpoch, s3SSTObjects);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(new ApiMessageAndVersion(new NodeWALMetadataRecord()
            .setNodeId(nodeId)
            .setNodeEpoch(nodeEpoch), (short) 0));
        s3SSTObjects.values().forEach(wal -> {
            writer.write(wal.toRecord());
        });
    }

    public Map<Long, S3SSTObject> getSSTObjects() {
        return s3SSTObjects;
    }

    public SortedMap<Long, S3SSTObject> getOrderIndex() {
        return orderIndex;
    }

    public List<S3SSTObject> orderList() {
        return orderIndex.values().stream().collect(Collectors.toList());
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getNodeEpoch() {
        return nodeEpoch;
    }

    @Override
    public String toString() {
        return "NodeS3WALMetadataImage{" +
            "nodeId=" + nodeId +
            ", nodeEpoch=" + nodeEpoch +
            ", s3SSTObjects=" + s3SSTObjects +
            '}';
    }
}
