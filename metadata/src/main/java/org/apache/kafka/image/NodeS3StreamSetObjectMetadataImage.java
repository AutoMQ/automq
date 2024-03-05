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

import com.automq.stream.s3.metadata.S3StreamConstant;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class NodeS3StreamSetObjectMetadataImage {
    public static final NodeS3StreamSetObjectMetadataImage EMPTY = new NodeS3StreamSetObjectMetadataImage(S3StreamConstant.INVALID_BROKER_ID,
            S3StreamConstant.INVALID_BROKER_EPOCH, new DeltaMap<>(new int[]{100}));
    private final int nodeId;
    private final long nodeEpoch;
    private final DeltaMap<Long/*objectId*/, S3StreamSetObject> s3Objects;
    private List<S3StreamSetObject> orderIndex;

    public NodeS3StreamSetObjectMetadataImage(int nodeId, long nodeEpoch, DeltaMap<Long, S3StreamSetObject> streamSetObjects) {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.s3Objects = streamSetObjects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeS3StreamSetObjectMetadataImage that = (NodeS3StreamSetObjectMetadataImage) o;
        return nodeId == that.nodeId && nodeEpoch == that.nodeEpoch && Objects.equals(s3Objects, that.s3Objects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, nodeEpoch, s3Objects);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(new ApiMessageAndVersion(new NodeWALMetadataRecord()
                .setNodeId(nodeId)
                .setNodeEpoch(nodeEpoch), (short) 0));
        s3Objects.forEach((k, v) -> writer.write(v.toRecord()));
    }

    public DeltaMap<Long, S3StreamSetObject> getObjects() {
        return s3Objects;
    }

    public List<S3StreamSetObject> orderList() {
        if (orderIndex == null) {
            List<S3StreamSetObject> objects = new ArrayList<>();
            s3Objects.forEach((k, v) -> objects.add(v));
            objects.sort(Comparator.comparingLong(S3StreamSetObject::orderId));
            orderIndex = objects;
        }
        return orderIndex;
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
                ", objects=" + s3Objects +
                '}';
    }
}
