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

package org.apache.kafka.image.node.automq;

import org.apache.kafka.image.NodeS3StreamSetObjectMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

import com.automq.stream.s3.metadata.StreamOffsetRange;

public class NodeImageNode implements MetadataNode {
    private final NodeS3StreamSetObjectMetadataImage image;

    public NodeImageNode(NodeS3StreamSetObjectMetadataImage image) {
        this.image = image;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public void print(MetadataNodePrinter printer) {
        StringBuilder sb = new StringBuilder();
        sb.append("[nodeId=").append(image.getNodeId()).append(", epoch=").append(image.getNodeEpoch()).append("]");
        printer.output(sb.toString());
        for (S3StreamSetObject object : image.orderList()) {
            sb = new StringBuilder();
            sb.append("objectId=").append(object.objectId()).append(", ranges=[");
            for (StreamOffsetRange range : object.offsetRangeList()) {
                sb.append(range.streamId()).append(":").append(range.startOffset()).append("-").append(range.endOffset()).append(", ");
            }
            sb.append("]");
            printer.output(sb.toString());
        }
    }
}
