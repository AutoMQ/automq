/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.image.node.automq;

import com.automq.stream.s3.metadata.StreamOffsetRange;
import org.apache.kafka.image.NodeS3StreamSetObjectMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

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
