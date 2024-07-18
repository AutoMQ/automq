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

import org.apache.kafka.image.S3StreamMetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;

public class StreamImageNode implements MetadataNode {
    private final S3StreamMetadataImage image;

    public StreamImageNode(S3StreamMetadataImage image) {
        this.image = image;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public void print(MetadataNodePrinter printer) {
        StringBuilder sb = new StringBuilder();
        sb.append("[streamId=").append(image.getStreamId()).append(", epoch=").append(image.getEpoch()).append(", startOffset=")
            .append(image.getStartOffset()).append(", state=").append(image.state().toString());
        sb.append(", tags={");
        StringBuilder finalSb = sb;
        image.tags().forEach(tag -> finalSb.append(tag.key()).append("=").append(tag.value()).append(", "));
        sb.append("}]");
        printer.output(sb.toString());
        // ranges: index={nodeId, epoch, start, end} ...
        sb = new StringBuilder();
        sb.append("ranges: ");
        for (RangeMetadata range : image.getRanges()) {
            sb.append(range.rangeIndex()).append("={nodeId=").append(range.nodeId()).append(", epoch=").append(range.epoch())
                .append(", start=").append(range.startOffset()).append(", end=").append(range.endOffset()).append("} ");
        }
        printer.output(sb.toString());
        sb = new StringBuilder();
        sb.append("stream objects: objectId=[startOffset, endOffset) ");
        for (S3StreamObject object : image.getStreamObjects()) {
            sb.append(object.objectId()).append("=[").append(object.startOffset()).append(", ").append(object.endOffset()).append(")");
        }
        printer.output(sb.toString());
    }
}
