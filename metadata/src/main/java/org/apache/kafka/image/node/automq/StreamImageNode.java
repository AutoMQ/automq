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
