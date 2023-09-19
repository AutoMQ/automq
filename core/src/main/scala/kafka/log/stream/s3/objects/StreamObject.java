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

package kafka.log.stream.s3.objects;

import org.apache.kafka.common.message.CommitWALObjectRequestData;

import java.util.List;

public class StreamObject {
    private long objectId;
    private long objectSize;
    private long streamId;
    private long startOffset;
    private long endOffset;

    /**
     * The source objects' id of the stream object.
     */
    private List<Long> sourceObjectIds;

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public long getStreamId() {
        return streamId;
    }

    public void setStreamId(long streamId) {
        this.streamId = streamId;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public List<Long> getSourceObjectIds() {
        return sourceObjectIds;
    }

    public void setSourceObjectIds(List<Long> sourceObjectIds) {
        this.sourceObjectIds = sourceObjectIds;
    }

    public CommitWALObjectRequestData.StreamObject toStreamObjectInRequest() {
        return new CommitWALObjectRequestData.StreamObject()
                .setStreamId(streamId)
                .setObjectId(objectId)
                .setObjectSize(objectSize)
                .setStartOffset(startOffset)
                .setEndOffset(endOffset);
    }

    @Override
    public String toString() {
        return "StreamObject{" +
                "objectId=" + objectId +
                ", objectSize=" + objectSize +
                ", streamId=" + streamId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", sourceObjectIds=" + sourceObjectIds +
                '}';
    }
}
