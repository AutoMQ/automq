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

package org.apache.kafka.controller.stream.s3;

import org.apache.kafka.common.metadata.StreamObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class StreamObject extends S3Object {

    private ObjectStreamIndex streamIndex;

    public StreamObject(final Long objectId) {
        super(objectId);
    }

    @Override
    public void onCreate(S3ObjectCreateContext createContext) {
        super.onCreate(createContext);
        this.streamIndex = ((StreamObjectCreateContext) createContext).streamIndex;
    }

    @Override
    public int compareTo(S3Object o) {
        if (!(o instanceof StreamObject)) {
            throw new IllegalArgumentException("Cannot compare StreamObject with non-StreamObject");
        }
        StreamObject streamObject = (StreamObject) o;
        // order by streamId first, then startOffset
        int res = this.streamIndex.getStreamId().compareTo(streamObject.streamIndex.getStreamId());
        return res == 0 ? this.streamIndex.getStartOffset().compareTo(streamObject.streamIndex.getStartOffset()) : res;
    }

    class StreamObjectCreateContext extends S3ObjectCreateContext {

        private final ObjectStreamIndex streamIndex;

        public StreamObjectCreateContext(
            final Long createTimeInMs,
            final Long objectSize,
            final String objectAddress,
            final S3ObjectType objectType,
            final ObjectStreamIndex streamIndex) {
            super(createTimeInMs, objectSize, objectAddress, objectType);
            this.streamIndex = streamIndex;
        }
    }

    public ObjectStreamIndex getStreamIndex() {
        return streamIndex;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new StreamObjectRecord()
            .setObjectId(objectId)
            .setStreamId(streamIndex.getStreamId())
            .setObjectState((byte)objectState.ordinal())
            .setObjectType((byte)objectType.ordinal())
            .setApplyTimeInMs(applyTimeInMs.get())
            .setCreateTimeInMs(createTimeInMs.get())
            .setDestroyTimeInMs(destroyTimeInMs.get())
            .setObjectSize(objectSize.get())
            .setStartOffset(streamIndex.getStartOffset())
            .setEndOffset(streamIndex.getEndOffset()), (short) 0);
    }
}
