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

package org.apache.kafka.metadata.stream;

import com.automq.stream.s3.metadata.S3ObjectMetadata;

import java.util.List;
import java.util.Objects;

public class InRangeObjects {

    public static final InRangeObjects INVALID = new InRangeObjects(-1, -1, -1, List.of());

    private final long streamId;
    private final long startOffset;
    private final long endOffset;
    private final List<S3ObjectMetadata> objects;

    public InRangeObjects(long streamId, long startOffset, long endOffset, List<S3ObjectMetadata> objects) {
        this.streamId = streamId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.objects = objects;
    }

    public long streamId() {
        return streamId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public List<S3ObjectMetadata> objects() {
        return objects;
    }

    @Override
    public String toString() {
        return "InRangeObjects{" +
            "streamId=" + streamId +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", objects=" + objects +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InRangeObjects that = (InRangeObjects) o;
        return streamId == that.streamId
            && startOffset == that.startOffset
            && endOffset == that.endOffset
            && objects.equals(that.objects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, startOffset, endOffset, objects);
    }
}
