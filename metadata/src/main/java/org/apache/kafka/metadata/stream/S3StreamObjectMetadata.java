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

import java.util.Objects;

public class S3StreamObjectMetadata implements Comparable<S3StreamObjectMetadata> {
    private final S3StreamObject s3StreamObject;
    private final long timestamp;

    public S3StreamObjectMetadata(S3StreamObject s3StreamObject, long timestamp) {
        this.s3StreamObject = s3StreamObject;
        this.timestamp = timestamp;
    }

    public long startOffset() {
        return s3StreamObject.streamOffsetRange().getStartOffset();
    }

    public long endOffset() {
        return s3StreamObject.streamOffsetRange().getEndOffset();
    }

    public long streamId() {
        return s3StreamObject.streamOffsetRange().getStreamId();
    }

    public long objectId() {
        return s3StreamObject.objectId();
    }

    public long objectSize() {
        return s3StreamObject.objectSize();
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(S3StreamObjectMetadata o) {
        return s3StreamObject.streamOffsetRange().compareTo(o.s3StreamObject.streamOffsetRange());
    }

    @Override
    public int hashCode() {
        return Objects.hash(s3StreamObject, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3StreamObjectMetadata)) return false;
        S3StreamObjectMetadata that = (S3StreamObjectMetadata) o;
        return s3StreamObject.equals(that.s3StreamObject) && timestamp == that.timestamp;
    }
}
