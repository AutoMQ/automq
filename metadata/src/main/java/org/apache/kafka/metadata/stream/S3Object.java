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

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.objects.ObjectAttributes;
import java.util.Objects;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.metadata.stream.S3ObjectState.PREPARED;

/**
 * S3Object is the base class of object in S3. Manages the lifecycle of S3Object.
 */
public class S3Object implements Comparable<S3Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Object.class);

    private final long objectId;

    private long objectSize = -1;

    /**
     * The timestamp of the object:
     * - If state = PREPARED, it's the deadline timestamp.
     * - If state = MARK_DESTROYED, it's the mark destroyed timestamp.
     */
    private long timestamp;

    private S3ObjectState s3ObjectState = S3ObjectState.UNINITIALIZED;

    private int attributes;

    public S3Object(
        final long objectId,
        final long objectSize,
        final long timestamp,
        final S3ObjectState s3ObjectState,
        final int attributes) {
        this.objectId = objectId;
        this.objectSize = objectSize;
        this.timestamp = timestamp;
        this.s3ObjectState = s3ObjectState;
        this.attributes = attributes;
    }

    public ApiMessageAndVersion toRecord(AutoMQVersion version) {
        S3ObjectRecord record = new S3ObjectRecord()
            .setObjectId(objectId)
            .setObjectSize(objectSize)
            .setObjectState(s3ObjectState.toByte());
        if (version.isHugeClusterSupported()) {
            record.setTimestamp(timestamp);
        } else {
            switch (S3ObjectState.fromByte(record.objectState())) {
                case PREPARED:
                    record.setExpiredTimeInMs(timestamp);
                    break;
                case MARK_DESTROYED:
                    record.setMarkDestroyedTimeInMs(timestamp);
                    break;
                case COMMITTED:
                    record.setCommittedTimeInMs(timestamp);
                    break;
                default:
                    LOGGER.error("Invalid S3ObjectState: {}", S3ObjectState.fromByte(record.objectState()));
            }
        }
        if (version.isObjectAttributesSupported()) {
            record.setAttributes(attributes);
        }
        return new ApiMessageAndVersion(record, version.objectRecordVersion());
    }

    public static S3Object of(S3ObjectRecord record) {
        long timestamp = 0;
        if (record.timestamp() != 0) {
            timestamp = record.timestamp();
        } else {
            switch (S3ObjectState.fromByte(record.objectState())) {
                case PREPARED:
                    timestamp = record.expiredTimeInMs();
                    break;
                case MARK_DESTROYED:
                    timestamp = record.markDestroyedTimeInMs();
                    break;
                case COMMITTED:
                    timestamp = record.committedTimeInMs();
                    break;
                default:
                    LOGGER.error("Invalid S3ObjectState: {}", S3ObjectState.fromByte(record.objectState()));
            }
        }
        return new S3Object(
            record.objectId(),
            record.objectSize(),
            timestamp,
            S3ObjectState.fromByte(record.objectState()),
            record.attributes()
        );
    }

    @Override
    public int compareTo(S3Object o) {
        return Long.compare(this.objectId, o.objectId);
    }

    public long getObjectId() {
        return objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public String getObjectKey() {
        return ObjectUtils.genKey(0, objectId);
    }

    public S3ObjectState getS3ObjectState() {
        return s3ObjectState;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isExpired(long now) {
        return this.s3ObjectState == PREPARED && now > timestamp;
    }

    public int getAttributes() {
        return attributes;
    }

    public short bucket() {
        return ObjectAttributes.from(attributes).bucket();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        S3Object s3Object = (S3Object) o;
        return objectId == s3Object.objectId && objectSize == s3Object.objectSize && timestamp == s3Object.timestamp && attributes == s3Object.attributes && s3ObjectState == s3Object.s3ObjectState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, objectSize, timestamp, s3ObjectState, attributes);
    }

    @Override
    public String toString() {
        return "S3Object{" +
            "objectId=" + objectId +
            ", objectSize=" + objectSize +
            ", timestamp=" + timestamp +
            ", s3ObjectState=" + s3ObjectState +
            ", attributes=" + attributes +
            '}';
    }
}
