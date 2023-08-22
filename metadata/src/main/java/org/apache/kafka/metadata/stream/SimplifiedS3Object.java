package org.apache.kafka.metadata.stream;

import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

/**
 * Simplified S3 object metadata, only be used in metadata cache of broker.
 */
public class SimplifiedS3Object {
    private final long objectId;
    private final S3ObjectState state;

    public SimplifiedS3Object(final long objectId, final S3ObjectState state) {
        this.objectId = objectId;
        this.state = state;
    }

    public long objectId() {
        return objectId;
    }

    public S3ObjectState state() {
        return state;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3ObjectRecord().
            setObjectId(objectId).
            setObjectState((byte) state.ordinal()), (short) 0);
    }

    public static SimplifiedS3Object of(final S3ObjectRecord record) {
        return new SimplifiedS3Object(record.objectId(), S3ObjectState.fromByte(record.objectState()));
    }
}
