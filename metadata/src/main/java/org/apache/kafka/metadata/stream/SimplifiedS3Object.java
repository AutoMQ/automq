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
