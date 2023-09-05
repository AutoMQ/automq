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

package org.apache.kafka.image;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.server.common.ApiMessageAndVersion;

/**
 * Represents the S3 objects in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public final class S3ObjectsImage {

    public static final S3ObjectsImage EMPTY =
        new S3ObjectsImage(-1, Collections.emptyMap());

    private long nextAssignedObjectId;

    private final Map<Long/*objectId*/, S3Object> objectsMetadata;

    public S3ObjectsImage(long assignedObjectId, final Map<Long, S3Object> objectsMetadata) {
        this.nextAssignedObjectId = assignedObjectId + 1;
        this.objectsMetadata = objectsMetadata;
    }

    public S3Object getObjectMetadata(long objectId) {
        return this.objectsMetadata.get(objectId);
    }

    public Map<Long, S3Object> objectsMetadata() {
        return objectsMetadata;
    }

    public long nextAssignedObjectId() {
        return nextAssignedObjectId;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(
            new ApiMessageAndVersion(
                new AssignedS3ObjectIdRecord().setAssignedS3ObjectId(nextAssignedObjectId - 1), (short) 0));
        objectsMetadata.values().stream().map(S3Object::toRecord).forEach(writer::write);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3ObjectsImage that = (S3ObjectsImage) o;
        return this.nextAssignedObjectId == that.nextAssignedObjectId &&
            objectsMetadata.equals(that.objectsMetadata);
    }

    public boolean isEmpty() {
        return objectsMetadata.isEmpty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedObjectId, objectsMetadata);
    }
}
