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

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

/**
 * Represents the S3 objects in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public final class S3ObjectsImage extends AbstractReferenceCounted {

    public static final S3ObjectsImage EMPTY = new S3ObjectsImage(-1, null, null, -1L);

    private long nextAssignedObjectId;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objects;
    private final SnapshotRegistry registry;
    private final long epoch;

    public S3ObjectsImage(long assignedObjectId, final TimelineHashMap<Long, S3Object> objects,
        final SnapshotRegistry registry, final long epoch) {
        this.nextAssignedObjectId = assignedObjectId + 1;
        this.objects = objects;
        this.registry = registry;
        this.epoch = epoch;
    }

    public S3Object getObjectMetadata(long objectId) {
        return this.objects.get(objectId, epoch);
    }

    public long nextAssignedObjectId() {
        return nextAssignedObjectId;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (objects == null) {
            return;
        }
        writer.write(
            new ApiMessageAndVersion(
                new AssignedS3ObjectIdRecord().setAssignedS3ObjectId(nextAssignedObjectId - 1), (short) 0));
        objects.values(epoch).forEach(v -> writer.write(v.toRecord()));
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
        return this.nextAssignedObjectId == that.nextAssignedObjectId && objects().equals(that.objects());
    }

    public boolean isEmpty() {
        return objects == null || objects.isEmpty(epoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedObjectId, objects);
    }

    public Collection<Long> objectIds() {
        return objects.keySet(epoch);
    }

    Collection<S3Object> objects() {
        return objects == null ? Collections.emptyList() : new LinkedList<>(objects.values(epoch));
    }

    TimelineHashMap<Long, S3Object> timelineObjects() {
        return objects;
    }

    SnapshotRegistry registry() {
        return registry;
    }

    long epoch() {
        return epoch;
    }

    @Override
    protected void deallocate() {
        if (registry != null) {
            registry.deleteSnapshotsUpTo(epoch);
        }
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
