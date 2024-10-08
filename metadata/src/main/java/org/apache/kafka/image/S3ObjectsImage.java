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

import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

/**
 * Represents the S3 objects in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public final class S3ObjectsImage extends AbstractReferenceCounted {

    public static final S3ObjectsImage EMPTY = new S3ObjectsImage(-1, new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), RegistryRef.NOOP);

    private final long nextAssignedObjectId;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objects;
    private final RegistryRef registryRef;

    public S3ObjectsImage(long assignedObjectId,
                          final TimelineHashMap<Long, S3Object> objects,
                          final RegistryRef registry) {
        this.nextAssignedObjectId = assignedObjectId + 1;
        this.objects = objects;
        this.registryRef = registry;
    }

    public S3Object getObjectMetadata(long objectId) {
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return null;
        }

        return registryRef.inLock(() -> this.objects.get(objectId, registryRef.epoch()));
    }

    public long nextAssignedObjectId() {
        return nextAssignedObjectId;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return;
        }

        writer.write(
            new ApiMessageAndVersion(
                new AssignedS3ObjectIdRecord().setAssignedS3ObjectId(nextAssignedObjectId - 1), (short) 0));
        // the writer#write maybe slow, so we use a copy to avoid holding the lock for a long time
        List<S3Object> copy = registryRef.inLock(() -> new ArrayList<>(objects.values(registryRef.epoch())));

        copy.forEach(v -> writer.write(v.toRecord(options.metadataVersion().autoMQVersion())));
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
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return true;
        }

        return registryRef.inLock(() -> objects.isEmpty(registryRef.epoch()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextAssignedObjectId);
    }

    public Collection<Long> objectIds() {
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyList();
        }

        return registryRef.inLock(() -> objects.keySet(registryRef.epoch()));
    }

    public int objectsCount() {
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return 0;
        }

        return registryRef.inLock(() -> objects.size(registryRef.epoch()));
    }

    Collection<S3Object> objects() {
        if (objects == null || registryRef == RegistryRef.NOOP) {
            return Collections.emptyList();
        }

        return registryRef.inLock(() -> new LinkedList<>(objects.values(registryRef.epoch())));
    }

    TimelineHashMap<Long, S3Object> timelineObjects() {
        return objects;
    }

    RegistryRef registryRef() {
        return registryRef;
    }

    @Override
    protected void deallocate() {
        if (registryRef == RegistryRef.NOOP) {
            return;
        }
        registryRef.release();
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
