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

import com.automq.stream.s3.Constants;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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

    public static final S3ObjectsImage EMPTY = new S3ObjectsImage(-1, null, null, Constants.NOOP_EPOCH, null);

    private long nextAssignedObjectId;

    private final TimelineHashMap<Long/*objectId*/, S3Object> objects;
    private final SnapshotRegistry registry;
    private final long epoch;
    private final List<Long> liveEpochs;

    public S3ObjectsImage(long assignedObjectId, final TimelineHashMap<Long, S3Object> objects,
        final SnapshotRegistry registry, final long epoch, final List<Long> liveEpochs) {
        this.nextAssignedObjectId = assignedObjectId + 1;
        this.objects = objects;
        this.registry = registry;
        this.epoch = epoch;
        this.liveEpochs = liveEpochs;
        if (epoch != Constants.NOOP_EPOCH) {
            if (liveEpochs == null) {
                throw new IllegalArgumentException("Require null liveEpochs for non-empty epoch");
            } else {
                this.liveEpochs.add(epoch);
            }
        }
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
        // the writer#write maybe slow, so we use a copy to avoid holding the lock for a long time
        List<S3Object> copy;
        synchronized (registry) {
            copy = new ArrayList<>(objects.values(epoch));
        }
        copy.forEach(v -> writer.write(v.toRecord()));
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

    List<Long> liveEpochs() {
        return liveEpochs;
    }

    @Override
    protected void deallocate() {
        if (registry != null) {
            synchronized (registry) {
                if (liveEpochs.isEmpty()) {
                    throw new IllegalStateException("liveEpochs is empty");
                }
                long oldFirst = liveEpochs.get(0);
                liveEpochs.remove(epoch);
                if (liveEpochs.isEmpty()) {
                    throw new IllegalStateException("liveEpochs is empty");
                }
                long newFirst = liveEpochs.get(0);
                if (newFirst != oldFirst) {
                    registry.deleteSnapshotsUpTo(newFirst);
                }
            }
        }
    }

    @Override
    public ReferenceCounted touch(Object o) {
        return this;
    }
}
