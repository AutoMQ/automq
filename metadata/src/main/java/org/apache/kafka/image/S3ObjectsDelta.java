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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.metadata.AssignedS3ObjectIdRecord;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents changes to a S3 object in the metadata image.
 */
public final class S3ObjectsDelta {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ObjectsDelta.class);

    private final S3ObjectsImage image;

    private final Map<Long/*objectId*/, S3Object> changedObjects = new HashMap<>();

    private final Set<Long/*objectId*/> removedObjectIds = new HashSet<>();

    private long currentAssignedObjectId;

    public S3ObjectsDelta(S3ObjectsImage image) {
        this.image = image;
        this.currentAssignedObjectId = image.nextAssignedObjectId() - 1;
    }

    public Map<Long, S3Object> changedObjects() {
        return changedObjects;
    }

    public Set<Long> removedObjects() {
        return removedObjectIds;
    }

    public void replay(AssignedS3ObjectIdRecord record) {
        currentAssignedObjectId = record.assignedS3ObjectId();
    }

    public void replay(S3ObjectRecord record) {
        changedObjects.put(record.objectId(), S3Object.of(record));
        // new add or update, so remove from removedObjects
        removedObjectIds.remove(record.objectId());
    }

    public void replay(RemoveS3ObjectRecord record) {
        removedObjectIds.add(record.objectId());
        // new remove, so remove from addedObjects
        changedObjects.remove(record.objectId());
    }

    public S3ObjectsImage apply() {
        // get original objects first
        TimelineHashMap<Long, S3Object> newObjects = image.timelineObjects();
        SnapshotRegistry registry = image.registry();
        List<Long> liveEpochs = image.liveEpochs();
        if (newObjects == null) {
            registry = new SnapshotRegistry(new LogContext());
            newObjects = new TimelineHashMap<>(registry, 100000);
            liveEpochs = new ArrayList<>();
        }
        long newEpoch = image.epoch() + 1;
        synchronized (registry) {
            if (registry.latestEpoch() > image.epoch()) {
                LOGGER.error("Don't expect duplicated apply in non-test code");
                registry.revertToSnapshot(image.epoch());
            }

            // put all new changed objects
            newObjects.putAll(changedObjects);
            // remove all removed objects
            removedObjectIds.forEach(newObjects::remove);
            registry.getOrCreateSnapshot(newEpoch);
        }
        return new S3ObjectsImage(currentAssignedObjectId, newObjects, registry, newEpoch, liveEpochs);
    }

}
