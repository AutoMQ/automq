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

import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class NodeS3WALMetadataDelta {

    private final NodeS3StreamSetObjectMetadataImage image;
    private int nodeId;
    private long nodeEpoch;
    private final Map<Long/*objectId*/, S3StreamSetObject> addedS3StreamSetObjects = new HashMap<>();

    private final Set<Long/*objectId*/> removedS3StreamSetObjects = new HashSet<>();
    private boolean snapshotReplay = false;

    public NodeS3WALMetadataDelta(NodeS3StreamSetObjectMetadataImage image) {
        this.image = image;
        this.nodeId = image.getNodeId();
        this.nodeEpoch = image.getNodeEpoch();
    }

    public void replay(NodeWALMetadataRecord record) {
        this.nodeId = record.nodeId();
        this.nodeEpoch = record.nodeEpoch();
    }

    public void replay(S3StreamSetObjectRecord record) {
        addedS3StreamSetObjects.put(record.objectId(), S3StreamSetObject.of(record));
        // new add or update, so remove from removedObjects
        removedS3StreamSetObjects.remove(record.objectId());
    }

    public void replay(RemoveStreamSetObjectRecord record) {
        removedS3StreamSetObjects.add(record.objectId());
        // new remove, so remove from addedObjects
        addedS3StreamSetObjects.remove(record.objectId());
    }

    public void finishSnapshot() {
        snapshotReplay = true;
        image.getObjects().toList().forEach(object -> {
            if (!addedS3StreamSetObjects.containsKey(object.objectId())) {
                removedS3StreamSetObjects.add(object.objectId());
            }
        });
    }

    @VisibleForTesting
    Map<Long, S3StreamSetObject> addedS3StreamSetObjects() {
        return addedS3StreamSetObjects;
    }

    @VisibleForTesting
    Set<Long> removedS3StreamSetObjects() {
        return removedS3StreamSetObjects;
    }

    public NodeS3StreamSetObjectMetadataImage apply() {
        DeltaList<S3StreamSetObject> streamSetObjects;
        if (addedS3StreamSetObjects.isEmpty() && removedS3StreamSetObjects.isEmpty()) {
            streamSetObjects = image.getObjects();
        } else if (snapshotReplay) {
            // Snapshot replay is a full-state replacement over a non-empty image. Rebuild
            // a clean list by object id so old DeltaList tombstones cannot hide updated
            // stream set objects with the same object id from forEach/orderList readers.
            Map<Long, S3StreamSetObject> objects = new HashMap<>();
            image.getObjects().toList().forEach(object -> objects.put(object.objectId(), object));
            removedS3StreamSetObjects.forEach(objects::remove);
            addedS3StreamSetObjects.forEach(objects::put);
            streamSetObjects = new DeltaList<>(new ArrayList<>(objects.values()));
        } else {
            streamSetObjects = image.getObjects().copy();
            addedS3StreamSetObjects.values().forEach(streamSetObjects::add);
            removedS3StreamSetObjects.forEach(id -> streamSetObjects.remove(obj -> Objects.equals(obj.objectId(), id)));
        }
        return new NodeS3StreamSetObjectMetadataImage(this.nodeId, this.nodeEpoch, streamSetObjects);
    }

}
