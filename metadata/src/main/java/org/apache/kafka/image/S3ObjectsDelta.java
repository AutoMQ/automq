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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.metadata.RemoveS3ObjectRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.metadata.stream.SimplifiedS3Object;

/**
 * Represents changes to a S3 object in the metadata image.
 */
public final class S3ObjectsDelta {
    private final S3ObjectsImage image;

    private final Set<SimplifiedS3Object> addedObjects = new HashSet<>();

    private final Set<Long/*objectId*/> removedObjectIds = new HashSet<>();

    public S3ObjectsDelta(S3ObjectsImage image) {
        this.image = image;
    }

    public S3ObjectsImage image() {
        return image;
    }

    public Set<SimplifiedS3Object> addedObjects() {
        return addedObjects;
    }

    public Set<Long> removedObjects() {
        return removedObjectIds;
    }

    public void replay(S3ObjectRecord record) {
        addedObjects.add(SimplifiedS3Object.of(record));
        // new add or update, so remove from removedObjects
        removedObjectIds.remove(record.objectId());
    }

    public void replay(RemoveS3ObjectRecord record) {
        removedObjectIds.add(record.objectId());
        // new remove, so remove from addedObjects
        addedObjects.remove(record.objectId());
    }

    public S3ObjectsImage apply() {
        // get original objects first
        Map<Long, SimplifiedS3Object> newObjectsMetadata = new HashMap<>(image.objectsMetadata());
        // put all new added objects
        addedObjects.forEach(obj -> newObjectsMetadata.put(obj.objectId(), obj));
        // remove all removed objects
        removedObjectIds.forEach(newObjectsMetadata::remove);
        return new S3ObjectsImage(newObjectsMetadata);
    }

}
