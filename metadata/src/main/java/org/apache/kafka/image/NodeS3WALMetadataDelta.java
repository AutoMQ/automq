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
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveStreamSetObjectRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.metadata.stream.S3StreamSetObject;

public class NodeS3WALMetadataDelta {

    private final NodeS3StreamSetObjectMetadataImage image;
    private int nodeId;
    private long nodeEpoch;
    private final Map<Long/*objectId*/, S3StreamSetObject> addedS3StreamSetObjects = new HashMap<>();

    private final Set<Long/*objectId*/> removedS3StreamSetObjects = new HashSet<>();

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

    public NodeS3StreamSetObjectMetadataImage apply() {
        DeltaList<S3StreamSetObject> streamSetObjects = image.getObjects().copy();
        // add all changed stream set objects
        addedS3StreamSetObjects.forEach((id, obj) -> streamSetObjects.add(obj));
        // remove all removed stream set objects
        removedS3StreamSetObjects.forEach(id -> streamSetObjects.remove(obj -> Objects.equals(obj.objectId(), id)));
        return new NodeS3StreamSetObjectMetadataImage(this.nodeId, this.nodeEpoch, streamSetObjects);
    }

}
