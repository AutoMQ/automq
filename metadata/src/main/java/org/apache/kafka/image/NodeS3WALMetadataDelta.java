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
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.RemoveSSTObjectRecord;
import org.apache.kafka.common.metadata.S3SSTObjectRecord;
import org.apache.kafka.metadata.stream.S3SSTObject;

public class NodeS3WALMetadataDelta {

    private final NodeS3SSTMetadataImage image;
    private int nodeId;
    private long nodeEpoch;
    private final Map<Long/*objectId*/, S3SSTObject> addedS3SSTObjects = new HashMap<>();

    private final Set<Long/*objectId*/> removedS3SSTObjects = new HashSet<>();

    public NodeS3WALMetadataDelta(NodeS3SSTMetadataImage image) {
        this.image = image;
        this.nodeId = image.getNodeId();
        this.nodeEpoch = image.getNodeEpoch();
    }

    public void replay(NodeWALMetadataRecord record) {
        this.nodeId = record.nodeId();
        this.nodeEpoch = record.nodeEpoch();
    }

    public void replay(S3SSTObjectRecord record) {
        addedS3SSTObjects.put(record.objectId(), S3SSTObject.of(record));
        // new add or update, so remove from removedObjects
        removedS3SSTObjects.remove(record.objectId());
    }

    public void replay(RemoveSSTObjectRecord record) {
        removedS3SSTObjects.add(record.objectId());
        // new remove, so remove from addedObjects
        addedS3SSTObjects.remove(record.objectId());
    }

    public NodeS3SSTMetadataImage apply() {
        Map<Long, S3SSTObject> newS3SSTObjects = new HashMap<>(image.getSSTObjects());
        // add all changed SST objects
        newS3SSTObjects.putAll(addedS3SSTObjects);
        // remove all removed SST objects
        removedS3SSTObjects.forEach(newS3SSTObjects::remove);
        return new NodeS3SSTMetadataImage(this.nodeId, this.nodeEpoch, newS3SSTObjects);
    }

}
