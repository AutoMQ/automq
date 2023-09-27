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
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.metadata.stream.S3WALObject;

public class NodeS3WALMetadataDelta {

    private final NodeS3WALMetadataImage image;
    private int nodeId;
    private long nodeEpoch;
    private final Map<Long/*objectId*/, S3WALObject> addedS3WALObjects = new HashMap<>();

    private final Set<Long/*objectId*/> removedS3WALObjects = new HashSet<>();

    public NodeS3WALMetadataDelta(NodeS3WALMetadataImage image) {
        this.image = image;
        this.nodeId = image.getNodeId();
        this.nodeEpoch = image.getNodeEpoch();
    }

    public void replay(NodeWALMetadataRecord record) {
        this.nodeId = record.nodeId();
        this.nodeEpoch = record.nodeEpoch();
    }

    public void replay(WALObjectRecord record) {
        addedS3WALObjects.put(record.objectId(), S3WALObject.of(record));
        // new add or update, so remove from removedObjects
        removedS3WALObjects.remove(record.objectId());
    }

    public void replay(RemoveWALObjectRecord record) {
        removedS3WALObjects.add(record.objectId());
        // new remove, so remove from addedObjects
        addedS3WALObjects.remove(record.objectId());
    }

    public NodeS3WALMetadataImage apply() {
        Map<Long, S3WALObject> newS3WALObjects = new HashMap<>(image.getWalObjects());
        // add all changed WAL objects
        newS3WALObjects.putAll(addedS3WALObjects);
        // remove all removed WAL objects
        removedS3WALObjects.forEach(newS3WALObjects::remove);
        return new NodeS3WALMetadataImage(this.nodeId, this.nodeEpoch, newS3WALObjects);
    }

}
