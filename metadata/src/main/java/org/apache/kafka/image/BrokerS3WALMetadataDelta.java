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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.metadata.RemoveWALObjectRecord;
import org.apache.kafka.common.metadata.WALObjectRecord;
import org.apache.kafka.metadata.stream.S3WALObject;

public class BrokerS3WALMetadataDelta {

    private final BrokerS3WALMetadataImage image;
    private final Set<S3WALObject> changedS3WALObjects = new HashSet<>();

    private final Set<S3WALObject/*objectId*/> removedS3WALObjects = new HashSet<>();

    public BrokerS3WALMetadataDelta(BrokerS3WALMetadataImage image) {
        this.image = image;
    }

    public void replay(WALObjectRecord record) {
        changedS3WALObjects.add(S3WALObject.of(record));
    }

    public void replay(RemoveWALObjectRecord record) {
        removedS3WALObjects.add(new S3WALObject(record.objectId()));
    }

    public BrokerS3WALMetadataImage apply() {
        List<S3WALObject> newS3WALObjects = new ArrayList<>(image.getWalObjects());
        // remove all removed WAL objects
        newS3WALObjects.removeAll(removedS3WALObjects);
        // add all changed WAL objects
        newS3WALObjects.addAll(changedS3WALObjects);
        return new BrokerS3WALMetadataImage(image.getBrokerId(), newS3WALObjects);
    }

}
