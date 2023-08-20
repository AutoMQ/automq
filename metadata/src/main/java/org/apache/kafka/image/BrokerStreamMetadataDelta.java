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
import org.apache.kafka.metadata.stream.WALObject;

public class BrokerStreamMetadataDelta {

    private final BrokerStreamMetadataImage image;
    private final Set<WALObject> changedWALObjects = new HashSet<>();

    private final Set<WALObject/*objectId*/> removedWALObjects = new HashSet<>();

    public BrokerStreamMetadataDelta(BrokerStreamMetadataImage image) {
        this.image = image;
    }

    public void replay(WALObjectRecord record) {
        changedWALObjects.add(WALObject.of(record));
    }

    public void replay(RemoveWALObjectRecord record) {
        removedWALObjects.add(new WALObject(record.objectId()));
    }

    public BrokerStreamMetadataImage apply() {
        List<WALObject> newWALObjects = new ArrayList<>(image.getWalObjects());
        // remove all removed WAL objects
        newWALObjects.removeAll(removedWALObjects);
        // add all changed WAL objects
        newWALObjects.addAll(changedWALObjects);
        return new BrokerStreamMetadataImage(image.getBrokerId(), newWALObjects);
    }

}
