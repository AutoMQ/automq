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

package org.apache.kafka.controller.stream;

import java.util.LinkedList;
import java.util.Queue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

/**
 * The S3ObjectControlManager manages all S3Object's lifecycle, such as apply, create, destroy, etc.
 */
public class S3ObjectControlManager {
    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;
    
    private final TimelineHashMap<Long/*objectId*/, S3Object> objectsMetadata;

    /**
     * The objectId of the next object to be applied. (start from 0)
     */
    private Long nextApplyObjectId = 0L;
    
    // TODO: add timer task to periodically check if there are objects to be destroyed or created
    private final Queue<Long/*objectId*/> appliedObjects;
    private final Queue<Long/*objectId*/> markDestroyedObjects;
    
    public S3ObjectControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(S3ObjectControlManager.class);
        this.objectsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.appliedObjects = new LinkedList<>();
        this.markDestroyedObjects = new LinkedList<>();
    }
    
    public Long appliedObjectNum() {
        return nextApplyObjectId;
    }
    
}
