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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.stream.s3.StreamObject;
import org.apache.kafka.controller.stream.s3.WALObject;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

public class StreamControlManager {

    class StreamMetadata {
        private Long streamId;
        private Integer epoch;
        private Long startOffset;
        private TimelineHashSet<RangeMetadata> ranges;
        private TimelineHashSet<StreamObject> streamObjects;
    }

    class BrokerStreamMetadata {
        private Integer brokerId;
        private TimelineHashSet<WALObject> walObjects;
    }

    private final SnapshotRegistry snapshotRegistry;

    private final Logger log;

    private final TimelineHashMap<Long/*streamId*/, StreamMetadata> streamsMetadata;

    private final TimelineHashMap<Integer/*brokerId*/, BrokerStreamMetadata> brokersMetadata;

    public StreamControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(StreamControlManager.class);
        this.streamsMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    

}
