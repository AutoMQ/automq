/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.log.streamaspect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class PartitionStatusTrackerTest {

    @Test
    public void testClose_duplicated() {
        PartitionStatusTracker tracker = new PartitionStatusTracker(new MockTime(), tp -> {
        });
        PartitionStatusTracker.Tracker tracker1 = tracker.tracker(new TopicPartition("test", 1));
        PartitionStatusTracker.Tracker tracker2 = tracker.tracker(new TopicPartition("test", 1));
        assertEquals(tracker1, tracker2);
        tracker1.release();
        tracker1.closed();
        assertEquals(2, tracker2.refCnt());
        assertEquals(1, tracker.trackers.size());
        tracker2.release();
        tracker2.closed();
        assertEquals(0, tracker2.refCnt());
        assertEquals(0, tracker.trackers.size());
    }

}
