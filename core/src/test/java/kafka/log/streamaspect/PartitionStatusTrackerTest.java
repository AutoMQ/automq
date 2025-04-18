/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
