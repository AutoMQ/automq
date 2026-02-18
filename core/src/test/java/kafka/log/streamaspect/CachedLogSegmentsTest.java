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
import org.apache.kafka.storage.internals.log.LogSegment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class CachedLogSegmentsTest {

    private CachedLogSegments cachedLogSegments;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition("test-topic", 0);
        cachedLogSegments = new CachedLogSegments(topicPartition);
    }

    private LogSegment createMockSegment(long baseOffset) {
        LogSegment segment = Mockito.mock(LogSegment.class);
        when(segment.baseOffset()).thenReturn(baseOffset);
        when(segment.toString()).thenReturn("LogSegment(baseOffset=" + baseOffset + ")");
        return segment;
    }

    @Test
    @DisplayName("Initial state should have null activeSegment")
    void testInitialState() {
        assertNull(cachedLogSegments.activeSegment());
        assertFalse(cachedLogSegments.lastSegment().isPresent());
        assertTrue(cachedLogSegments.isEmpty());
    }

    @Test
    @DisplayName("Adding first segment should set it as activeSegment")
    void testAddFirstSegment() {
        LogSegment segment0 = createMockSegment(0);

        LogSegment added = cachedLogSegments.add(segment0);

        assertEquals(null, added); // Returns null as there was no previous segment at this offset
        assertEquals(segment0, cachedLogSegments.activeSegment());
        assertEquals(segment0, cachedLogSegments.lastSegment().get());
        assertEquals(1, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Adding segments in order should update activeSegment to highest offset")
    void testAddSegmentsInOrder() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment200 = createMockSegment(200);

        cachedLogSegments.add(segment0);
        assertEquals(segment0, cachedLogSegments.activeSegment());

        cachedLogSegments.add(segment100);
        assertEquals(segment100, cachedLogSegments.activeSegment());

        cachedLogSegments.add(segment200);
        assertEquals(segment200, cachedLogSegments.activeSegment());

        assertEquals(3, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Adding segments out of order should maintain correct activeSegment")
    void testAddSegmentsOutOfOrder() {
        LogSegment segment200 = createMockSegment(200);
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment300 = createMockSegment(300);

        // Add highest first
        cachedLogSegments.add(segment200);
        assertEquals(segment200, cachedLogSegments.activeSegment());

        // Add lower offsets - should not change activeSegment
        cachedLogSegments.add(segment0);
        assertEquals(segment200, cachedLogSegments.activeSegment());

        cachedLogSegments.add(segment100);
        assertEquals(segment200, cachedLogSegments.activeSegment());

        // Add higher offset - should update activeSegment
        cachedLogSegments.add(segment300);
        assertEquals(segment300, cachedLogSegments.activeSegment());

        assertEquals(4, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Adding segment with same offset as current active should update activeSegment")
    void testAddSegmentWithSameOffsetAsActive() {
        LogSegment segment100Old = createMockSegment(100);
        LogSegment segment100New = createMockSegment(100);

        cachedLogSegments.add(segment100Old);
        assertEquals(segment100Old, cachedLogSegments.activeSegment());

        // Adding new segment with same offset should replace and update activeSegment
        LogSegment replaced = cachedLogSegments.add(segment100New);
        assertEquals(segment100Old, replaced); // Returns the old segment that was replaced
        assertEquals(segment100New, cachedLogSegments.activeSegment());
    }

    @Test
    @DisplayName("Removing non-active segment should not affect activeSegment")
    void testRemoveNonActiveSegment() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment200 = createMockSegment(200);

        cachedLogSegments.add(segment0);
        cachedLogSegments.add(segment100);
        cachedLogSegments.add(segment200);

        assertEquals(segment200, cachedLogSegments.activeSegment());

        // Remove non-active segment
        cachedLogSegments.remove(100);

        assertEquals(segment200, cachedLogSegments.activeSegment());
        assertEquals(2, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Removing active segment should update activeSegment to last remaining segment")
    void testRemoveActiveSegment() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment200 = createMockSegment(200);

        cachedLogSegments.add(segment0);
        cachedLogSegments.add(segment100);
        cachedLogSegments.add(segment200);

        assertEquals(segment200, cachedLogSegments.activeSegment());

        // Remove active segment
        cachedLogSegments.remove(200);

        assertEquals(segment100, cachedLogSegments.activeSegment());
        assertEquals(2, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Removing last remaining segment should set activeSegment to null")
    void testRemoveLastSegment() {
        LogSegment segment0 = createMockSegment(0);

        cachedLogSegments.add(segment0);
        assertEquals(segment0, cachedLogSegments.activeSegment());

        cachedLogSegments.remove(0);

        assertNull(cachedLogSegments.activeSegment());
        assertFalse(cachedLogSegments.lastSegment().isPresent());
        assertEquals(0, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Removing non-existent segment should not affect state")
    void testRemoveNonExistentSegment() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);

        cachedLogSegments.add(segment0);
        cachedLogSegments.add(segment100);

        assertEquals(segment100, cachedLogSegments.activeSegment());

        // Try to remove non-existent segment
        cachedLogSegments.remove(200);

        assertEquals(segment100, cachedLogSegments.activeSegment());
        assertEquals(2, cachedLogSegments.numberOfSegments());
    }

    @Test
    @DisplayName("Clear should reset all state")
    void testClear() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment200 = createMockSegment(200);

        cachedLogSegments.add(segment0);
        cachedLogSegments.add(segment100);
        cachedLogSegments.add(segment200);

        assertEquals(3, cachedLogSegments.numberOfSegments());
        assertEquals(segment200, cachedLogSegments.activeSegment());

        cachedLogSegments.clear();

        assertNull(cachedLogSegments.activeSegment());
        assertFalse(cachedLogSegments.lastSegment().isPresent());
        assertEquals(0, cachedLogSegments.numberOfSegments());
        assertTrue(cachedLogSegments.isEmpty());
    }

    @Test
    @DisplayName("lastSegment should always return activeSegment when segments exist")
    void testLastSegmentConsistency() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment100 = createMockSegment(100);

        // Empty state
        assertFalse(cachedLogSegments.lastSegment().isPresent());
        assertNull(cachedLogSegments.activeSegment());

        // Single segment
        cachedLogSegments.add(segment0);
        assertEquals(cachedLogSegments.activeSegment(), cachedLogSegments.lastSegment().get());

        // Multiple segments
        cachedLogSegments.add(segment100);
        assertEquals(cachedLogSegments.activeSegment(), cachedLogSegments.lastSegment().get());

        // After removal
        cachedLogSegments.remove(100);
        assertEquals(cachedLogSegments.activeSegment(), cachedLogSegments.lastSegment().get());
    }

    @Test
    @DisplayName("Complex sequence of operations should maintain consistency")
    void testComplexOperationSequence() {
        LogSegment segment0 = createMockSegment(0);
        LogSegment segment50 = createMockSegment(50);
        LogSegment segment100 = createMockSegment(100);
        LogSegment segment150 = createMockSegment(150);
        LogSegment segment200 = createMockSegment(200);

        // Build up segments
        cachedLogSegments.add(segment100);
        assertEquals(segment100, cachedLogSegments.activeSegment());

        cachedLogSegments.add(segment0);
        assertEquals(segment100, cachedLogSegments.activeSegment()); // 100 > 0

        cachedLogSegments.add(segment200);
        assertEquals(segment200, cachedLogSegments.activeSegment()); // 200 > 100

        cachedLogSegments.add(segment50);
        assertEquals(segment200, cachedLogSegments.activeSegment()); // 200 > 50

        cachedLogSegments.add(segment150);
        assertEquals(segment200, cachedLogSegments.activeSegment()); // 200 > 150

        assertEquals(5, cachedLogSegments.numberOfSegments());

        // Remove some segments
        cachedLogSegments.remove(50);
        assertEquals(segment200, cachedLogSegments.activeSegment());
        assertEquals(4, cachedLogSegments.numberOfSegments());

        cachedLogSegments.remove(200); // Remove active
        assertEquals(segment150, cachedLogSegments.activeSegment());
        assertEquals(3, cachedLogSegments.numberOfSegments());

        cachedLogSegments.remove(0);
        assertEquals(segment150, cachedLogSegments.activeSegment());
        assertEquals(2, cachedLogSegments.numberOfSegments());

        cachedLogSegments.remove(150); // Remove active again
        assertEquals(segment100, cachedLogSegments.activeSegment());
        assertEquals(1, cachedLogSegments.numberOfSegments());

        cachedLogSegments.remove(100); // Remove last
        assertNull(cachedLogSegments.activeSegment());
        assertEquals(0, cachedLogSegments.numberOfSegments());
    }

}
