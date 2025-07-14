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

package org.apache.kafka.controller.stream;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.automq.stream.s3.metadata.StreamState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class StreamRuntimeMetadataTest {
    private static final long STREAM_ID = 233L;
    private static final long EPOCH = 1L;
    StreamRuntimeMetadata metadata;

    @BeforeEach
    public void setup() {
        metadata = new StreamRuntimeMetadata(STREAM_ID, EPOCH, 3, 0,
            StreamState.OPENED, Collections.emptyMap(), new SnapshotRegistry(new LogContext()));
    }

    @Test
    public void testCheckRemovableRanges() {
        for (int i = 0; i < 4; i++) {
            metadata.ranges().put(i, new RangeMetadata(STREAM_ID, EPOCH, i, i * 100, (i + 1) * 100, i));
        }
        // The stream objects cover data ranges [0, 220)
        for (int i = 0; i < 11; i++) {
            metadata.streamObjects().put((long) i, new S3StreamObject(i, STREAM_ID, i * 20, (i + 1) * 20));
        }
        List<RangeMetadata> ranges = metadata.checkRemovableRanges();
        assertEquals(2, ranges.size());
        assertEquals(0, ranges.get(0).rangeIndex());
        assertEquals(1, ranges.get(1).rangeIndex());

        // The stream objects cover data ranges [0, 400), the range 3 is the last range, so checkRemovableRanges should not contain it.
        for (int i = 11; i < 20; i++) {
            metadata.streamObjects().put((long) i, new S3StreamObject(i, STREAM_ID, i * 20, (i + 1) * 20));
        }
        ranges = metadata.checkRemovableRanges();
        assertEquals(3, ranges.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(i, ranges.get(i).rangeIndex());
        }
    }

}
