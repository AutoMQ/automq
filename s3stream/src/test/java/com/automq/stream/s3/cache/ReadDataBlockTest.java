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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.model.StreamRecordBatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies payload-byte accounting for complete records returned under soft read limits.
 */
@Tag("S3Unit")
public class ReadDataBlockTest {
    /**
     * Two mocked 1 GiB records report a positive long total without allocating payload buffers.
     */
    @Test
    public void testSizeInBytesExceedsIntRange() {
        StreamRecordBatch first = mock(StreamRecordBatch.class);
        StreamRecordBatch second = mock(StreamRecordBatch.class);
        when(first.size()).thenReturn(1 << 30);
        when(second.size()).thenReturn(1 << 30);
        ReadDataBlock block = new ReadDataBlock(List.of(first, second), CacheAccessType.BLOCK_CACHE_HIT);

        assertEquals(1L << 31, block.sizeInBytes());
    }
}
