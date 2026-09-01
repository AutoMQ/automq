/*
 * Copyright 2026, AutoMQ HK Limited.
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

package org.apache.kafka.server.common.automq;

import com.automq.stream.Version;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies feature-level capability boundaries declared by {@link AutoMQVersion}.
 */
@Tag("S3Unit")
public class AutoMQVersionTest {

    /**
     * Given adjacent AutoMQ feature levels, fast partition reassignment starts at V6.
     */
    @Test
    public void testFastPartitionReassignmentCapabilityStartsAtV6() {
        assertFalse(AutoMQVersion.V5.isFastPartitionReassignmentSupported());
        assertTrue(AutoMQVersion.V6.isFastPartitionReassignmentSupported());
        assertSame(AutoMQVersion.V6, AutoMQVersion.from((short) 7));
    }

    /**
     * Given the Archive feature level, verify only finalized V7 enables Stream Archive.
     */
    @Test
    public void testStreamArchiveCapabilityStartsAtV7() {
        assertFalse(AutoMQVersion.V6.isStreamArchiveSupported());
        assertTrue(AutoMQVersion.V7.isStreamArchiveSupported());
        assertSame(AutoMQVersion.V7, AutoMQVersion.LATEST);
        assertSame(AutoMQVersion.V7, AutoMQVersion.from((short) 8));
        assertSame(Version.V1, AutoMQVersion.V6.s3streamVersion());
        assertSame(Version.V2, AutoMQVersion.V7.s3streamVersion());
    }
}
