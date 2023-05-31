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
package org.apache.kafka.tools;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataQuorumCommandErrorTest {

    @Test
    public void testPropertiesFileDoesNotExists() {
        assertEquals(1,
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe"));
        assertEquals("Properties file admin.properties does not exists!",
            ToolsTestUtils.captureStandardErr(() ->
                MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "--command-config", "admin.properties", "describe")));
    }

    @Test
    public void testDescribeOptions() {
        assertEquals(1, MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "describe"));
        assertEquals("One of --status or --replication must be specified with describe sub-command",
            ToolsTestUtils.captureStandardErr(() ->
                MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "describe")));

        assertEquals(1,
            MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "describe", "--status", "--replication"));
        assertEquals("Only one of --status or --replication should be specified with describe sub-command",
            ToolsTestUtils.captureStandardErr(() ->
                MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092", "describe", "--status", "--replication")));
    }

    @Test
    public void testRelativeTimeMs() {
        long validEpochMs = Instant.now().minusSeconds(5).toEpochMilli();
        assertTrue(MetadataQuorumCommand.relativeTimeMs(validEpochMs, "test") >= 0);
        long nowMs = Instant.now().toEpochMilli();
        assertTrue(MetadataQuorumCommand.relativeTimeMs(nowMs, "test") >= 0);
        long invalidEpochMs = Instant.EPOCH.minus(1, ChronoUnit.DAYS).toEpochMilli();
        assertThrows(KafkaException.class, () -> MetadataQuorumCommand.relativeTimeMs(invalidEpochMs, "test"));
        long futureEpochMs = Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli();
        assertThrows(KafkaException.class, () -> MetadataQuorumCommand.relativeTimeMs(futureEpochMs, "test"));
    }

}
