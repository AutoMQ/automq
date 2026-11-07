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

package kafka.automq.table.coordinator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class CheckpointTest {

    @Test
    public void testCodec() {
        Checkpoint checkpoint = new Checkpoint(Status.REQUEST_COMMIT, UUID.randomUUID(), 10L, new long[] {2, 3, 3}, UUID.randomUUID(), 1000L, new long[] {1, 2, 3});
        Checkpoint rst = Checkpoint.decode(ByteBuffer.wrap(checkpoint.encode()));
        assertEquals(checkpoint.status(), rst.status());
        assertEquals(checkpoint.commitId(), rst.commitId());
        assertEquals(checkpoint.taskOffset(), rst.taskOffset());
        assertArrayEquals(checkpoint.nextOffsets(), checkpoint.nextOffsets());
        assertEquals(checkpoint.lastCommitId(), rst.lastCommitId());
        assertEquals(checkpoint.lastCommitTimestamp(), rst.lastCommitTimestamp());
        assertArrayEquals(checkpoint.preCommitOffsets(), rst.preCommitOffsets());
    }

}
