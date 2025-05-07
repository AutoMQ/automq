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

package kafka.automq.table.events;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class EventTest {

    @Test
    public void testCommitRequestCodec() throws IOException {
        Event event = new Event(1, EventType.COMMIT_REQUEST,
            new CommitRequest(UUID.randomUUID(), "topic-xx", List.of(new WorkerOffset(1, 2, 3)))
        );
        Event rst = AvroCodec.decode(AvroCodec.encode(event));

        assertEquals(event.timestamp(), rst.timestamp());
        assertEquals(event.type(), rst.type());
        CommitRequest req1 = event.payload();
        CommitRequest req2 = rst.payload();
        assertEquals(req1.commitId(), req2.commitId());
        assertEquals(req1.topic(), req2.topic());
        assertEquals(req1.offsets().size(), req2.offsets().size());
        assertTrue(workOffsetEqual(req1.offsets().get(0), req2.offsets().get(0)));
    }

    @Test
    public void testCommitResponseCodec() throws IOException {
        Event event = new Event(2, EventType.COMMIT_RESPONSE,
            new CommitResponse(
                Types.StructType.of(),
                233,
                UUID.randomUUID(),
                "topic",
                List.of(new WorkerOffset(1, 2, 3)),
                List.of(EventTestUtil.createDataFile()), List.of(EventTestUtil.createDeleteFile(),
                EventTestUtil.createDeleteFile()),
                new TopicMetric(233),
                List.of(new PartitionMetric(1, 200))
            )
        );
        Event rst = AvroCodec.decode(AvroCodec.encode(event));
        assertEquals(event.timestamp(), rst.timestamp());
        assertEquals(event.type(), rst.type());
        CommitResponse resp1 = event.payload();
        CommitResponse resp2 = rst.payload();
        assertEquals(resp1.code(), resp2.code());
        assertEquals(resp1.commitId(), resp2.commitId());
        assertEquals(resp1.topic(), resp2.topic());
        assertEquals(resp1.nextOffsets().size(), resp2.nextOffsets().size());
        assertTrue(workOffsetEqual(resp1.nextOffsets().get(0), resp2.nextOffsets().get(0)));

        assertEquals(1, resp2.dataFiles().size());
        assertEquals(resp1.dataFiles().get(0).path(), resp2.dataFiles().get(0).path());

        assertEquals(2, resp2.deleteFiles().size());
        assertEquals(resp1.deleteFiles().get(0).path(), resp2.deleteFiles().get(0).path());
        assertEquals(resp1.deleteFiles().get(1).path(), resp2.deleteFiles().get(1).path());

        assertEquals(resp1.topicMetric(), resp2.topicMetric());
        assertEquals(resp1.partitionMetrics(), resp2.partitionMetrics());
    }

    private boolean workOffsetEqual(WorkerOffset o1, WorkerOffset o2) {
        return o1.partition() == o2.partition() && o1.epoch() == o2.epoch() && o1.offset() == o2.offset();
    }

}
