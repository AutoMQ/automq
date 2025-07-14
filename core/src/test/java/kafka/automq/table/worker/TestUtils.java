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

package kafka.automq.table.worker;

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import org.junit.jupiter.api.Tag;

import scala.Option;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class TestUtils {
    public static Partition partition(int partitionId, int epoch) {
        Partition partition = mock(Partition.class);
        when(partition.partitionId()).thenReturn(partitionId);
        when(partition.getLeaderEpoch()).thenReturn(epoch);
        UnifiedLog log = mock(UnifiedLog.class);
        when(partition.log()).thenReturn(Option.apply(log));
        return partition;
    }

    public static MemoryRecords newMemoryRecord(long offset, int count) {
        return newMemoryRecord(offset, count, 1);
    }

    public static MemoryRecords newMemoryRecord(long offset, int count, int recordSize) {
        SimpleRecord[] records = new SimpleRecord[count];
        for (int i = 0; i < count; i++) {
            records[i] = new SimpleRecord(new byte[recordSize]);
        }

        return MemoryRecords.withRecords(offset, Compression.NONE, records);
    }
}
