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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

import com.automq.stream.utils.MockTime;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@Tag("S3Unit")
public class RecordIteratorTest {
    private static final String TOPIC_NAME = "test";

    private static final TopicPartition TP = new TopicPartition(TOPIC_NAME, 0);
    private static final long PRODUCER_ID = 1000L;
    private static final short PRODUCER_EPOCH = 0;

    @Test
    public void testAbortedTransactionRecordsRemoved() {
        Records abortRawRecords = newTranscactionalRecords(ControlRecordType.ABORT, 0, 10);
        Records commitrawRecords = newTranscactionalRecords(ControlRecordType.COMMIT, 11, 1);
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(((MemoryRecords) abortRawRecords).buffer());
        buf.writeBytes(((MemoryRecords) commitrawRecords).buffer());
        Records rawRecords = MemoryRecords.readableRecords(buf.nioBuffer());

        RecordsIterator it = new RecordsIterator(0, new FetchDataInfo(null, rawRecords, false, Optional.of(newAbortedTransactions())), BufferSupplier.NO_CACHING);
        Assertions.assertTrue(it.hasNext());
        Record record = it.next();
        Assertions.assertEquals("value11", new String(buf2bytes(record.value())));
        Assertions.assertFalse(it.hasNext());
        Assertions.assertEquals(13, it.nextOffset());
    }

    @Test
    public void testCommittedTransactionRecordsIncluded() {
        int numRecords = 10;
        Records rawRecords = newTranscactionalRecords(ControlRecordType.COMMIT, 0, numRecords);

        RecordsIterator it = new RecordsIterator(0, new FetchDataInfo(null, rawRecords, false, Optional.empty()), BufferSupplier.NO_CACHING);
        for (int i = 0; i < 10; i++) {
            Assertions.assertTrue(it.hasNext());
            it.next();
        }
        Assertions.assertFalse(it.hasNext());
        Assertions.assertEquals(11, it.nextOffset());
    }

    private Records newTranscactionalRecords(ControlRecordType controlRecordType, long baseOffset, int numRecords) {
        Time time = new MockTime();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            baseOffset,
            time.milliseconds(),
            PRODUCER_ID,
            PRODUCER_EPOCH,
            0,
            true,
            RecordBatch.NO_PARTITION_LEADER_EPOCH)) {
            for (int i = 0; i < numRecords; i++)
                builder.append(new SimpleRecord(time.milliseconds(), "key".getBytes(), ("value" + (baseOffset + i)).getBytes()));

            builder.build();
        }

        writeTransactionMarker(buffer, controlRecordType, baseOffset + numRecords, time);
        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }

    private void writeTransactionMarker(ByteBuffer buffer,
        ControlRecordType controlRecordType,
        long offset,
        Time time) {
        MemoryRecords.writeEndTransactionalMarker(buffer,
            offset,
            time.milliseconds(),
            0,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            new EndTransactionMarker(controlRecordType, 0));
    }

    private List<FetchResponseData.AbortedTransaction> newAbortedTransactions() {
        FetchResponseData.AbortedTransaction abortedTransaction = new FetchResponseData.AbortedTransaction();
        abortedTransaction.setFirstOffset(0);
        abortedTransaction.setProducerId(PRODUCER_ID);
        return Collections.singletonList(abortedTransaction);
    }

    static byte[] buf2bytes(ByteBuffer buf) {
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }
}
