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

package kafka.automq.partition.snapshot;

import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;

import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import io.netty.buffer.Unpooled;

import static kafka.automq.partition.snapshot.ConfirmWalDataDelta.MAX_RECORDS_BUFFER_SIZE;
import static kafka.automq.partition.snapshot.ConfirmWalDataDelta.STATE_NOT_SYNC;
import static kafka.automq.partition.snapshot.ConfirmWalDataDelta.STATE_SYNCING;
import static kafka.automq.partition.snapshot.ConfirmWalDataDelta.decodeDeltaRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfirmWalDataDeltaTest {

    ConfirmWAL confirmWAL;
    ConfirmWalDataDelta delta;
    long walOffset = 233;
    long nextWalOffset = walOffset;

    @BeforeEach
    void setup() {
        confirmWAL = mock(ConfirmWAL.class);
        when(confirmWAL.addAppendListener(any())).thenReturn(() -> {
        });
        delta = new ConfirmWalDataDelta(confirmWAL);
        walOffset = 233;
        nextWalOffset = walOffset;
    }

    @Test
    public void testHandle() {
        AutomqGetPartitionSnapshotResponseData resp = new AutomqGetPartitionSnapshotResponseData();
        // There's no new wal data.
        DefaultRecordOffset confirmOffset = DefaultRecordOffset.of(1, 233, 0);
        when(confirmWAL.confirmOffset()).thenReturn(confirmOffset);
        delta.handle((short) 1, resp);
        assertEquals(confirmOffset, DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())));
        // In requestVersion=1, the confirmWalDeltaData is an empty array.
        assertEquals(0, resp.confirmWalDeltaData().length);

        // In requestVersion=2, the confirmWalDeltaData is null when there isn't new wal data, or it's not in STATE_SYNCING.
        resp = new AutomqGetPartitionSnapshotResponseData();
        when(confirmWAL.confirmOffset()).thenReturn(confirmOffset);
        delta.handle((short) 2, resp);
        assertEquals(confirmOffset, DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())));
        Assertions.assertNull(resp.confirmWalDeltaData());
        assertEquals(STATE_NOT_SYNC, delta.state);

        // New record has been appended, the state will change from STATE_NOT_SYNC to STATE_SYNCING.
        when(confirmWAL.confirmOffset()).thenThrow(new UnsupportedOperationException());
        for (int i = 0; i < 64; i++) {
            for (int j = 0; j < 3; j++) {
                onAppend(3 * i + j);
            }
            resp = new AutomqGetPartitionSnapshotResponseData();
            delta.handle((short) 2, resp);
            assertEquals(DefaultRecordOffset.of(1, nextWalOffset, 0), DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())));
            if (i == 0) {
                // The first response in STATE_SYNCING only take confirmOffset from wal records and set the confirmWalDeltaData null.
                Assertions.assertNull(resp.confirmWalDeltaData());
            } else {
                List<StreamRecordBatch> recordList = decodeDeltaRecords(resp.confirmWalDeltaData());
                assertEquals(3, recordList.size());
                for (int j = 0; j < 3; j++) {
                    StreamRecordBatch record = recordList.get(j);
                    assertEquals(3 * i + j, record.getBaseOffset());
                    assertEquals(1024, record.getPayload().readableBytes());
                    record.release();
                }
            }
            assertEquals(0, delta.size.get());
            assertEquals(STATE_SYNCING, delta.state);
        }
    }

    @Test
    public void testOnAppend_bufferExceed() {
        AutomqGetPartitionSnapshotResponseData resp = new AutomqGetPartitionSnapshotResponseData();

        onAppend(3);

        resp = new AutomqGetPartitionSnapshotResponseData();
        delta.handle((short) 2, resp);
        assertEquals(nextWalOffset, DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())).offset());
        Assertions.assertNull(resp.confirmWalDeltaData());
        assertEquals(STATE_SYNCING, delta.state);

        // buffer exceed
        int i = 0;
        for (int size = 0; size < MAX_RECORDS_BUFFER_SIZE; i++) {
            size += onAppend(4 + i);
        }
        assertEquals(0, delta.size.get());
        assertEquals(STATE_NOT_SYNC, delta.state);

        onAppend(4 + i);
        resp = new AutomqGetPartitionSnapshotResponseData();
        delta.handle((short) 2, resp);
        assertEquals(nextWalOffset, DefaultRecordOffset.of(Unpooled.wrappedBuffer(resp.confirmWalEndOffset())).offset());
        Assertions.assertNull(resp.confirmWalDeltaData());
        assertEquals(STATE_SYNCING, delta.state);
        assertEquals(0, delta.size.get());
    }

    int onAppend(long recordBaseOffset) {
        StreamRecordBatch record = new StreamRecordBatch(1, 2, recordBaseOffset, 1, Unpooled.wrappedBuffer(new byte[1024]));
        nextWalOffset = walOffset + record.encoded().readableBytes();
        record.encoded();
        delta.onAppend(
            record,
            DefaultRecordOffset.of(1, walOffset, record.encoded().readableBytes()),
            DefaultRecordOffset.of(1, nextWalOffset, 0)
        );
        walOffset = nextWalOffset;
        return record.encoded().readableBytes();
    }

}
