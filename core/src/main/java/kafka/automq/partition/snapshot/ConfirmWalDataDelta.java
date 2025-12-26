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
import com.automq.stream.s3.wal.RecordOffset;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Maintains a bounded, in-memory delta of recent WAL appends so snapshot responses can
 * piggy-back fresh data instead of forcing clients to replay the physical WAL.
 *
 * <p><strong>Responsibilities</strong>
 * <ul>
 *   <li>Subscribe to {@link ConfirmWAL} append events and retain the encoded
 *       {@link StreamRecordBatch} payloads while they are eligible for delta export.</li>
 *   <li>Track confirm offsets and expose them via {@link #handle(short, AutomqGetPartitionSnapshotResponseData)}.</li>
 *   <li>Serialize buffered batches into {@code confirmWalDeltaData} for request versions
 *       &gt;= 2, or signal that callers must replay the WAL otherwise.</li>
 *   <li>Enforce {@link #MAX_RECORDS_BUFFER_SIZE} so the delta cache remains lightweight.</li>
 * </ul>
 *
 * <p><strong>State machine</strong>
 * <ul>
 *   <li>{@link #STATE_NOT_SYNC}: Buffer content is discarded (e.g. overflow) and only confirm
 *       offsets are returned until new appends arrive.</li>
 *   <li>{@link #STATE_SYNCING}: Buffered records are eligible to be drained and turned into a
 *       delta payload when {@link #handle(short, AutomqGetPartitionSnapshotResponseData)} runs.</li>
 *   <li>{@link #STATE_CLOSED}: Listener is torn down and ignores subsequent appends.</li>
 * </ul>
 *
 * <p><strong>Concurrency and lifecycle</strong>
 * <ul>
 *   <li>All public methods are synchronized to guard the state machine, queue, and
 *       {@link #lastConfirmOffset} tracking.</li>
 *   <li>Buffered batches are reference-counted; ownership transfers to this class until the
 *       delta is emitted or the buffer is dropped/closed.</li>
 *   <li>{@link #close()} must be invoked when the owning {@link PartitionSnapshotsManager.Session} ends to release buffers
 *       and remove the {@link ConfirmWAL.AppendListener}.</li>
 * </ul>
 *
 * <p><strong>Snapshot interaction</strong>
 * <ul>
 *   <li>{@link #handle(short, AutomqGetPartitionSnapshotResponseData)} always updates
 *       {@code confirmWalEndOffset} and, when possible, attaches {@code confirmWalDeltaData}.</li>
 *   <li>A {@code null} delta signals the client must replay the WAL, whereas an empty byte array
 *       indicates no new data but confirms offsets.</li>
 *   <li>When the aggregated encoded bytes would exceed {@link #MAX_RECORDS_BUFFER_SIZE}, the
 *       buffer is dropped and state resets to {@link #STATE_NOT_SYNC}.</li>
 * </ul>
 */
public class ConfirmWalDataDelta implements ConfirmWAL.AppendListener {
    static final int STATE_NOT_SYNC = 0;
    static final int STATE_SYNCING = 1;
    static final int STATE_CLOSED = 9;
    static final int MAX_RECORDS_BUFFER_SIZE = 32 * 1024; // 32KiB
    private final ConfirmWAL confirmWAL;

    private final ConfirmWAL.ListenerHandle listenerHandle;
    final BlockingQueue<RecordExt> records = new LinkedBlockingQueue<>();
    final AtomicInteger size = new AtomicInteger(0);

    private RecordOffset lastConfirmOffset = null;

    int state = STATE_NOT_SYNC;

    public ConfirmWalDataDelta(ConfirmWAL confirmWAL) {
        this.confirmWAL = confirmWAL;
        this.listenerHandle = confirmWAL.addAppendListener(this);
    }

    public synchronized void close() {
        this.state = STATE_CLOSED;
        this.listenerHandle.close();
        records.forEach(r -> r.record.release());
        records.clear();
    }

    public void handle(short requestVersion,
        AutomqGetPartitionSnapshotResponseData resp) {
        RecordOffset newConfirmOffset = null;
        List<RecordExt> delta = null;
        synchronized (this) {
            if (state == STATE_NOT_SYNC) {
                List<RecordExt> drainedRecords = new ArrayList<>(records.size());
                records.drainTo(drainedRecords);
                size.addAndGet(-drainedRecords.stream().mapToInt(r -> r.record.encoded().readableBytes()).sum());
                if (!drainedRecords.isEmpty()) {
                    RecordOffset deltaConfirmOffset = drainedRecords.get(drainedRecords.size() - 1).nextOffset();
                    if (lastConfirmOffset == null || deltaConfirmOffset.compareTo(lastConfirmOffset) > 0) {
                        newConfirmOffset = deltaConfirmOffset;
                        state = STATE_SYNCING;
                    }
                    drainedRecords.forEach(r -> r.record.release());
                }
            } else if (state == STATE_SYNCING) {
                delta = new ArrayList<>(records.size());

                records.drainTo(delta);
                size.addAndGet(-delta.stream().mapToInt(r -> r.record.encoded().readableBytes()).sum());
                newConfirmOffset = delta.isEmpty() ? lastConfirmOffset : delta.get(delta.size() - 1).nextOffset();
            }
            if (newConfirmOffset == null) {
                newConfirmOffset = confirmWAL.confirmOffset();
            }
            this.lastConfirmOffset = newConfirmOffset;
        }
        resp.setConfirmWalEndOffset(newConfirmOffset.bufferAsBytes());
        if (delta != null) {
            int size = delta.stream().mapToInt(r -> r.record.encoded().readableBytes()).sum();
            byte[] data = new byte[size];
            ByteBuf buf = Unpooled.wrappedBuffer(data).clear();
            delta.forEach(r -> {
                buf.writeBytes(r.record.encoded());
                r.record.release();
            });
            if (requestVersion >= 2) {
                // The confirmWalDeltaData is only supported in request version >= 2
                resp.setConfirmWalDeltaData(data);
            }
        } else {
            if (requestVersion >= 2) {
                // - Null means the client needs replay from the physical WAL
                // - Empty means there is no delta data.
                resp.setConfirmWalDeltaData(null);
            }
        }
    }

    @Override
    public synchronized void onAppend(StreamRecordBatch record, RecordOffset recordOffset,
        RecordOffset nextOffset) {
        if (state == STATE_CLOSED) {
            return;
        }
        record.retain();
        records.add(new RecordExt(record, recordOffset, nextOffset));
        if (size.addAndGet(record.encoded().readableBytes()) > MAX_RECORDS_BUFFER_SIZE) {
            // If the buffer is full, drop all records and switch to NOT_SYNC state.
            // It's cheaper to replay from the physical WAL instead of transferring the data by network.
            state = STATE_NOT_SYNC;
            records.forEach(r -> r.record.release());
            records.clear();
            size.set(0);
        }
    }

    record RecordExt(StreamRecordBatch record, RecordOffset recordOffset, RecordOffset nextOffset) {
    }

    public static List<StreamRecordBatch> decodeDeltaRecords(byte[] data) {
        if (data == null) {
            return null;
        }
        List<StreamRecordBatch> records = new ArrayList<>();
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        while (buf.readableBytes() > 0) {
            StreamRecordBatch record = StreamRecordBatch.parse(buf, false);
            records.add(record);
        }
        return records;
    }
}
