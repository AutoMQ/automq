/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.s3;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class RecordAccumulator {
    private final ReentrantLock lock = new ReentrantLock();

    private long nextOffset = 0;
    private long flushedOffset = 0;

    // TODO: initialize pending list
    private List<Record> bufferList = new ArrayList<>();

    // TODO: move buffer list to upload list periodically.
    private final TreeMap<Long, List<Record>> uploadList = new TreeMap<>();

    // TODO: backoff list, which is should be a priority queue

    private final TreeSet<WALObject> objectSet = new TreeSet<>();

    private final ObjectStorage objectStorage;

    public RecordAccumulator(ObjectStorage objectStorage) {
        this.objectStorage = objectStorage;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long flushedOffset() {
        return flushedOffset;
    }

    public List<WALObject> objectList() {
        lock.lock();
        try {
            return new ArrayList<>(objectSet);
        } finally {
            lock.unlock();
        }
    }

    public long append(long recordSize, Function<Long, ByteBuf> recordSupplier,
        CompletableFuture<AppendResult.CallbackResult> future) throws OverCapacityException {
        lock.lock();
        try {
            bufferList.add(new Record(nextOffset, recordSupplier.apply(recordSize), future));

            // TODO: trigger upload if the size of records exceeds the limit
            nextOffset += recordSize;
            if (nextOffset - bufferList.get(0).offset > 1024 * 1024) {
                upload();
            }

            // TODO: throw OverCapacityException if the size of records exceeds the limit
            if (nextOffset - flushedOffset > 1024 * 1024 * 1024) {
                throw new OverCapacityException("");
            }

            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

    private static class Record {
        public final long offset;
        public final ByteBuf record;
        public final CompletableFuture<AppendResult.CallbackResult> future;

        public Record(long offset, ByteBuf record,
            CompletableFuture<AppendResult.CallbackResult> future) {
            this.offset = offset;
            this.record = record;
            this.future = future;
        }
    }

    public static class WALObject implements Comparable<WALObject> {
        private final String path;
        private final long startOffset;
        private final long length;

        public WALObject(String path, long startOffset, long length) {
            this.path = path;
            this.startOffset = startOffset;
            this.length = length;
        }

        @Override
        public int compareTo(WALObject o) {
            return Long.compare(startOffset, o.startOffset);
        }

        public String path() {
            return path;
        }

        public long startOffset() {
            return startOffset;
        }

        public long length() {
            return length;
        }
    }

    protected void upload() throws OverCapacityException {
        lock.lock();
        try {
            // No records to upload
            if (bufferList.isEmpty()) {
                return;
            }

            // TODO: limit the number of pending to upload
            if (uploadList.size() > 10) {
                throw new OverCapacityException("Too many pending upload");
            }

            CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
            bufferList.forEach(record -> buffer.addComponent(true, record.record));

            // Clean up records.
            long firstOffset = bufferList.get(0).offset;
            Record lastRecord = bufferList.get(bufferList.size() - 1);
            long lastOffset = lastRecord.offset + lastRecord.record.readableBytes();
            uploadList.put(firstOffset, bufferList);
            bufferList = new ArrayList<>();

            // TODO: change path
            String path = "wal/" + UUID.randomUUID();
            objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, path, buffer)
                .thenApply(v -> {
                    lock.lock();
                    try {
                        objectSet.add(new WALObject(path, firstOffset, lastOffset - firstOffset));

                        List<Record> uploadedRecords = uploadList.remove(firstOffset);

                        // Update flushed offset
                        if (!uploadList.isEmpty()) {
                            flushedOffset = uploadList.firstKey() - 1;
                        } else if (!bufferList.isEmpty()) {
                            flushedOffset = bufferList.get(0).offset - 1;
                        } else {
                            flushedOffset = nextOffset - 1;
                        }

                        uploadedRecords.forEach(record -> record.future.complete(() -> flushedOffset));
                    } finally {
                        lock.unlock();
                    }
                    return null;
                })
                .exceptionally(e -> {
                    // TODO: handle exception
                    return null;
                });
        } finally {
            lock.unlock();
        }
    }
}
