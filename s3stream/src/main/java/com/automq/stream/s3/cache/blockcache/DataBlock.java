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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.utils.CloseableIterator;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@EventLoopSafe public class DataBlock {
    private static final int UNREAD_INIT = -1;
    private final long objectId;
    private final DataBlockIndex dataBlockIndex;
    private final CompletableFuture<DataBlock> loadCf = new CompletableFuture<>();
    private final CompletableFuture<DataBlock> freeCf = new CompletableFuture<>();
    private final AtomicInteger unreadCnt = new AtomicInteger(UNREAD_INIT);
    private final ReadStatusChangeListener listener;
    private ObjectReader.DataBlockGroup dataBlockGroup;

    public DataBlock(long objectId, DataBlockIndex dataBlockIndex, ReadStatusChangeListener observeListener) {
        this.objectId = objectId;
        this.dataBlockIndex = dataBlockIndex;
        this.listener = observeListener;
    }

    /**
     * Complete the data loading
     */
    public void complete(ObjectReader.DataBlockGroup dataBlockGroup) {
        this.dataBlockGroup = dataBlockGroup;
        loadCf.complete(this);
    }

    /**
     * Complete the data loading with exception
     */
    public void completeExceptionally(Throwable ex) {
        loadCf.completeExceptionally(ex);
        freeCf.complete(null);
    }

    public CompletableFuture<DataBlock> dataFuture() {
        return loadCf;
    }

    public void free() {
        if (dataBlockGroup != null) {
            dataBlockGroup.release();
        }
        freeCf.complete(this);
    }

    public CompletableFuture<DataBlock> freeFuture() {
        return freeCf;
    }

    public long objectId() {
        return objectId;
    }

    public DataBlockIndex dataBlockIndex() {
        return dataBlockIndex;
    }

    public void markUnread() {
        if (dataBlockGroup == null) {
            throw new IllegalStateException("DataBlock is not loaded yet.");
        }
        if (unreadCnt.get() == UNREAD_INIT) {
            unreadCnt.set(1);
        } else {
            int old = unreadCnt.getAndIncrement();
            if (old == 0) {
                // observe
                listener.markUnread(this);
            }
        }
    }

    public void markRead() {
        if (dataBlockGroup == null) {
            throw new IllegalStateException("DataBlock is not loaded yet.");
        }
        int unreadCnt = this.unreadCnt.decrementAndGet();
        if (unreadCnt <= 0) {
            listener.markRead(this);
        }
    }

    public void retain() {
        dataBlockGroup.retain();
    }

    public void release() {
        dataBlockGroup.release();
    }

    ByteBuf dataBuf() {
        return dataBlockGroup.buf();
    }

    public List<StreamRecordBatch> getRecords(long startOffset, long endOffset, int maxBytes) {
        List<StreamRecordBatch> records = new ArrayList<>();
        int remainingBytes = maxBytes;
        // TODO: iterator from certain offset
        try (CloseableIterator<StreamRecordBatch> it = dataBlockGroup.iterator()) {
            while (it.hasNext()) {
                StreamRecordBatch recordBatch = it.next();
                if (recordBatch.getBaseOffset() < endOffset && recordBatch.getLastOffset() > startOffset) {
                    records.add(recordBatch);
                    remainingBytes -= recordBatch.size();
                    if (remainingBytes <= 0) {
                        break;
                    }
                    continue;
                } else {
                    recordBatch.release();
                }
                if (recordBatch.getBaseOffset() >= endOffset) {
                    break;
                }
            }
        }
        return records;
    }

    @Override
    public String toString() {
        return "DataBlock{" + "objectId=" + objectId + ", index=" + dataBlockIndex + '}';
    }
}
