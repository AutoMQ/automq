/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.model;

import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.utils.biniarysearch.ComparableItem;

import io.netty.buffer.ByteBuf;

public class StreamRecordBatch implements Comparable<StreamRecordBatch>, ComparableItem<Long> {
    private static final int OBJECT_OVERHEAD = 48 /* fields */ + 48 /* ByteBuf payload */ + 48 /* ByteBuf encoded */;
    private final long streamId;
    private final long epoch;
    private final long baseOffset;
    private final int count;
    private ByteBuf payload;
    private ByteBuf encoded;

    public StreamRecordBatch(long streamId, long epoch, long baseOffset, int count, ByteBuf payload) {
        this.streamId = streamId;
        this.epoch = epoch;
        this.baseOffset = baseOffset;
        this.count = count;
        this.payload = payload;
    }

    public ByteBuf encoded() {
        // TODO: keep the ref count
        if (encoded == null) {
            encoded = StreamRecordBatchCodec.encode(this);
            ByteBuf oldPayload = payload;
            payload = encoded.slice(encoded.readerIndex() + encoded.readableBytes() - payload.readableBytes(), payload.readableBytes());
            oldPayload.release();
        }
        return encoded.duplicate();
    }

    public long getStreamId() {
        return streamId;
    }

    public long getEpoch() {
        return epoch;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getLastOffset() {
        return baseOffset + count;
    }

    public int getCount() {
        return count;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    public int size() {
        return payload.readableBytes();
    }

    public int occupiedSize() {
        return size() + OBJECT_OVERHEAD;
    }

    public void retain() {
        if (encoded != null) {
            encoded.retain();
        } else {
            payload.retain();
        }
    }

    public void release() {
        if (encoded != null) {
            encoded.release();
        } else {
            payload.release();
        }
    }

    @Override
    public int compareTo(StreamRecordBatch o) {
        int rst = Long.compare(streamId, o.streamId);
        if (rst != 0) {
            return rst;
        }
        rst = Long.compare(epoch, o.epoch);
        if (rst != 0) {
            return rst;
        }
        return Long.compare(baseOffset, o.baseOffset);
    }

    @Override
    public String toString() {
        return "StreamRecordBatch{" +
            "streamId=" + streamId +
            ", epoch=" + epoch +
            ", baseOffset=" + baseOffset +
            ", count=" + count +
            ", size=" + size() + '}';
    }

    @Override
    public boolean isLessThan(Long value) {
        return getLastOffset() <= value;
    }

    @Override
    public boolean isGreaterThan(Long value) {
        return getBaseOffset() > value;
    }
}
