package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.wal.RecordOffset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DefaultRecordOffset implements RecordOffset {
    private static final byte MAGIC = (byte) 0xA8;
    private final long offset;
    private final int size;

    private DefaultRecordOffset(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public static DefaultRecordOffset of(long recordOffset, int recordSize) {
        return new DefaultRecordOffset(recordOffset, recordSize);
    }

    public long offset() {
        return offset;
    }

    public int size() {
        return size;
    }

    @Override
    public ByteBuf buffer() {
        ByteBuf buffer = Unpooled.buffer(1 + 8 + 4);
        buffer.writeByte(MAGIC);
        buffer.writeLong(this.offset);
        buffer.writeInt(this.size);
        return null;
    }

    @Override
    public String toString() {
        return "DefaultRecordOffset{" +
            "recordOffset=" + offset +
            ", recordSize=" + size +
            '}';
    }
}
