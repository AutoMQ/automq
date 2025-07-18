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

    public static DefaultRecordOffset of(ByteBuf buf) {
        buf = buf.slice();
        byte magic = buf.readByte();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }
        return new DefaultRecordOffset(buf.readLong(), buf.readInt());
    }

    public static DefaultRecordOffset of(RecordOffset recordOffset) {
        if (recordOffset instanceof DefaultRecordOffset) {
            return (DefaultRecordOffset) recordOffset;
        }
        return of(recordOffset.buffer());
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
        return buffer;
    }

    @Override
    public String toString() {
        return "DefaultRecordOffset{" +
            "recordOffset=" + offset +
            ", recordSize=" + size +
            '}';
    }
}
