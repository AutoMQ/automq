package com.automq.stream.s3.wal.impl.object;

import java.util.Objects;

public class WALObject implements Comparable<WALObject> {
    private final short bucketId;
    private final String path;
    private final long epoch;
    private final long startOffset;
    private final long endOffset;
    private final long length;

    public WALObject(short bucketId, String path, long epoch, long startOffset, long length) {
        this.bucketId = bucketId;
        this.path = path;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = WALObjectHeader.calculateEndOffsetV0(startOffset, length);
        this.length = length;
    }

    public WALObject(short bucketId, String path, long epoch, long startOffset, long endOffset, long length) {
        this.bucketId = bucketId;
        this.path = path;
        this.epoch = epoch;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.length = length;
    }

    @Override
    public int compareTo(WALObject o) {
        return Long.compare(startOffset, o.startOffset);
    }

    public short bucketId() {
        return bucketId;
    }

    public String path() {
        return path;
    }

    public long epoch() {
        return epoch;
    }

    public long startOffset() {
        return startOffset;
    }

    public long length() {
        return length;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "WALObject{" +
            "bucketId=" + bucketId +
            ", path='" + path + '\'' +
            ", startOffset=" + startOffset +
            ", endOffset=" + endOffset +
            ", length=" + length +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WALObject))
            return false;
        WALObject object = (WALObject) o;
        return bucketId == object.bucketId && startOffset == object.startOffset && endOffset == object.endOffset && length == object.length && Objects.equals(path, object.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, path, startOffset, endOffset, length);
    }
}
