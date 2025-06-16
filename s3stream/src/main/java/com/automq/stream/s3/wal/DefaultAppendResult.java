package com.automq.stream.s3.wal;

public class DefaultAppendResult implements AppendResult {
    private final RecordOffset recordOffset;

    public DefaultAppendResult(RecordOffset recordOffset) {
        this.recordOffset = recordOffset;
    }

    @Override
    public RecordOffset recordOffset() {
        return recordOffset;
    }
}
