package com.automq.stream.s3;

import com.automq.stream.s3.wal.WriteAheadLog;

public class ConfirmWAL {
    private WriteAheadLog log;

    public static final ConfirmWAL INSTANCE = new ConfirmWAL();

    public static void setup(WriteAheadLog log) {
        INSTANCE.log = log;
    }

    public static ConfirmWAL instance() {
        return INSTANCE;
    }

    public long confirmOffset() {
        return log.confirmOffset();
    }

}
