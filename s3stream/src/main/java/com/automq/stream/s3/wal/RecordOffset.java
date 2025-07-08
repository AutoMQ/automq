package com.automq.stream.s3.wal;

import io.netty.buffer.ByteBuf;

public interface RecordOffset {

    ByteBuf buffer();

}
