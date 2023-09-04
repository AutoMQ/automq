package com.automq.kafka.cloudstorage;

import java.nio.ByteBuffer;

public interface IOTask {
    boolean synchronous();

    long walOffset();

    ByteBuffer record();

    long length();
}
