package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.api.FastWAL;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IOTaskResponse {
    // writeOffset 包含了 Record header 的大小
    long writeOffset();
}
