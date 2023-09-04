package com.automq.kafka.cloudstorage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class SlidingWindowService {
    private AtomicLong slidingWindowNextWriteOffset = new AtomicLong(0);
    private AtomicLong slidingWindowMaxSize = new AtomicLong(0);

    private ConcurrentHashMap<Long, Long> tableWriteResponse = new ConcurrentHashMap<>();

    private BlockingQueue<IOTask> ioTaskQueue = new LinkedBlockingQueue<>();

}

