package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.api.FastWAL;
import com.automq.kafka.cloudstorage.util.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 维护滑动窗口，单线程写入，AIO 线程池异步通知调用方。
 * 滑动窗口容量不足时，单线程停止当前操作，执行同步扩容操作。
 * AIO 线程池返回时，同步更新滑动窗口的最小 Offset。
 */
public class SlidingWindowService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlidingWindowService.class.getSimpleName());
    private AtomicLong slidingWindowNextWriteOffset = new AtomicLong(0);

    private AtomicLong slidingWindowMaxSize = new AtomicLong(0);

    private AtomicLong slidingWindowMinOffset = new AtomicLong(0);
    private BlockingQueue<IOTask> queueIOTaskRequest = new LinkedBlockingQueue<>();

    // 单线程读写，不需要加锁

    private TreeMap<Long, IOTaskRequest> treeMapIOTaskRequest = new TreeMap<>();
    private static int BlockSize = Integer.parseInt(System.getProperty(//
            "automq.ebswal.blocksize", //
            "4096"));

    private static long SlidingWindowUpperLimit = Long.parseLong(System.getProperty(//
            "automq.ebswal.slidingWindowUpperLimit", //
            String.valueOf(1024 * 1024 * 512)));

    private static long SlidingWindowScaleUnit = Long.parseLong(System.getProperty(//
            "automq.ebswal.slidingWindowScaleUnit", //
            String.valueOf(1024 * 1024 * 16)));

    private long contentSize = 1024 * 1024 * 1024 * 8;

    public static final int RecordMetaHeaderSize = 4 + 4 + 8 + 4 + 4;


    @Override
    public String getServiceName() {
        return SlidingWindowService.class.getSimpleName();
    }

    private boolean makeIOTaskHitWindow(final IOTaskRequest ioTaskRequest) {
        if (!treeMapIOTaskRequest.isEmpty()) {
            Long firstKey = treeMapIOTaskRequest.firstKey();
            long newWindowTail = ioTaskRequest.writeOffset() + ioTaskRequest.recordHeader().limit() + ioTaskRequest.recordBody().limit();

            if (newWindowTail - firstKey > slidingWindowMaxSize.get()) {
                long newSlidingWindowMaxSize = newWindowTail - firstKey + SlidingWindowScaleUnit;
                if (newSlidingWindowMaxSize > SlidingWindowUpperLimit) {
                    try {
                        // 回调 IO TASK Future，通知用户发生了灾难性故障，可能是磁盘损坏
                        String exceptionMessage = String.format("new sliding window size [%d] is too large, upper limit [%d]", newSlidingWindowMaxSize, SlidingWindowUpperLimit);
                        ioTaskRequest.future().completeExceptionally(new FastWAL.OverCapacityException(exceptionMessage));
                    } catch (Throwable ignored) {
                    }
                    return false;
                } else {
                    slidingWindowMaxSize.set(newSlidingWindowMaxSize);
                    ioTaskRequest.flushWALHeader(newSlidingWindowMaxSize);
                    LOGGER.info("[KEY_EVENT] Sliding window is too small to start the scale process, current window size [{}], new window size [{}]", //
                            slidingWindowMaxSize.get(), newSlidingWindowMaxSize);
                }
            }
        }

        return true;
    }

    public long allocateWriteOffset(final int recordBodySize, final long trimOffset) throws FastWAL.OverCapacityException {
        // 计算要写入的总大小
        int totalWriteSize = RecordMetaHeaderSize + recordBodySize;

        // 计算写入 wal offset
        long lastWriteOffset = 0;
        long expectedWriteOffset = 0;
        do {
            lastWriteOffset = slidingWindowNextWriteOffset.get();
            expectedWriteOffset = lastWriteOffset % BlockSize == 0  //
                    ? lastWriteOffset //
                    : lastWriteOffset + BlockSize - lastWriteOffset % BlockSize;

            // 如果物理设备末尾不足这次写入，则跳转到物理设备起始位置
            if ((contentSize - expectedWriteOffset % contentSize) < totalWriteSize) {
                expectedWriteOffset = expectedWriteOffset + contentSize - expectedWriteOffset % contentSize;
            }

            // 如果 trim 不及时，会导致写 RingBuffer 覆盖有效数据，抛异常
            if (expectedWriteOffset + totalWriteSize - trimOffset > contentSize) {
                throw new FastWAL.OverCapacityException(String.format("RingBuffer is full, please trim wal. expectedWriteOffset [%d] trimOffset [%d] totalWriteSize [%d]",//
                        expectedWriteOffset, trimOffset, totalWriteSize));
            }

        } while (!slidingWindowNextWriteOffset.compareAndSet(lastWriteOffset, expectedWriteOffset + totalWriteSize));

        return expectedWriteOffset;
    }

    @Override
    public void run() {
        LOGGER.info("{} service started", getServiceName());
        while (!isStopped()) {
            try {
                IOTask ioTask = queueIOTaskRequest.poll(3000, TimeUnit.MILLISECONDS);
                if (ioTask != null) {
                    if (ioTask instanceof IOTaskRequest) {
                        IOTaskRequest ioTaskRequest = (IOTaskRequest) ioTask;
                        if (makeIOTaskHitWindow(ioTaskRequest)) {
                            treeMapIOTaskRequest.put(ioTaskRequest.writeOffset(), ioTaskRequest);

                            // TODO 发起异步写请求
                        }

                    } else if (ioTask instanceof IOTaskResponse) {
                        IOTaskResponse ioTaskResponse = (IOTaskResponse) ioTask;
                        // 更新滑动窗口的最小 Offset
                        treeMapIOTaskRequest.remove(ioTaskResponse.writeOffset());

                        if (!treeMapIOTaskRequest.isEmpty()) {
                            slidingWindowMinOffset.set(treeMapIOTaskRequest.firstKey());
                        } else {
                            slidingWindowMinOffset.set(slidingWindowNextWriteOffset.get());
                        }
                    }
                }
            } catch (Throwable e) {
                LOGGER.error(String.format("%s service has exception. ", getServiceName()), e);
            }
        }
        LOGGER.info("{} service stopped", getServiceName());
    }

    public long getSlidingWindowMaxSize() {
        return slidingWindowMaxSize.get();
    }

    public long getSlidingWindowNextWriteOffset() {
        return slidingWindowNextWriteOffset.get();
    }
}
