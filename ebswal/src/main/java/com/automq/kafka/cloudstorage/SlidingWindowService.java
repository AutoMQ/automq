package com.automq.kafka.cloudstorage;

import com.automq.kafka.cloudstorage.util.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

    private ConcurrentHashMap<Long, Long> tableIOTaskRequest = new ConcurrentHashMap<>();

    private BlockingQueue<IOTask> queueIOTaskRequest = new LinkedBlockingQueue<>();

    private static int BlockSize = Integer.parseInt(System.getProperty(//
            "automq.ebswal.blocksize", //
            "4096"));

    private long contentSize = 1024 * 1024 * 1024 * 8;

    public static final int RecordMetaHeaderSize = 4 + 4 + 8 + 4 + 4;


    @Override
    public String getServiceName() {
        return SlidingWindowService.class.getSimpleName();
    }

    private void makeIOTaskHitWindow(final IOTask ioTask) {

    }

    public long allocateWriteOffset(final int recordBodySize, final long trimOffset) {
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

            // 如果 trim 不及时，导致 RingBuffer 出现写覆盖有效数据，抛异常
            if (expectedWriteOffset - trimOffset < totalWriteSize) {
                throw new RuntimeException(String.format("Sliding window is full, please trim wal expectedWriteOffset [%d] trimOffset [%d] totalWriteSize [%d]",//
                        expectedWriteOffset, trimOffset, totalWriteSize));
            }

        } while (!slidingWindowNextWriteOffset.compareAndSet(lastWriteOffset, expectedWriteOffset));

        return expectedWriteOffset;
    }

    @Override
    public void run() {
        LOGGER.info("{} service started", getServiceName());
        while (!isStopped()) {
            try {
                IOTask ioTask = queueIOTaskRequest.poll(3000, TimeUnit.MILLISECONDS);
                if (ioTask != null) {
                    makeIOTaskHitWindow(ioTask);
                }

            } catch (Throwable e) {
                LOGGER.error(String.format("%s service has exception. ", getServiceName()), e);
            }
        }
        LOGGER.info("{} service stopped", getServiceName());
    }
}

