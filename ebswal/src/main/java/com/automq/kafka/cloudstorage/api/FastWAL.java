package com.automq.kafka.cloudstorage.api;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * 是一个从零开始无限增长的 WAL，实际实现会使用块设备，每次写入采用块对齐方式。Record 之间非连续存储。
 * 如果上一次是优雅关闭，重新启动后，read 返回的数据为空集合。
 */
public interface FastWAL {
    class OverCapacityException extends Exception {
        public OverCapacityException(String message) {
            super(message);
        }
    }

    /**
     * 启动线程，加载元数据
     */
    void start();

    /**
     * 关闭线程，保存元数据，其中包含 trim offset。
     */
    void shutdown();

    interface AppendResult {
        // Record body 预分配的存储起始位置
        long walOffset();

        // Record body 的长度（不包含任何元数据长度）
        int length();

        CompletableFuture<CallbackResult> future();

        interface CallbackResult {
            // 滑动窗口的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
            long slidingWindowMinOffset();

            AppendResult appendResult();

        }
    }

    /**
     * trim 不及时，会抛异常。
     * 滑动窗口无法扩容，也会抛异常。
     *
     * @throws OverCapacityException
     */
    AppendResult append(ByteBuffer record, //
                        int crc // 如果 crc == 0，表示需要重新计算 crc
    ) throws OverCapacityException;

    interface RecoverResult {
        ByteBuffer record();

        AppendResult appendResult();
    }

    Iterator<RecoverResult> recover();

    /**
     * 抹除所有小于 offset 的数据。
     * >= offset 的数据仍然可以读取。
     */
    void trim(long offset);
}

