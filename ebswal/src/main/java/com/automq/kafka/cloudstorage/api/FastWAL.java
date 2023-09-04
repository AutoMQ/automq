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

    class RecordEntity {
        private final ByteBuffer record;
        private final long walOffset;

        private final int length;

        public RecordEntity(ByteBuffer record, long walOffset, int length) {
            this.record = record;
            this.walOffset = walOffset;
            this.length = length;
        }

        public ByteBuffer getRecord() {
            return record;
        }

        public long getWalOffset() {
            return walOffset;
        }

        public int getLength() {
            return length;
        }
    }

    interface AppendResult {
        // 预分配好的 Reocord body 存储的起始位置
        long walOffset();

        int length();

        CompletableFuture<CallbackResult> future();

        class CallbackResult {
            // 滑动窗口的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
            private final long slidingWindowMinOffset;
            // 预分配好的 Reocord 存储的起始位置
            private final long walOffset;

            // 预分配好的 Reocord 的大小
            private final int length;

            public CallbackResult(long slidingWindowMinOffset, long walOffset, int length) {
                this.slidingWindowMinOffset = slidingWindowMinOffset;
                this.walOffset = walOffset;
                this.length = length;
            }

            public long getSlidingWindowMinOffset() {
                return slidingWindowMinOffset;
            }

            public long getWalOffset() {
                return walOffset;
            }

            public int getLength() {
                return length;
            }
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

    /**
     * trim 不及时，会抛异常。
     * 滑动窗口无法扩容，也会抛异常。
     *
     * @throws OverCapacityException
     */
    AppendResult append(ByteBuffer record, //
                        int crc //
    ) throws OverCapacityException;


    Iterator<RecordEntity> recover();

    /**
     * 抹除所有小于 offset 的数据。
     * >= offset 的数据仍然可以读取。
     */
    void trim(long offset);
}

