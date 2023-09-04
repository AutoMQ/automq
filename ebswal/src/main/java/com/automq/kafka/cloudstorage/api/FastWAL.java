package com.automq.kafka.cloudstorage.api;

import com.google.common.util.concurrent.FutureCallback;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

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

        // TODO future

        CompletableFuture<CallbackResult> future();

        class CallbackResult {
            // Pending IO Window 的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
            private final long pendingIOWindowMinOffset;
            // 预分配好的 Reocord 存储的起始位置
            private final long walOffset;

            // 预分配好的 Reocord 的大小
            private final int length;

            public CallbackResult(long pendingIOWindowMinOffset, long walOffset, int length) {
                this.pendingIOWindowMinOffset = pendingIOWindowMinOffset;
                this.walOffset = walOffset;
                this.length = length;
            }

            public long getPendingIOWindowMinOffset() {
                return pendingIOWindowMinOffset;
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
     * 优雅 shutodnw 流程
     * 调用层等待所有 Append 完成
     * 上传 S3 完成
     * 调用 trim
     * 然后调用 shutdown，保证 trim offset 持久化
     */
    void shutdown();

    AppendResult append(ByteBuffer record, //
                        int crc, //
                        FutureCallback<AppendResult.CallbackResult> callback //
    ) throws OverCapacityException;

    // PendingIO Window 由一个最大值，512MB，不能无限扩容。


    Iterator<RecordEntity> recover();

    /**
     * 抹除所有小于 offset 的数据。
     * >= offset 的数据仍然可以读取。
     */
    void trim(long offset);
}

