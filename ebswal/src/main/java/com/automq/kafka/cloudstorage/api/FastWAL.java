package com.automq.kafka.cloudstorage.api;

import com.google.common.util.concurrent.FutureCallback;

import java.nio.ByteBuffer;
import java.util.List;

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
        private ByteBuffer record;
        private long recordBodyBeginOffset;

        public RecordEntity(ByteBuffer record, long recordBodyBeginOffset) {
            this.record = record;
            this.recordBodyBeginOffset = recordBodyBeginOffset;
        }

        public ByteBuffer getRecord() {
            return record;
        }

        public long getRecordBodyBeginOffset() {
            return recordBodyBeginOffset;
        }
    }

    interface AppendResult {
        // 预分配好的 Reocord body 存储的起始位置
        long recordBodyBeginOffset();

        int recordBodySize();

        class CallbackResult {
            // Pending IO Window 的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
            private final long pendingIOWindowMinOffset;
            // 预分配好的 Reocord 存储的起始位置
            private final long recordBodyBeginOffset;

            // 预分配好的 Reocord 的大小
            private final int recordBodySize;

            public CallbackResult(long pendingIOWindowMinOffset, long recordBodyBeginOffset, int recordBodySize) {
                this.pendingIOWindowMinOffset = pendingIOWindowMinOffset;
                this.recordBodyBeginOffset = recordBodyBeginOffset;
                this.recordBodySize = recordBodySize;
            }

            private long getPendingIOWindowMinOffset() {
                return pendingIOWindowMinOffset;
            }

            private long getRecordBodyBeginOffset() {
                return recordBodyBeginOffset;
            }

            private int getRecordBodySize() {
                return recordBodySize;
            }
        }
    }


    /**
     * 启动线程，加载元数据
     */
    void start();

    /**
     * 阻止新的 Append 写入，等待已经写入的 Append 完成。
     * 为优雅关闭 EBS WAL Service 服务。
     * 第一步：调用 stopAppending() 方法，阻止新的 Append 写入。
     * 第二步：将所有数据上传 S3
     * 第三步：调用 trim() 方法，删除本地数据
     * 第四步：调用 shutdown() 方法，关闭线程
     */
    void stopAppending();


    /**
     * 关闭线程，保存元数据，其中包含 trim offset。
     */
    void shutdown();

    AppendResult append(ByteBuffer record, //
                        int crc, //
                        FutureCallback<AppendResult.CallbackResult> callback //
    ) throws OverCapacityException;


    List<RecordEntity> read();

    void trim(long offset);
}

