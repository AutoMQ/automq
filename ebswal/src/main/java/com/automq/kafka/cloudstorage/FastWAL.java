package com.automq.kafka.cloudstorage;

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
        private long beginOffset;

        public RecordEntity(ByteBuffer record, long beginOffset) {
            this.record = record;
            this.beginOffset = beginOffset;
        }

        public ByteBuffer getRecord() {
            return record;
        }

        public long getBeginOffset() {
            return beginOffset;
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

