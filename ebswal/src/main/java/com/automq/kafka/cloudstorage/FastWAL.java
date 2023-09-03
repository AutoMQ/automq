package com.automq.kafka.cloudstorage;

import com.google.common.util.concurrent.FutureCallback;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 是一个从零开始无限增长的 WAL，实际实现会使用块设备，每次写入采用块对齐方式。Record 之间非连续存储。
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
     * 关闭线程，保存元数据
     */
    void shutdown();

    AppendResult append(ByteBuffer record, //
                        int crc, //
                        FutureCallback<AppendResult.CallbackResult> callback //
    ) throws OverCapacityException;


    List<RecordEntity> read();

    void trim(long offset);


    long trimedOffset();
}

