package com.automq.kafka.cloudstorage;

public interface AppendResult {
    // 预分配好的 Reocord 存储的起始位置
    long beginOffset();

    class CallbackResult extends Object {
        // Pending IO Window 的最小 Offset，此 Offset 之前的数据已经全部成功写入存储设备
        long confirmedMinOffset;
        // 预分配好的 Reocord 存储的起始位置
        long beginOffset;

        public long getConfirmedMinOffset() {
            return confirmedMinOffset;
        }

        public void setConfirmedMinOffset(long confirmedMinOffset) {
            this.confirmedMinOffset = confirmedMinOffset;
        }

        public long getBeginOffset() {
            return beginOffset;
        }

        public void setBeginOffset(long beginOffset) {
            this.beginOffset = beginOffset;
        }
    }
}
