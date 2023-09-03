package com.automq.kafka.cloudstorage;

public interface AppendResult {
    long beginOffset();

    class CallbackResult extends Object {
        long pendingIOFistCommitOffset;
        long beginOffset;

        public long getPendingIOFistCommitOffset() {
            return pendingIOFistCommitOffset;
        }

        public void setPendingIOFistCommitOffset(long pendingIOFistCommitOffset) {
            this.pendingIOFistCommitOffset = pendingIOFistCommitOffset;
        }

        public long getBeginOffset() {
            return beginOffset;
        }

        public void setBeginOffset(long beginOffset) {
            this.beginOffset = beginOffset;
        }
    }
}
