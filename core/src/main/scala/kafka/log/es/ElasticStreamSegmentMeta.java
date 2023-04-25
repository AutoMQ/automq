package kafka.log.es;

public class ElasticStreamSegmentMeta {
    private long segmentBaseOffset;
    private long segmentEndOffset = -1L;
    private long dataStreamStartOffset;
    private long timeStreamStartOffset;
    private long txnStreamStartOffset;

    public ElasticStreamSegmentMeta() {
    }

    public long getSegmentBaseOffset() {
        return segmentBaseOffset;
    }

    public void setSegmentBaseOffset(long segmentBaseOffset) {
        this.segmentBaseOffset = segmentBaseOffset;
    }

    public long getDataStreamStartOffset() {
        return dataStreamStartOffset;
    }

    public void setDataStreamStartOffset(long dataStreamStartOffset) {
        this.dataStreamStartOffset = dataStreamStartOffset;
    }

    public long getTimeStreamStartOffset() {
        return timeStreamStartOffset;
    }

    public void setTimeStreamStartOffset(long timeStreamStartOffset) {
        this.timeStreamStartOffset = timeStreamStartOffset;
    }

    public long getTxnStreamStartOffset() {
        return txnStreamStartOffset;
    }

    public void setTxnStreamStartOffset(long txnStreamStartOffset) {
        this.txnStreamStartOffset = txnStreamStartOffset;
    }

    public long getSegmentEndOffset() {
        return segmentEndOffset;
    }

    public void setSegmentEndOffset(long segmentEndOffset) {
        this.segmentEndOffset = segmentEndOffset;
    }
}
