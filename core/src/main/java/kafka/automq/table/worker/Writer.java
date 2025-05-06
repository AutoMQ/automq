package kafka.automq.table.worker;

import kafka.automq.table.events.PartitionMetric;
import kafka.automq.table.events.TopicMetric;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.io.WriteResult;

public interface Writer {

    /**
     * Write record to memory
     */
    void write(int partition, org.apache.kafka.common.record.Record kafkaRecord) throws IOException;

    /**
     * Asynchronously flush the data from memory to persistence storage
     */
    CompletableFuture<Void> flush(FlushMode flushMode, ExecutorService flushExecutor, Executor eventLoop);

    /**
     * Abort the writer and clean up the inflight resources.
     */
    void abort() throws IOException;

    /**
     * Complete the writer and return the results
     */
    List<WriteResult> complete() throws IOException;

    /**
     * Get the results of the completed writer
     */
    List<WriteResult> results();

    /**
     * Check if the writer is completed
     */
    boolean isCompleted();

    /**
     * Check if the writer is full if full should switch to a new writer.
     */
    boolean isFull();

    /**
     * Get partition to the offset range map .
     */
    Map<Integer, OffsetRange> getOffsets();

    /**
     * Get partition offset range.
     */
    IcebergWriter.OffsetRange getOffset(int partition) ;

    /**
     * Set partition initial offset range [offset, offset).
     */
    void setOffset(int partition, long offset);

    /**
     * Set new end offset for the partition.
     */
    void setEndOffset(int partition, long offset);

    /**
     * Get in memory dirty bytes.
     */
    long dirtyBytes();

    void updateWatermark(int partition, long timestamp);

    TopicMetric topicMetric();

    Map<Integer, PartitionMetric> partitionMetrics();

    int targetFileSize();

    class OffsetRange {
        long start;
        long end;

        public OffsetRange(long start) {
            this.start = start;
            this.end = start;
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

    }
}
