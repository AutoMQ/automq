package kafka.automq.zerozone;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class LegacyRecordConverter {

    public static MemoryRecords maybeConvert(MemoryRecords records) {
        byte toMagic = RecordBatch.MAGIC_VALUE_V2;
        if (records.hasMatchingMagic(toMagic)) {
            return records;
        }
        // https://kafka.apache.org/30/implementation/message-format/
        // The magic0-1 MemoryRecords has three types:
        // - single message set => RecordBatch(Record)
        // - multiple message set without compression => RecordBatch(Record), RecordBatch(Record), RecordBatch(Record), ...
        // - single message set with compression => RecordBatch(Record, Record, ...)
        // zerozone expects only single RecordBatch per partition in ProduceRequest. So we need to convert all above three types into single RecordBatch.

        // copy from LogValidator#convertAndAssignOffsetsNonCompressed
        int sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagic, 0,
            CompressionType.NONE, records.records());
        try {
            records.batches().iterator().next();
        } catch (NoSuchElementException e) {
            throw new InvalidRecordException("Record batch has no batches at all");
        }
        ByteBuffer newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion);
        @SuppressWarnings("resource")
        MemoryRecordsBuilder builder = MemoryRecords.builder(newBuffer, toMagic, Compression.NONE,
            TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        int recordIndex = 0;
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                builder.appendWithOffset(recordIndex, record);
                ++recordIndex;
            }
        }
        return builder.build();
    }

}
