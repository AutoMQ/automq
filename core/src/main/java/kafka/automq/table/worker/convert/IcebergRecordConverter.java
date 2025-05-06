package kafka.automq.table.worker.convert;

import org.apache.iceberg.data.Record;

public interface IcebergRecordConverter<R> {
    Record convertRecord(R record);

    /**
     * Return processed field count
     */
    long fieldCount();
}