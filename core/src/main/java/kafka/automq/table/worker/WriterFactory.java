package kafka.automq.table.worker;

import org.apache.iceberg.PartitionSpec;

public interface WriterFactory {

    Writer newWriter();

    PartitionSpec partitionSpec();

    void reset();
}
