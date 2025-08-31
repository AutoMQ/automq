package kafka.automq.table.worker;

import kafka.automq.table.process.RecordProcessorFactory;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergWriterFactory implements WriterFactory {
    private final TableIdentifier tableIdentifier;
    private final IcebergTableManager icebergTableManager;
    private final RecordProcessorFactory recordProcessorFactory;
    private final WorkerConfig config;
    private final String topic;

    public IcebergWriterFactory(Catalog catalog, TableIdentifier tableIdentifier, RecordProcessorFactory recordProcessorFactory, WorkerConfig config, String topic) {
        this.topic = topic;
        this.tableIdentifier = tableIdentifier;
        this.icebergTableManager = new IcebergTableManager(catalog, tableIdentifier, config);
        this.recordProcessorFactory = recordProcessorFactory;
        this.config = config;
    }

    @Override
    public Writer newWriter() {
        return new IcebergWriter(icebergTableManager,
            recordProcessorFactory.create(config, topic), config);
    }

    @Override
    public PartitionSpec partitionSpec() {
        return icebergTableManager.spec();
    }

    @Override
    public void reset() {
        icebergTableManager.reset();
    }
}
