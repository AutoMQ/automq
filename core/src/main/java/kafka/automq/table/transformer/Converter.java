package kafka.automq.table.transformer;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;

public interface Converter {
    /**
     * Convert a Kafka record to an Iceberg record
     * * 1. If the table is not configured, create an Iceberg record based on the record's schema
     * * 2. If the table is configured, create an Iceberg record based on the table's schema
     * * 3. If the record's schema is inconsistent with the table's schema, create an Iceberg record based on the record's schema
     * @param record Kafka record
     * @return Iceberg record
     */
    Record convert(org.apache.kafka.common.record.Record record);

    /**
     * Configure the table for the converter
     * when converting, the fieldId should be consistent with the actual fieldId
     * if the table is not configured, create an Iceberg record based on the record's schema,
     * the fieldId in the record may not be consistent with the actual fieldId
     * @param tableSchema the Iceberg table schema
     */
    void tableSchema(Schema tableSchema);

    /**
     * Get current schema information
     * @return Current schema state including ID and schema
     */
    SchemaState currentSchema();

    /**
     * Get processed field count.
     * @return processed field count
     */
    long fieldCount();
}
