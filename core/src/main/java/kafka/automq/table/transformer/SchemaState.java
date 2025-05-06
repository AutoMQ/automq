package kafka.automq.table.transformer;

import org.apache.iceberg.Schema;

/**
 * Represents the current schema state
 */
public record SchemaState(Schema schema, boolean isTableSchemaUsed) {
}
