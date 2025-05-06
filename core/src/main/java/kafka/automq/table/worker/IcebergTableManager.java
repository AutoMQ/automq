/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table.worker;

import kafka.automq.table.utils.PartitionUtil;

import com.google.common.annotations.VisibleForTesting;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergTableManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableManager.class);
    private final Catalog catalog;
    private final TableIdentifier tableId;
    private final WorkerConfig config;
    private volatile Table table;
    private volatile PartitionSpec partitionSpec;

    public IcebergTableManager(Catalog catalog, TableIdentifier tableId, WorkerConfig config) {
        this.catalog = catalog;
        this.tableId = tableId;
        this.config = config;
    }

    public Table getTableOrCreate(Schema schema) {
        Table currentTable = table;
        if (currentTable == null) {
            synchronized (this) {
                currentTable = table;
                if (currentTable == null) {
                    table = currentTable = getTableOrCreate(schema, 1);
                    partitionSpec = currentTable.spec();
                }
            }
        }
        return currentTable;
    }

    public Table getTableOrCreate(Schema schema, int retries) {
        AtomicReference<Table> result = new AtomicReference<>();
        Tasks.range(1)
            .retry(retries)
            .run(notUsed -> {
                try {
                    result.set(catalog.loadTable(tableId));
                } catch (NoSuchTableException e) {
                    if (catalog instanceof SupportsNamespaces) {
                        SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
                        try {
                            namespaceCatalog.loadNamespaceMetadata(tableId.namespace());
                        } catch (NoSuchNamespaceException e2) {
                            LOGGER.info("Namespace {} does not exist, creating", tableId.namespace());
                            try {
                                ((SupportsNamespaces) catalog).createNamespace(tableId.namespace());
                            } catch (AlreadyExistsException e3) {
                                LOGGER.info("Namespace {} already exists", tableId.namespace());
                            }
                        }
                    }
                    try {
                        PartitionSpec spec = PartitionUtil.buildPartitionSpec(config.partitionBy(), schema);
                        Map<String, String> options = new HashMap<>();
                        options.put(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
                        options.put(TableProperties.OBJECT_STORE_ENABLED, "true");
                        LOGGER.info("Table {} does not exist, create with schema={}, partition={}", tableId, schema, spec);
                        result.set(catalog.createTable(tableId, schema, spec, options));
                    } catch (AlreadyExistsException e1) {
                        LOGGER.info("Table {} already exists", tableId);
                        result.set(catalog.loadTable(tableId));
                    }
                }
            });
        return result.get();
    }

    public boolean handleSchemaChangesWithFlush(Record record, FlushAction flush) throws IOException {
        Table currentTable = getTableOrCreate(record.struct().asSchema());
        List<SchemaChange> changes = checkSchemaChanges(currentTable, record);
        if (changes.isEmpty()) {
            return false;
        }

        flush.perform();

        applySchemaChange(currentTable, changes);
        return true;
    }

    public TableIdentifier tableId() {
        return tableId;
    }

    public PartitionSpec spec() {
        return partitionSpec;
    }

    public void reset() {
        table = null;
    }

    /**
     * Check schema changes between the current record and the table schema
     *
     * @param record current record
     * @return list of schema changes
     */
    @VisibleForTesting
    protected List<SchemaChange> checkSchemaChanges(Table table, Record record) {
        Schema currentSchema = record.struct().asSchema();
        return collectSchemaChanges(currentSchema, table);
    }

    /**
     * Apply schema changes to the table
     *
     * @param changes list of schema changes
     */
    @VisibleForTesting
    protected synchronized void applySchemaChange(Table table, List<SchemaChange> changes) {
        LOGGER.info("Applying schema changes to table {}, changes {}", tableId, changes);
        Tasks.range(1)
            .retry(2)
            .run(notUsed -> applyChanges(table, changes));
        table.refresh();
    }

    private static UpdateSchema applySchemaChange(UpdateSchema updateSchema, SchemaChange change) {
        switch (change.getType()) {
            case ADD_COLUMN:
                if (change.getParentName() == null) {
                    return updateSchema
                        .addColumn(change.getColumnName(), change.getNewType());
                } else {
                    return updateSchema
                        .addColumn(change.getParentName(), change.getColumnName(), change.getNewType());
                }
            case MAKE_OPTIONAL:
                return updateSchema
                    .makeColumnOptional(change.getColumnName());
            case PROMOTE_TYPE:
                return updateSchema
                    .updateColumn(change.getColumnName(), change.getNewType());
            default:
                return updateSchema;
        }
    }

    private static boolean shouldSkipChange(org.apache.iceberg.Schema schema, SchemaChange change) {
        Types.NestedField field = schema.findField(change.getColumnFullName());
        switch (change.getType()) {
            case ADD_COLUMN: {
                return field != null;
            }
            case MAKE_OPTIONAL: {
                return field.isOptional();
            }
            case PROMOTE_TYPE: {
                return field.type().equals(change.getNewType());
            }
            default: {
                return false;
            }
        }
    }

    private List<SchemaChange> collectSchemaChanges(Schema currentSchema, Table table) {
        Schema tableSchema = table.schema();
        List<SchemaChange> changes = new ArrayList<>();

        for (Types.NestedField currentField : currentSchema.columns()) {
            collectFieldChanges(currentField, null, tableSchema, changes);
        }
        return changes;
    }

    private void collectFieldChanges(Types.NestedField currentField, String parentName, Schema tableSchema,
        List<SchemaChange> changes) {
        String fullName = parentName == null ? currentField.name() : parentName + "." + currentField.name();
        Types.NestedField tableField = tableSchema.findField(fullName);

        if (tableField == null) {
            changes.add(new SchemaChange(SchemaChange.ChangeType.ADD_COLUMN, currentField.name(), currentField.type().asPrimitiveType(), parentName));
        } else {
            // process optional fields
            if (!tableField.isOptional() && currentField.isOptional()) {
                changes.add(new SchemaChange(SchemaChange.ChangeType.MAKE_OPTIONAL, fullName, null, parentName));
            }

            // promote type if needed
            if (!tableField.type().equals(currentField.type()) && canPromoteType(tableField.type(), currentField.type())) {
                changes.add(new SchemaChange(SchemaChange.ChangeType.PROMOTE_TYPE, fullName, currentField.type().asPrimitiveType(), parentName));
            }

            // if it is a nested field, recursively process subfields
            if (currentField.type().isStructType() && tableField.type().isStructType()) {
                List<Types.NestedField> currentSubFields = currentField.type().asStructType().fields();

                for (Types.NestedField currentSubField : currentSubFields) {
                    collectFieldChanges(currentSubField, fullName, tableSchema, changes);
                }
            }
        }
    }

    private boolean canPromoteType(Type oldType, Type newType) {
        if (oldType.typeId() == Type.TypeID.INTEGER && newType.typeId() == Type.TypeID.LONG) {
            return true;
        }
        return oldType.typeId() == Type.TypeID.FLOAT && newType.typeId() == Type.TypeID.DOUBLE;
    }

    private void applyChanges(Table table, List<SchemaChange> changes) {
        table.refresh();
        UpdateSchema updateSchema = table.updateSchema();
        changes.stream().filter(c -> !shouldSkipChange(table.schema(), c))
            .forEach(c -> applySchemaChange(updateSchema, c));
        updateSchema.commit();
    }

    @FunctionalInterface
    public interface FlushAction {
        void perform() throws IOException;
    }

    static class SchemaChange {
        private final ChangeType type;
        private final String columnName;
        private final Type.PrimitiveType newType;
        private final String parentName;  // For nested fields

        public SchemaChange(ChangeType type, String columnName, Type.PrimitiveType newType, String parentName) {
            this.type = type;
            this.columnName = columnName;
            this.newType = newType;
            this.parentName = parentName;
        }

        public ChangeType getType() {
            return type;
        }

        public String getColumnName() {
            return columnName;
        }

        public Type.PrimitiveType getNewType() {
            return newType;
        }

        public String getParentName() {
            return parentName;
        }

        public String getColumnFullName() {
            return parentName == null ? columnName : parentName + "." + columnName;
        }

        enum ChangeType {
            ADD_COLUMN,
            MAKE_OPTIONAL,
            PROMOTE_TYPE
        }
    }
}
