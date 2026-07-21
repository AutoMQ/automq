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
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
                        List<String> properties = config.icebergAutoCreateProperties();
                        if (properties != null) {
                            for (String prop : properties) {
                                int index = prop.indexOf('=');
                                if (index > 0) {
                                    String key = prop.substring(0, index).trim();
                                    String value = prop.substring(index + 1).trim();
                                    options.put(key, value);
                                }
                            }
                        }
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

    public boolean handleSchemaChangesWithFlush(Schema schema, FlushAction flush) throws IOException {
        Table currentTable = getTableOrCreate(schema);
        List<SchemaChange> changes = checkSchemaChanges(currentTable, schema);
        Set<String> expectedIdentifierFieldNames = identifierFieldNames(schema);
        boolean identifierFieldsChanged = identifierFieldsChanged(currentTable, expectedIdentifierFieldNames);
        if (changes.isEmpty() && !identifierFieldsChanged) {
            return false;
        }

        flush.perform();

        applySchemaChange(currentTable, changes, expectedIdentifierFieldNames);
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
     */
    @VisibleForTesting
    protected List<SchemaChange> checkSchemaChanges(Table table, Schema currentSchema) {
        return collectSchemaChanges(currentSchema, table);
    }

    /**
     * Apply schema changes to the table
     *
     * @param changes list of schema changes
     */
    @VisibleForTesting
    protected void applySchemaChange(Table table, List<SchemaChange> changes) {
        applySchemaChange(table, changes, Set.of());
    }

    private synchronized void applySchemaChange(Table table, List<SchemaChange> changes, Set<String> expectedIdentifierFieldNames) {
        LOGGER.info("Applying schema changes to table {}, changes {}, identifier fields {}",
            tableId, changes.stream().map(c -> c.getType() + ":" + c.getColumnFullName()).toList(), expectedIdentifierFieldNames);
        Tasks.range(1)
            .retry(2)
            .run(notUsed -> applyChanges(table, changes, expectedIdentifierFieldNames));
        table.refresh();
    }

    @VisibleForTesting
    protected void ensureIdentifierFields(Table table, Set<String> expectedIdentifierFieldNames) {
        if (expectedIdentifierFieldNames.isEmpty()) {
            return;
        }
        table.refresh();
        Set<String> currentIdentifierFieldNames = identifierFieldNames(table.schema());
        if (currentIdentifierFieldNames.equals(expectedIdentifierFieldNames)) {
            return;
        }
        LOGGER.info("Setting identifier fields for table {}, fields {}, previous fields {}",
            tableId, expectedIdentifierFieldNames, currentIdentifierFieldNames);
        applySchemaChange(table, List.of(), expectedIdentifierFieldNames);
    }

    private boolean identifierFieldsChanged(Table table, Set<String> expectedIdentifierFieldNames) {
        return !expectedIdentifierFieldNames.isEmpty()
            && !identifierFieldNames(table.schema()).equals(expectedIdentifierFieldNames);
    }

    private Set<String> identifierFieldNames(Schema schema) {
        return schema.identifierFieldIds().stream()
            .map(id -> schema.findField(id).name())
            .collect(Collectors.toSet());
    }

    private static UpdateSchema applySchemaChange(UpdateSchema updateSchema, SchemaChange change) {
        switch (change.getType()) {
            case ADD_COLUMN:
                if (change.getParentName() == null) {
                    return updateSchema
                        .addColumn(change.getColumnFullName(), change.getNewType());
                } else {
                    return updateSchema
                        .addColumn(change.getParentName(), change.getColumnName(), change.getNewType());
                }
            case MAKE_OPTIONAL:
                return updateSchema
                    .makeColumnOptional(change.getColumnFullName());
            case PROMOTE_TYPE:
                return updateSchema
                    .updateColumn(change.getColumnFullName(), change.getNewType().asPrimitiveType());
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
                return field != null && field.isOptional();
            }
            case PROMOTE_TYPE: {
                return field != null && field.type().equals(change.getNewType());
            }
            default: {
                return false;
            }
        }
    }

    protected List<SchemaChange> collectSchemaChanges(Schema currentSchema, Table table) {
        Schema tableSchema = table.schema();
        List<SchemaChange> changes = new ArrayList<>();

        for (Types.NestedField currentField : currentSchema.columns()) {
            collectFieldChanges(currentField, null, tableSchema, changes);
        }

        for (Types.NestedField tableField : tableSchema.columns()) {
            collectRemovedField(tableField, null, currentSchema, changes);
        }
        return changes;
    }

    private void collectRemovedField(Types.NestedField tableField, String parentName, Schema currentSchema,
                                     List<SchemaChange> changes) {
        String fieldName = tableField.name();
        String fullFieldName = parentName == null ? fieldName : parentName + "." + fieldName;
        Types.NestedField currentField = currentSchema.findField(fullFieldName);

        // if field doesn't exist in current schema and it's not a struct, mark it as optional (soft removal)
        if (currentField == null && !tableField.isOptional()) {
            changes.add(new SchemaChange(SchemaChange.ChangeType.MAKE_OPTIONAL, fieldName,
                null, parentName));
            return;
        }
        // if it is a nested field, recursively process subfields
        if (tableField.type().isStructType()) {
            collectRemovedStructFields(tableField.type().asStructType().fields(), fullFieldName, currentSchema, changes);
        } else if (isStructList(tableField.type())) {
            collectRemovedStructFields(tableField.type().asListType().elementType().asStructType().fields(),
                fullFieldName + ".element", currentSchema, changes);
        } else if (isStructMap(tableField.type())) {
            collectRemovedStructFields(tableField.type().asMapType().valueType().asStructType().fields(),
                fullFieldName + ".value", currentSchema, changes);
        }
    }

    private void collectFieldChanges(Types.NestedField currentField, String parentName, Schema tableSchema,
        List<SchemaChange> changes) {
        String fieldName = currentField.name();
        String fullFieldName = parentName == null ? fieldName : parentName + "." + fieldName;
        Types.NestedField tableField = tableSchema.findField(fullFieldName);

        if (tableField == null) {
            changes.add(new SchemaChange(SchemaChange.ChangeType.ADD_COLUMN, fieldName,
                currentField.type(), parentName));
            return;
        } else {
            Type currentType = currentField.type();
            Type tableType = tableField.type();
            if (currentType.isStructType() && tableType.isStructType()) {
                collectStructFieldChanges(currentType.asStructType().fields(), fullFieldName, tableSchema, changes);
                collectOptionalFieldChanges(currentField, parentName, changes, tableField, fieldName);
            } else if (isStructList(currentType) && isStructList(tableType)) {
                collectStructFieldChanges(currentType.asListType().elementType().asStructType().fields(),
                    fullFieldName + ".element", tableSchema, changes);
            } else if (isStructMap(currentType) && isStructMap(tableType)) {
                collectStructFieldChanges(currentType.asMapType().valueType().asStructType().fields(),
                    fullFieldName + ".value", tableSchema, changes);
            } else if (!currentType.isStructType() && !tableType.isStructType()) {
                collectOptionalFieldChanges(currentField, parentName, changes, tableField, fieldName);

                if (!tableType.equals(currentType) && canPromoteType(tableType, currentType)) {
                    changes.add(new SchemaChange(SchemaChange.ChangeType.PROMOTE_TYPE, fieldName, currentType, parentName));
                }
            }
        }
    }

    private static void collectOptionalFieldChanges(Types.NestedField currentField, String parentName, List<SchemaChange> changes, Types.NestedField tableField, String fieldName) {
        if (!tableField.isOptional() && currentField.isOptional()) {
            changes.add(new SchemaChange(SchemaChange.ChangeType.MAKE_OPTIONAL, fieldName, null, parentName));
        }
    }

    private void collectStructFieldChanges(List<Types.NestedField> currentSubFields, String parentFullName,
        Schema tableSchema, List<SchemaChange> changes) {
        for (Types.NestedField currentSubField : currentSubFields) {
            collectFieldChanges(currentSubField, parentFullName, tableSchema, changes);
        }
    }

    private void collectRemovedStructFields(List<Types.NestedField> tableSubFields, String parentFullName,
        Schema currentSchema, List<SchemaChange> changes) {
        for (Types.NestedField tableSubField : tableSubFields) {
            collectRemovedField(tableSubField, parentFullName, currentSchema, changes);
        }
    }

    private boolean isStructList(Type type) {
        return type.typeId() == Type.TypeID.LIST && type.asListType().elementType().isStructType();
    }

    private boolean isStructMap(Type type) {
        return type.typeId() == Type.TypeID.MAP && type.asMapType().valueType().isStructType();
    }

    private boolean canPromoteType(Type oldType, Type newType) {
        if (oldType.typeId() == Type.TypeID.INTEGER && newType.typeId() == Type.TypeID.LONG) {
            return true;
        }
        return oldType.typeId() == Type.TypeID.FLOAT && newType.typeId() == Type.TypeID.DOUBLE;
    }

    private void applyChanges(Table table, List<SchemaChange> changes, Set<String> expectedIdentifierFieldNames) {
        table.refresh();
        UpdateSchema updateSchema = table.updateSchema();
        boolean changed = false;
        for (SchemaChange change : changes) {
            if (!shouldSkipChange(table.schema(), change)) {
                applySchemaChange(updateSchema, change);
                changed = true;
            }
        }
        if (!expectedIdentifierFieldNames.isEmpty()
            && !identifierFieldNames(table.schema()).equals(expectedIdentifierFieldNames)) {
            updateSchema.setIdentifierFields(expectedIdentifierFieldNames);
            changed = true;
        }
        if (!changed) {
            return;
        }
        updateSchema.commit();
    }

    @FunctionalInterface
    public interface FlushAction {
        void perform() throws IOException;
    }

    static class SchemaChange {
        private final ChangeType type;
        private final String columnName;
        private final Type newType;
        private final String parentName;  // For nested fields

        public SchemaChange(ChangeType type, String columnName, Type newType, String parentName) {
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

        public Type getNewType() {
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
