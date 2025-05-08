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

import com.google.common.collect.ImmutableMap;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class IcebergTableManagerTest {
    private InMemoryCatalog catalog;
    private IcebergTableManager icebergTableManager;

    @BeforeEach
    public void setup() {
        catalog = initializeCatalog();
        catalog.createNamespace(Namespace.of("default"));
    }

    private String generateRandomTableName() {
        int randomNum = ThreadLocalRandom.current().nextInt(1000, 10000);
        return "my_table_" + randomNum;
    }

    @Test
    public void testTableCreationAndLoad() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        Table table = icebergTableManager.getTableOrCreate(schema);
        assertNotNull(table, "Table should not be null");

        Table loadedTable = icebergTableManager.getTableOrCreate(schema);
        assertEquals(table, loadedTable, "Loaded table should be the same as the created table");
    }

    @Test
    public void testTableCreationWithDotInName() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name.test", Types.StringType.get()));

        Table table = icebergTableManager.getTableOrCreate(schema);
        assertNotNull(table, "Table should not be null");

        Table loadedTable = icebergTableManager.getTableOrCreate(schema);
        assertEquals(table, loadedTable, "Loaded table should be the same as the created table");

        Types.NestedField field = loadedTable.schema().findField("name.test");
        assertNotNull(field, "Field 'name.test' should exist in the table schema");
        assertEquals(field.name(), "name.test", "Field name should be 'name.test'");
    }

    @Test
    public void testCheckAndApplySchemaChanges() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        Table table = icebergTableManager.getTableOrCreate(initialSchema);

        Schema updatedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get()));

        Record record = GenericRecord.create(updatedSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, record);
        assertFalse(schemaChanges.isEmpty(), "Schema changes should be applied");
        icebergTableManager.applySchemaChange(table, schemaChanges);

        // Reload table and verify the schema changes
        Table updatedTable = catalog.loadTable(tableId);
        List<Types.NestedField> columns = updatedTable.schema().columns();
        assertEquals(3, columns.size(), "Table schema should have three columns");
        assertEquals(Types.LongType.get(), columns.get(0).type(), "Column 'id' should be of type Long");
    }

    @Test
    public void testAddNewColumn() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = icebergTableManager.getTableOrCreate(initialSchema);

        Schema updatedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get()));

        Record record = GenericRecord.create(updatedSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, record);
        assertFalse(schemaChanges.isEmpty(), "New columns should be added");
        icebergTableManager.applySchemaChange(table, schemaChanges);

        // Reload table and verify the new columns
        Table updatedTable = catalog.loadTable(tableId);
        List<Types.NestedField> columns = updatedTable.schema().columns();
        assertEquals(3, columns.size(), "Table schema should have three columns");
        assertEquals(Types.StringType.get(), columns.get(2).type(), "Column 'email' should be of type String");
    }

    @Test
    public void testMakeColumnOptional() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = icebergTableManager.getTableOrCreate(initialSchema);

        Schema updatedSchema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()));

        Record record = GenericRecord.create(updatedSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, record);
        assertFalse(schemaChanges.isEmpty(), "Column 'id' should be made optional");
        icebergTableManager.applySchemaChange(table, schemaChanges);

        // Reload table and verify column is optional
        Table updatedTable = catalog.loadTable(tableId);
        Types.NestedField column = updatedTable.schema().findField("id");
        assertTrue(column.isOptional(), "Column 'id' should be optional");
    }

    @Test
    public void testNoSchemaChanges() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        Schema initialSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = icebergTableManager.getTableOrCreate(initialSchema);

        Record record = GenericRecord.create(initialSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, record);
        assertTrue(schemaChanges.isEmpty(), "No schema changes should be detected");
    }

    @Test
    public void testNoChangesWhenV2AddsColumnAndV1RecordProvided() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        // v2 Schema with additional column
        Schema v2Schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get()));

        // Create the table with v2 schema
        Table table = icebergTableManager.getTableOrCreate(v2Schema);

        // v1 Record Schema without the new column
        Schema v1Schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record v1Record = GenericRecord.create(v1Schema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, v1Record);
        assertTrue(schemaChanges.isEmpty(), "No schema changes should be applied in this case");

        // Verify the schema remains unchanged
        List<Types.NestedField> columns = catalog.loadTable(tableId).schema().columns();
        assertEquals(3, columns.size(), "Table schema should have three columns");
        assertEquals(Types.StringType.get(), columns.get(2).type(), "Column 'email' should exist and be of type String");
    }

    @Test
    public void testNoChangesWhenV2SetsFieldToOptionalAndV1RecordProvided() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        // v2 Schema with "id" set to optional
        Schema v2Schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        // Create the table with v2 schema
        Table table = icebergTableManager.getTableOrCreate(v2Schema);

        // v1 Record Schema with "id" required
        Schema v1Schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record v1Record = GenericRecord.create(v1Schema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, v1Record);
        assertTrue(schemaChanges.isEmpty(), "No schema changes should be applied in this case");

        // Verify the schema remains unchanged
        List<Types.NestedField> columns = catalog.loadTable(tableId).schema().columns();
        assertEquals(2, columns.size(), "Table schema should have two columns");
        assertTrue(columns.get(0).isOptional(), "Column 'id' should remain optional");
    }

    @Test
    public void testNoChangesWhenV2PromotesTypeAndV1RecordProvided() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);
        icebergTableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));

        // v2 Schema with "id" type promoted to Long
        Schema v2Schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        // Create the table with v2 schema
        Table table = icebergTableManager.getTableOrCreate(v2Schema);

        // v1 Record Schema with "id" as Integer
        Schema v1Schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record v1Record = GenericRecord.create(v1Schema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, v1Record);

        assertTrue(schemaChanges.isEmpty(), "No schema changes should be applied in this case");

        // Verify the schema remains unchanged
        List<Types.NestedField> columns = catalog.loadTable(tableId).schema().columns();
        assertEquals(2, columns.size(), "Table schema should have two columns");
        assertEquals(Types.LongType.get(), columns.get(0).type(), "Column 'id' should remain of type Long");
    }

    @Test
    public void testUpdateTableOnErrorRetry() {
        String tableName = generateRandomTableName();
        TableIdentifier tableId = TableIdentifier.of("default", tableName);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));

        Schema updatedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "email", Types.StringType.get()));

        Catalog mockCatalog = mock(Catalog.class);
        Table mockTable = mock(Table.class);
        UpdateSchema mockUpdateSchema = mock(UpdateSchema.class);

        when(mockCatalog.loadTable(eq(tableId))).thenThrow(new NoSuchTableException("Table not found"));
        when(mockCatalog.createTable(eq(tableId), eq(schema), any(), any())).thenReturn(mockTable);
        when(mockTable.schema()).thenReturn(schema);
        when(mockTable.updateSchema()).thenReturn(mockUpdateSchema);

        // Ensure that addColumn method also returns the mock for fluent API chaining
        when(mockUpdateSchema.addColumn("email", Types.StringType.get())).thenReturn(mockUpdateSchema);

        // Configure commit to throw exception on first call and succeed on second call
        doAnswer(new Answer<Void>() {
            private int count = 0;

            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                if (count < 2) {
                    count++;
                    throw new RuntimeException("Commit Error");
                }
                return null; // No exception on the second call
            }
        }).when(mockUpdateSchema).commit();

        IcebergTableManager icebergTableManager = new IcebergTableManager(mockCatalog, tableId, mock(WorkerConfig.class));
        Table table = icebergTableManager.getTableOrCreate(schema);

        Record record = GenericRecord.create(updatedSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = icebergTableManager.checkSchemaChanges(table, record);
        assertFalse(schemaChanges.isEmpty(), "Schema changes should be applied after retrying on error");
        icebergTableManager.applySchemaChange(table, schemaChanges);
    }

    private InMemoryCatalog initializeCatalog() {
        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize("test", ImmutableMap.of());
        return inMemoryCatalog;
    }
}