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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class IcebergTableManagerTest {
    private InMemoryCatalog catalog;

    @BeforeEach
    public void setup() {
        catalog = initializeCatalog();
        catalog.createNamespace(Namespace.of("default"));
    }

    @Test
    public void shouldCreateTableOnceAndReuseInstance() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        Table table = manager.getTableOrCreate(schema);
        assertNotNull(table);
        assertEquals(table, manager.getTableOrCreate(schema));
    }

    @Test
    public void createsMissingNamespaceBeforeCreatingTable() {
        String namespace = "ns_" + ThreadLocalRandom.current().nextInt(1000, 10000);
        TableIdentifier tableId = TableIdentifier.of(namespace, "table_" + ThreadLocalRandom.current().nextInt(1000, 10000));
        IcebergTableManager manager = newManager(tableId);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        Table table = manager.getTableOrCreate(schema);
        assertNotNull(table);
        // Namespace should now exist and table should be loadable
        assertNotNull(catalog.loadTable(tableId));
    }

    @Test
    public void supportsFieldNamesContainingDots() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name.test", Types.StringType.get()));

        Table table = manager.getTableOrCreate(schema);
        Types.NestedField field = table.schema().findField("name.test");
        assertNotNull(field);
        assertEquals("name.test", field.name());
    }

    @Test
    public void addsPrimitiveColumn() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "email", Types.StringType.get())));

        assertNotNull(updated.schema().findField("email"));
    }

    @Test
    public void promotesPrimitiveColumnType() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get())));

        assertEquals(Types.LongType.get(), updated.schema().findField("id").type());
    }

    @Test
    public void makesColumnOptional() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get())));

        assertTrue(updated.schema().findField("id").isOptional());
    }

    @Test
    public void addsStructColumn() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "address", Types.StructType.of(
                Types.NestedField.optional(3, "street", Types.StringType.get()),
                Types.NestedField.optional(4, "zipCode", Types.IntegerType.get())))));

        Types.NestedField address = updated.schema().findField("address");
        assertNotNull(address);
        assertTrue(address.type().isStructType());
        assertNotNull(updated.schema().findField("address.street"));
        assertNotNull(updated.schema().findField("address.zipCode"));
    }

    @Test
    public void addsNestedFieldInsideStruct() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "address", Types.StructType.of(
                Types.NestedField.optional(3, "street", Types.StringType.get())))));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "address", Types.StructType.of(
                Types.NestedField.optional(3, "street", Types.StringType.get()),
                Types.NestedField.optional(4, "zipCode", Types.IntegerType.get())))));

        assertNotNull(updated.schema().findField("address.zipCode"));
    }

    @Test
    public void addsFieldInsideListElementStruct() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "street", Types.StringType.get()))))));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(
                    Types.NestedField.optional(3, "street", Types.StringType.get()),
                    Types.NestedField.optional(4, "zip", Types.IntegerType.get()))))));

        assertNotNull(updated.schema().findField("addresses.element.zip"));
    }

    @Test
    public void addsFieldInsideMapValueStruct() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(Types.NestedField.optional(4, "city", Types.StringType.get()))))));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.optional(4, "city", Types.StringType.get()),
                    Types.NestedField.optional(5, "country", Types.StringType.get()))))));

        assertNotNull(updated.schema().findField("attributes.value.country"));
    }

    @Test
    public void promotesFieldInsideListElementStruct() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.IntegerType.get()))))));

        Table updated = applyChanges(manager, table, new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.LongType.get()))))));

        assertEquals(Types.LongType.get(), updated.schema().findField("addresses.element.zip").type());
    }

    @Test
    public void exposesPartitionSpecAndAllowsReset() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);
        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        assertNotNull(manager.spec(), "Partition spec should be captured after table creation");

        manager.reset();
        Table reloaded = manager.getTableOrCreate(table.schema());
        assertEquals(table.schema().asStruct(), reloaded.schema().asStruct());
    }

    @Test
    public void handleSchemaChangesWithFlushTriggersFlushOnlyWhenNeeded() throws Exception {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);
        manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        AtomicInteger flushCount = new AtomicInteger();
        boolean changed = manager.handleSchemaChangesWithFlush(newSchema, () -> {
            flushCount.incrementAndGet();
        });
        assertTrue(changed);
        assertEquals(1, flushCount.get());
        assertNotNull(catalog.loadTable(tableId).schema().findField("name"));

        boolean noChange = manager.handleSchemaChangesWithFlush(newSchema, () -> {
            flushCount.incrementAndGet();
        });
        assertFalse(noChange);
        assertEquals(1, flushCount.get());
    }

    @Test
    public void handleSchemaChangesWithFlushPropagatesFlushFailures() throws Exception {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);
        manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get())));

        Schema newSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        assertThrows(IOException.class, () -> manager.handleSchemaChangesWithFlush(newSchema, () -> {
            throw new IOException("flush failed");
        }));

        assertNull(catalog.loadTable(tableId).schema().findField("name"));
    }

    @Test
    public void retriesSchemaCommitOnFailure() {
        TableIdentifier tableId = randomTableId();
        Schema baseSchema = new Schema(
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
        when(mockCatalog.createTable(eq(tableId), eq(baseSchema), any(), any())).thenReturn(mockTable);
        when(mockTable.schema()).thenReturn(baseSchema);
        when(mockTable.updateSchema()).thenReturn(mockUpdateSchema);
        when(mockUpdateSchema.addColumn("email", Types.StringType.get())).thenReturn(mockUpdateSchema);

        doAnswer(new Answer<Void>() {
            private int count = 0;

            @Override
            public Void answer(InvocationOnMock invocation) {
                if (count < 1) {
                    count++;
                    throw new RuntimeException("Commit Error");
                }
                return null;
            }
        }).when(mockUpdateSchema).commit();

        IcebergTableManager manager = new IcebergTableManager(mockCatalog, tableId, mock(WorkerConfig.class));
        Table table = manager.getTableOrCreate(baseSchema);

        Record record = GenericRecord.create(updatedSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertFalse(schemaChanges.isEmpty());
        manager.applySchemaChange(table, schemaChanges);
    }

    @Test
    public void ignoresOlderRecordMissingColumn() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "email", Types.StringType.get())));

        Schema olderSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record record = GenericRecord.create(olderSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertTrue(schemaChanges.isEmpty());
        assertNotNull(catalog.loadTable(tableId).schema().findField("email"));
    }

    @Test
    public void ignoresOlderRecordWhenFieldAlreadyOptional() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())));

        Schema olderSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record record = GenericRecord.create(olderSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertTrue(schemaChanges.isEmpty());
        assertTrue(catalog.loadTable(tableId).schema().findField("id").isOptional());
    }

    @Test
    public void ignoresOlderRecordWhenTypeAlreadyPromoted() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())));

        Schema olderSchema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        Record record = GenericRecord.create(olderSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertTrue(schemaChanges.isEmpty());
        assertEquals(Types.LongType.get(), catalog.loadTable(tableId).schema().findField("id").type());
    }

    @Test
    public void doesNothingWhenSchemasMatch() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = manager.getTableOrCreate(schema);

        Record record = GenericRecord.create(schema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertTrue(schemaChanges.isEmpty());
    }

    @Test
    public void skipsDuplicateNestedAdditions() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Table table = manager.getTableOrCreate(new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "address", Types.StructType.of(
                Types.NestedField.optional(3, "street", Types.StringType.get())))));

        List<IcebergTableManager.SchemaChange> schemaChanges = List.of(
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN,
                "street", Types.StringType.get(), "address"),
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN,
                "zipCode", Types.IntegerType.get(), "address"));

        manager.applySchemaChange(table, schemaChanges);

        Table updatedTable = catalog.loadTable(tableId);
        Types.NestedField addressField = updatedTable.schema().findField("address");
        assertNotNull(addressField);
        List<Types.NestedField> nestedFields = addressField.type().asStructType().fields();
        assertEquals(2, nestedFields.size());
        assertNotNull(updatedTable.schema().findField("address.street"));
        assertNotNull(updatedTable.schema().findField("address.zipCode"));
    }

    @Test
    public void skipsMakeOptionalAndPromoteWhenAlreadyApplied() {
        TableIdentifier tableId = randomTableId();
        IcebergTableManager manager = newManager(tableId);

        Schema tableSchema = new Schema(
            Types.NestedField.optional(1, "name", Types.StringType.get()),
            Types.NestedField.required(2, "id", Types.LongType.get()));

        Table mockTable = mock(Table.class);
        UpdateSchema mockUpdateSchema = mock(UpdateSchema.class);
        when(mockTable.schema()).thenReturn(tableSchema);
        when(mockTable.updateSchema()).thenReturn(mockUpdateSchema);

        List<IcebergTableManager.SchemaChange> schemaChanges = List.of(
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL,
                "name", null, null),
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE,
                "id", Types.LongType.get(), null));

        manager.applySchemaChange(mockTable, schemaChanges);

        verify(mockUpdateSchema, never()).makeColumnOptional("name");
        verify(mockUpdateSchema, never()).updateColumn(eq("id"), any());
        verify(mockUpdateSchema).commit();
    }

    private Table applyChanges(IcebergTableManager manager, Table table, Schema newSchema) {
        Record record = GenericRecord.create(newSchema);
        List<IcebergTableManager.SchemaChange> schemaChanges = manager.checkSchemaChanges(table, record.struct().asSchema());
        assertFalse(schemaChanges.isEmpty(), "Expected schema changes to be detected");
        manager.applySchemaChange(table, schemaChanges);
        return catalog.loadTable(manager.tableId());
    }

    private IcebergTableManager newManager(TableIdentifier tableId) {
        return new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));
    }

    private TableIdentifier randomTableId() {
        return TableIdentifier.of("default", "table_" + ThreadLocalRandom.current().nextInt(1000, 10000));
    }

    private InMemoryCatalog initializeCatalog() {
        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize("test", ImmutableMap.of());
        return inMemoryCatalog;
    }
}
