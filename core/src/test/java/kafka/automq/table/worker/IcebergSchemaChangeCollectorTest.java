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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class IcebergSchemaChangeCollectorTest {
    private InMemoryCatalog catalog;

    @BeforeEach
    public void setup() {
        catalog = initializeCatalog();
        catalog.createNamespace(Namespace.of("default"));
    }

    @Test
    public void shouldReturnEmptyWhenSchemasMatch() {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        List<IcebergTableManager.SchemaChange> changes = collectChanges(schema, schema);
        assertTrue(changes.isEmpty());
    }

    @Test
    public void shouldDetectTopLevelAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "name", null);
        assertEquals(Types.StringType.get(), change.getNewType());
    }

    @Test
    public void shouldDetectTopLevelOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "id", null);
    }

    @Test
    public void shouldDetectTopLevelPromotion() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE, "id", null);
        assertEquals(Types.LongType.get(), change.getNewType());
    }

    @Test
    public void shouldDetectNestedAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "email", Types.StringType.get()))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "email", "user");
    }

    @Test
    public void shouldDetectListElementStructAddition() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "street", Types.StringType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(
                    Types.NestedField.optional(3, "street", Types.StringType.get()),
                    Types.NestedField.optional(4, "zip", Types.IntegerType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "zip", "addresses.element");
    }

    @Test
    public void shouldPromoteListElementStructFieldType() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.IntegerType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.LongType.get())))));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE, "zip", "addresses.element");
        assertEquals(Types.LongType.get(), change.getNewType());
    }

    @Test
    public void shouldPromoteMapValueStructFieldType() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(Types.NestedField.optional(4, "zip", Types.IntegerType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(Types.NestedField.optional(4, "zip", Types.LongType.get())))));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE, "zip", "attributes.value");
        assertEquals(Types.LongType.get(), change.getNewType());
    }

    @Test
    public void shouldMakeListElementStructFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.required(3, "zip", Types.IntegerType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.IntegerType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "zip", "addresses.element");
    }

    @Test
    public void shouldSoftRemoveMapValueStructField() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(Types.NestedField.required(4, "zip", Types.IntegerType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of())));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "zip", "attributes.value");
    }

    @Test
    public void shouldSkipDuplicateListElementChanges() {
        Schema schema = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.LongType.get())))));

        List<IcebergTableManager.SchemaChange> changes = List.of(
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "zip", null, "addresses.element"),
            new IcebergTableManager.SchemaChange(IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE, "zip", Types.LongType.get(), "addresses.element"));

        IcebergTableManager manager = new IcebergTableManager(initializeCatalog(), TableIdentifier.of("default", "dummy"), mock(WorkerConfig.class));
        Table table = manager.getTableOrCreate(schema);
        manager.applySchemaChange(table, changes);
    }

    @Test
    public void ignoresElementTypeReplacementAsIncompatible() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2,
                Types.StructType.of(Types.NestedField.optional(3, "zip", Types.IntegerType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "addresses", Types.ListType.ofOptional(2, Types.IntegerType.get())));

        List<IcebergTableManager.SchemaChange> changes = collectChanges(initial, current);
        assertTrue(changes.isEmpty(), "Incompatible list element type change should be ignored");
    }

    @Test
    public void ignoresPrimitiveListElementPromotion() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "nums", Types.ListType.ofOptional(2, Types.IntegerType.get())));
        Schema current = new Schema(
            Types.NestedField.optional(1, "nums", Types.ListType.ofOptional(2, Types.LongType.get())));

        List<IcebergTableManager.SchemaChange> changes = collectChanges(initial, current);
        assertTrue(changes.isEmpty(), "list<int> -> list<long> promotion is unsupported and should be ignored");
    }

    @Test
    public void shouldDetectMapValueStructAddition() {
        Schema initial = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(Types.NestedField.optional(4, "city", Types.StringType.get())))));
        Schema current = new Schema(
            Types.NestedField.optional(1, "attributes", Types.MapType.ofOptional(2, 3,
                Types.StringType.get(),
                Types.StructType.of(
                    Types.NestedField.optional(4, "city", Types.StringType.get()),
                    Types.NestedField.optional(5, "country", Types.StringType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "country", "attributes.value");
    }

    @Test
    public void shouldDetectDeepNestedAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()))))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.optional(5, "age", Types.IntegerType.get()))))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "age", "user.profile");
    }

    @Test
    public void shouldDetectNestedFieldMadeOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.required(5, "age", Types.IntegerType.get()))))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.optional(5, "age", Types.IntegerType.get()))))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "age", "user.profile");
    }

    @Test
    public void shouldDetectNestedPromotion() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "age", Types.IntegerType.get()))))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "age", Types.LongType.get()))))));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE, "age", "user.profile");
        assertEquals(Types.LongType.get(), change.getNewType());
    }

    @Test
    public void shouldSoftRemoveTopLevelField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "email", Types.StringType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "email", null);
    }

    @Test
    public void shouldSoftRemoveNonPrimitiveField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "ids", Types.ListType.ofRequired(2, Types.LongType.get())));
        Schema current = new Schema();

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "ids", null);
        // ensure newType is null for non-primitive types so runtime won't call asPrimitiveType
        assertEquals(null, change.getNewType());
    }

    @Test
    public void shouldSoftRemoveNestedField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()),
                    Types.NestedField.required(5, "age", Types.IntegerType.get()))))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "profile", Types.StructType.of(
                    Types.NestedField.required(4, "name", Types.StringType.get()))))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "age", "user.profile");
    }

    @Test
    public void shouldReportMixedChanges() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "score", Types.FloatType.get()),
            Types.NestedField.optional(4, "old_field", Types.StringType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "score", Types.DoubleType.get()),
            Types.NestedField.optional(5, "new_field", Types.StringType.get()));

        List<IcebergTableManager.SchemaChange> changes = collectChanges(initial, current);
        assertEquals(4, changes.size());
        assertTrue(changes.stream().anyMatch(c -> c.getType() == IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE
            && c.getColumnName().equals("id")));
        assertTrue(changes.stream().anyMatch(c -> c.getType() == IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL
            && c.getColumnName().equals("name")));
        assertTrue(changes.stream().anyMatch(c -> c.getType() == IcebergTableManager.SchemaChange.ChangeType.PROMOTE_TYPE
            && c.getColumnName().equals("score")));
        assertTrue(changes.stream().anyMatch(c -> c.getType() == IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN
            && c.getColumnName().equals("new_field")));
    }


    @Test
    public void shouldDetectOptionalListFieldAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "tags", null);
        assertEquals(Types.ListType.ofOptional(3, Types.StringType.get()), change.getNewType());
    }

    @Test
    public void shouldDetectOptionalMapFieldAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(3, 4,
                Types.StringType.get(), Types.StringType.get())));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "metadata", null);
        assertEquals(Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.StringType.get()),
            change.getNewType());
    }

    @Test
    public void shouldDetectRequiredListFieldAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "scores", Types.ListType.ofRequired(3, Types.IntegerType.get())));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "scores", null);
        assertEquals(Types.ListType.ofRequired(3, Types.IntegerType.get()), change.getNewType());
    }

    @Test
    public void shouldDetectRequiredMapFieldAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "attributes", Types.MapType.ofRequired(3, 4,
                Types.StringType.get(), Types.IntegerType.get())));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "attributes", null);
        assertEquals(Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get()),
            change.getNewType());
    }

    @Test
    public void shouldSoftRemoveRequiredListField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "tags", null);
        // newType should be null for non-primitive types
        assertEquals(null, change.getNewType());
    }

    @Test
    public void shouldSoftRemoveRequiredMapField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metadata", Types.MapType.ofOptional(3, 4,
                Types.StringType.get(), Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        IcebergTableManager.SchemaChange change = assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "metadata", null);
        assertEquals(null, change.getNewType());
    }

    @Test
    public void shouldSoftRemoveOptionalListField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        // Optional field removal should still trigger MAKE_OPTIONAL to ensure idempotency
        List<IcebergTableManager.SchemaChange> changes = collectChanges(initial, current);
        assertTrue(changes.isEmpty() || changes.stream().allMatch(c ->
            c.getType() == IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL &&
            c.getColumnName().equals("tags")));
    }

    @Test
    public void shouldSoftRemoveOptionalMapField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(3, 4,
                Types.StringType.get(), Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

        List<IcebergTableManager.SchemaChange> changes = collectChanges(initial, current);
        assertTrue(changes.isEmpty() || changes.stream().allMatch(c ->
            c.getType() == IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL &&
            c.getColumnName().equals("metadata")));
    }

    @Test
    public void shouldMakeListFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "tags", null);
    }

    @Test
    public void shouldMakeMapFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "metadata", Types.MapType.ofOptional(3, 4,
                Types.StringType.get(), Types.StringType.get())));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "metadata", Types.MapType.ofOptional(3, 4,
                Types.StringType.get(), Types.StringType.get())));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "metadata", null);
    }

    @Test
    public void shouldDetectNestedListFieldAddition() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "hobbies", Types.ListType.ofOptional(5, Types.StringType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.ADD_COLUMN, "hobbies", "user");
    }

    @Test
    public void shouldSoftRemoveNestedListField() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "hobbies", Types.ListType.ofOptional(5, Types.StringType.get())))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "hobbies", "user");
    }

    @Test
    public void shouldMakeNestedMapFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.required(4, "preferences", Types.MapType.ofOptional(5, 6,
                    Types.StringType.get(), Types.StringType.get())))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", Types.StructType.of(
                Types.NestedField.required(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "preferences", Types.MapType.ofOptional(5, 6,
                    Types.StringType.get(), Types.StringType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "preferences", "user");
    }


    @Test
    public void shouldMakeStructFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "address", Types.StructType.of(
                Types.NestedField.required(3, "street", Types.StringType.get()),
                Types.NestedField.required(4, "city", Types.StringType.get()))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "address", Types.StructType.of(
                Types.NestedField.required(3, "street", Types.StringType.get()),
                Types.NestedField.required(4, "city", Types.StringType.get()))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "address", null);
    }

    @Test
    public void shouldMakeListNestedStructFieldOptional() {
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "contacts", Types.ListType.ofRequired(3,
                Types.StructType.of(
                    Types.NestedField.required(4, "type", Types.StringType.get()),
                    Types.NestedField.required(5, "detail", Types.StringType.get())))));
        Schema current = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "contacts", Types.ListType.ofRequired(3,
                Types.StructType.of(
                    Types.NestedField.optional(4, "type", Types.StringType.get()),
                    Types.NestedField.required(5, "detail", Types.StringType.get())))));

        assertSingleChange(
            collectChanges(initial, current), IcebergTableManager.SchemaChange.ChangeType.MAKE_OPTIONAL, "type", "contacts.element");
    }

    private List<IcebergTableManager.SchemaChange> collectChanges(Schema tableSchema, Schema currentSchema) {
        TableIdentifier tableId = TableIdentifier.of("default", generateRandomTableName());
        IcebergTableManager tableManager = new IcebergTableManager(catalog, tableId, mock(WorkerConfig.class));
        Table table = tableManager.getTableOrCreate(tableSchema);
        return tableManager.collectSchemaChanges(currentSchema, table);
    }

    private IcebergTableManager.SchemaChange assertSingleChange(List<IcebergTableManager.SchemaChange> changes,
                                                                IcebergTableManager.SchemaChange.ChangeType type,
                                                                String columnName,
                                                                String parentName) {
        assertEquals(1, changes.size(), "Expected exactly one schema change");
        IcebergTableManager.SchemaChange change = changes.get(0);
        assertEquals(type, change.getType());
        assertEquals(columnName, change.getColumnName());
        assertEquals(parentName, change.getParentName());
        return change;
    }

    private String generateRandomTableName() {
        int randomNum = ThreadLocalRandom.current().nextInt(1000, 10000);
        return "schema_table_" + randomNum;
    }

    private InMemoryCatalog initializeCatalog() {
        InMemoryCatalog inMemoryCatalog = new InMemoryCatalog();
        inMemoryCatalog.initialize("test", ImmutableMap.of());
        return inMemoryCatalog;
    }
}
