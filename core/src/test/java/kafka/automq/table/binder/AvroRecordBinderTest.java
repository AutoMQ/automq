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

package kafka.automq.table.binder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.CodecSetup;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class AvroRecordBinderTest {

    private static final String TEST_NAMESPACE = "kafka.automq.table.binder";

    static {
        CodecSetup.setup();
    }

    /**
     * Tests that when the same Schema instance is used in multiple places (direct field and list element),
     * the RecordBinder correctly shares the same binder for that schema instance.
     * This verifies the IdentityHashMap optimization.
     */
    @Test
    public void testStructSchemaInstanceReuseSharesBinder() {
        Schema sharedStruct = Schema.createRecord("SharedStruct", null, TEST_NAMESPACE, false);
        sharedStruct.setFields(Arrays.asList(
            new Schema.Field("value", Schema.create(Schema.Type.LONG), null, null)
        ));

        Schema listSchema = Schema.createArray(sharedStruct);

        Schema parent = Schema.createRecord("SharedStructReuseRoot", null, TEST_NAMESPACE, false);
        parent.setFields(Arrays.asList(
            new Schema.Field("directField", sharedStruct, null, null),
            new Schema.Field("listField", listSchema, null, null)
        ));

        GenericRecord directValue = new GenericData.Record(sharedStruct);
        directValue.put("value", 1L);

        @SuppressWarnings("unchecked")
        GenericData.Array<GenericRecord> listValue = new GenericData.Array<>(2, listSchema);
        GenericRecord listEntry1 = new GenericData.Record(sharedStruct);
        listEntry1.put("value", 2L);
        listValue.add(listEntry1);
        GenericRecord listEntry2 = new GenericData.Record(sharedStruct);
        listEntry2.put("value", 3L);
        listValue.add(listEntry2);

        GenericRecord parentRecord = new GenericData.Record(parent);
        parentRecord.put("directField", directValue);
        parentRecord.put("listField", listValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(parent);
        Record icebergRecord = new RecordBinder(icebergSchema, parent).bind(parentRecord);

        Record directRecord = (Record) icebergRecord.getField("directField");
        assertEquals(1L, directRecord.getField("value"));

        @SuppressWarnings("unchecked")
        List<Record> boundList = (List<Record>) icebergRecord.getField("listField");
        assertEquals(2, boundList.size());
        assertEquals(2L, boundList.get(0).getField("value"));
        assertEquals(3L, boundList.get(1).getField("value"));
    }

    /**
     * Tests that structs with the same full name but different schemas in different contexts
     * (direct field vs list element) are handled correctly using IdentityHashMap.
     * This ensures schema identity, not name equality, is used for binder lookup.
     */
    @Test
    public void testStructBindersHandleDuplicateFullNames() {
        Schema directStruct = Schema.createRecord("DuplicatedStruct", null, TEST_NAMESPACE, false);
        directStruct.setFields(Arrays.asList(
            new Schema.Field("directOnly", Schema.create(Schema.Type.STRING), null, null)
        ));

        Schema listStruct = Schema.createRecord("DuplicatedStruct", null, TEST_NAMESPACE, false);
        listStruct.setFields(Arrays.asList(
            new Schema.Field("listOnly", Schema.create(Schema.Type.INT), null, null)
        ));

        Schema listSchema = Schema.createArray(listStruct);

        Schema parent = Schema.createRecord("StructCollisionRoot", null, TEST_NAMESPACE, false);
        parent.setFields(Arrays.asList(
            new Schema.Field("directField", directStruct, null, null),
            new Schema.Field("listField", listSchema, null, null)
        ));

        GenericRecord parentRecord = new GenericData.Record(parent);
        GenericRecord directRecord = new GenericData.Record(directStruct);
        directRecord.put("directOnly", new Utf8("direct"));
        parentRecord.put("directField", directRecord);

        @SuppressWarnings("unchecked")
        GenericData.Array<GenericRecord> listValue = new GenericData.Array<>(1, listSchema);
        GenericRecord listRecord = new GenericData.Record(listStruct);
        listRecord.put("listOnly", 42);
        listValue.add(listRecord);
        parentRecord.put("listField", listValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(parent);
        Record icebergRecord = new RecordBinder(icebergSchema, parent).bind(parentRecord);

        Record directField = (Record) icebergRecord.getField("directField");
        assertEquals("direct", directField.getField("directOnly").toString());

        @SuppressWarnings("unchecked")
        List<Record> boundList = (List<Record>) icebergRecord.getField("listField");
        assertEquals(1, boundList.size());
        assertEquals(42, boundList.get(0).getField("listOnly"));
    }

    /**
     * Tests duplicate struct names in map values context.
     * Verifies IdentityHashMap correctly distinguishes between schemas with same name.
     */
    @Test
    public void testStructBindersHandleDuplicateFullNamesInMapValues() {
        Schema directStruct = Schema.createRecord("DuplicatedStruct", null, TEST_NAMESPACE, false);
        directStruct.setFields(Arrays.asList(
            new Schema.Field("directOnly", Schema.create(Schema.Type.STRING), null, null)
        ));

        Schema mapStruct = Schema.createRecord("DuplicatedStruct", null, TEST_NAMESPACE, false);
        mapStruct.setFields(Arrays.asList(
            new Schema.Field("mapOnly", Schema.create(Schema.Type.LONG), null, null)
        ));

        Schema mapSchema = Schema.createMap(mapStruct);

        Schema parent = Schema.createRecord("StructCollisionMapRoot", null, TEST_NAMESPACE, false);
        parent.setFields(Arrays.asList(
            new Schema.Field("directField", directStruct, null, null),
            new Schema.Field("mapField", mapSchema, null, null)
        ));

        GenericRecord parentRecord = new GenericData.Record(parent);
        GenericRecord directRecord = new GenericData.Record(directStruct);
        directRecord.put("directOnly", new Utf8("direct"));
        parentRecord.put("directField", directRecord);

        Map<String, GenericRecord> mapValue = new HashMap<>();
        GenericRecord mapEntry = new GenericData.Record(mapStruct);
        mapEntry.put("mapOnly", 123L);
        mapValue.put("key", mapEntry);
        parentRecord.put("mapField", mapValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(parent);
        Record icebergRecord = new RecordBinder(icebergSchema, parent).bind(parentRecord);

        Record directField = (Record) icebergRecord.getField("directField");
        assertEquals("direct", directField.getField("directOnly").toString());

        @SuppressWarnings("unchecked")
        Map<CharSequence, Record> boundMap = (Map<CharSequence, Record>) icebergRecord.getField("mapField");
        assertEquals(1, boundMap.size());
        assertEquals(123L, boundMap.get("key").getField("mapOnly"));
    }

    /**
     * Tests that AvroValueAdapter throws IllegalStateException when trying to convert
     * a struct with missing fields in the source Avro schema.
     */
    @Test
    public void testConvertStructThrowsWhenSourceFieldMissing() {
        Schema nestedSchema = Schema.createRecord("NestedRecord", null, TEST_NAMESPACE, false);
        nestedSchema.setFields(Arrays.asList(
            new Schema.Field("presentField", Schema.create(Schema.Type.STRING), null, null)
        ));

        GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("presentField", new Utf8("value"));

        Types.StructType icebergStruct = Types.StructType.of(
            Types.NestedField.optional(2, "presentField", Types.StringType.get()),
            Types.NestedField.optional(3, "missingField", Types.StringType.get())
        );

        AvroValueAdapter adapter = new AvroValueAdapter();
        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> adapter.convert(nestedRecord, nestedSchema, icebergStruct));
        assertTrue(exception.getMessage().contains("missingField"));
        assertTrue(exception.getMessage().contains("NestedRecord"));
    }

    /**
     * Tests field count statistics for various field types and sizes.
     * Verifies that small/large strings, binary fields, and primitives are counted correctly.
     */
    @Test
    public void testFieldCountStatistics() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"TestRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"smallString\", \"type\": \"string\"},\n" +
            "    {\"name\": \"largeString\", \"type\": \"string\"},\n" +
            "    {\"name\": \"intField\", \"type\": \"int\"},\n" +
            "    {\"name\": \"binaryField\", \"type\": \"bytes\"},\n" +
            "    {\"name\": \"optionalStringField\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("smallString", "small"); // 5 chars = 3 field
        avroRecord.put("largeString", "a".repeat(50)); // 50 chars = 3 + 50/32 = 4
        avroRecord.put("intField", 42); // primitive = 1 field
        avroRecord.put("binaryField", ByteBuffer.wrap("test".repeat(10).getBytes())); // 5
        avroRecord.put("optionalStringField", "optional");

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access all fields to trigger counting
        assertEquals("small", icebergRecord.getField("smallString"));
        assertEquals("a".repeat(50), icebergRecord.getField("largeString"));
        assertEquals(42, icebergRecord.getField("intField"));
        assertEquals("test".repeat(10), new String(((ByteBuffer) icebergRecord.getField("binaryField")).array()));
        assertEquals("optional", icebergRecord.getField("optionalStringField").toString());

        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(16, fieldCount);

        // Second call should return 0 (reset)
        assertEquals(0, recordBinder.getAndResetFieldCount());
    }

    /**
     * Tests field counting for complex types (LIST and MAP).
     * Verifies that list and map elements are counted correctly.
     */
    @Test
    public void testFieldCountWithComplexTypes() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ComplexRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"stringList\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
            "    {\"name\": \"stringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("stringList", Arrays.asList("a", "b", "c"));

        Map<String, String> map = new HashMap<>();
        map.put("key1", "val1");
        map.put("key2", "val2");
        avroRecord.put("stringMap", map);

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access fields to trigger counting
        icebergRecord.getField("stringList");
        icebergRecord.getField("stringMap");

        // Total: 10 (list) + 13 (map) = 23 fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(23, fieldCount);
    }

    /**
     * Tests field counting for nested struct fields.
     * Verifies that nested struct fields contribute to the count correctly.
     */
    @Test
    public void testFieldCountWithNestedStructure() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"NestedRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"simpleField\", \"type\": \"string\"},\n" +
            "    {\n" +
            "      \"name\": \"nestedField\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"Nested\",\n" +
            "        \"fields\": [\n" +
            "          {\"name\": \"nestedString\", \"type\": \"string\"},\n" +
            "          {\"name\": \"nestedInt\", \"type\": \"int\"}\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        GenericRecord nestedRecord = new GenericData.Record(avroSchema.getField("nestedField").schema());
        nestedRecord.put("nestedString", "nested");
        nestedRecord.put("nestedInt", 123);

        GenericRecord mainRecord = new GenericData.Record(avroSchema);
        mainRecord.put("simpleField", "simple");
        mainRecord.put("nestedField", nestedRecord);

        Record icebergRecord = recordBinder.bind(mainRecord);

        // Access all fields including nested ones
        assertEquals("simple", icebergRecord.getField("simpleField"));
        Record nested = (Record) icebergRecord.getField("nestedField");
        assertEquals("nested", nested.getField("nestedString"));
        assertEquals(123, nested.getField("nestedInt"));

        // Total: 3 (simple) + 1(struct) + 3 (nested string) + 1 (nested int) = 8 fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(8, fieldCount);
    }

    /**
     * Tests that field counts accumulate across multiple record bindings.
     * Verifies batch processing statistics.
     */
    @Test
    public void testFieldCountBatchAccumulation() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"SimpleRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"stringField\", \"type\": \"string\"},\n" +
            "    {\"name\": \"intField\", \"type\": \"int\"}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        // Process multiple records
        for (int i = 0; i < 3; i++) {
            GenericRecord avroRecord = new GenericData.Record(avroSchema);
            avroRecord.put("stringField", "test" + i);
            avroRecord.put("intField", i);

            Record icebergRecord = recordBinder.bind(avroRecord);
            // Access fields to trigger counting
            icebergRecord.getField("stringField");
            icebergRecord.getField("intField");
        }

        // Total: 3 records * 4 fields each = 12 fields
        long totalFieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(12, totalFieldCount);
    }

    /**
     * Tests that null values don't contribute to field count.
     */
    @Test
    public void testFieldCountWithNullValues() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"NullableRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"nonNullField\", \"type\": \"string\"},\n" +
            "    {\"name\": \"nullField\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nonNullField", "value");
        avroRecord.put("nullField", null);

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access both fields
        assertEquals("value", icebergRecord.getField("nonNullField"));
        assertNull(icebergRecord.getField("nullField"));

        // Only the non-null field should count
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(3, fieldCount);
    }

    /**
     * Tests field counting for optional union fields with both null and non-null values.
     */
    @Test
    public void testFieldCountWithUnionFields() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"UnionCountRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"optionalString\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        // Test with non-null value
        GenericRecord nonNullRecord = new GenericData.Record(avroSchema);
        nonNullRecord.put("optionalString", "value");

        Record icebergRecord = recordBinder.bind(nonNullRecord);
        assertEquals("value", icebergRecord.getField("optionalString").toString());
        assertEquals(3, recordBinder.getAndResetFieldCount());

        // Test with null value
        GenericRecord nullRecord = new GenericData.Record(avroSchema);
        nullRecord.put("optionalString", null);

        Record nullIcebergRecord = recordBinder.bind(nullRecord);
        assertNull(nullIcebergRecord.getField("optionalString"));
        assertEquals(0, recordBinder.getAndResetFieldCount());
    }

    /**
     * Tests that binding a null GenericRecord returns null.
     */
    @Test
    public void testBindNullRecordReturnsNull() {
        Schema avroSchema = Schema.createRecord("TestRecord", null, TEST_NAMESPACE, false);
        avroSchema.setFields(Arrays.asList(
            new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)
        ));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        Record result = recordBinder.bind(null);
        assertNull(result);
    }

    /**
     * Tests that accessing a field with negative position throws IndexOutOfBoundsException.
     */
    @Test
    public void testGetFieldWithNegativePositionThrowsException() {
        Schema avroSchema = Schema.createRecord("TestRecord", null, TEST_NAMESPACE, false);
        avroSchema.setFields(Arrays.asList(
            new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)
        ));

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("field", new Utf8("value"));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);
        Record icebergRecord = recordBinder.bind(avroRecord);

        IndexOutOfBoundsException exception = assertThrows(IndexOutOfBoundsException.class,
            () -> icebergRecord.get(-1));
        assertTrue(exception.getMessage().contains("out of bounds"));
    }

    /**
     * Tests that accessing a field with position >= size throws IndexOutOfBoundsException.
     */
    @Test
    public void testGetFieldWithExcessivePositionThrowsException() {
        Schema avroSchema = Schema.createRecord("TestRecord", null, TEST_NAMESPACE, false);
        avroSchema.setFields(Arrays.asList(
            new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)
        ));

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("field", new Utf8("value"));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);
        Record icebergRecord = recordBinder.bind(avroRecord);

        IndexOutOfBoundsException exception = assertThrows(IndexOutOfBoundsException.class,
            () -> icebergRecord.get(999));
        assertTrue(exception.getMessage().contains("out of bounds"));
    }

    /**
     * Tests that accessing a field by an unknown name returns null.
     */
    @Test
    public void testGetFieldByUnknownNameReturnsNull() {
        Schema avroSchema = Schema.createRecord("TestRecord", null, TEST_NAMESPACE, false);
        avroSchema.setFields(Arrays.asList(
            new Schema.Field("existingField", Schema.create(Schema.Type.STRING), null, null)
        ));

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("existingField", new Utf8("value"));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);
        Record icebergRecord = recordBinder.bind(avroRecord);

        assertNull(icebergRecord.getField("nonExistentField"));
    }

    /**
     * Tests that a UNION containing only NULL type throws IllegalArgumentException.
     */
    @Test
    public void testUnionWithOnlyNullThrowsException() {
        Schema nullOnlyUnion = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL)));

        Schema avroSchema = Schema.createRecord("TestRecord", null, TEST_NAMESPACE, false);
        avroSchema.setFields(Arrays.asList(
            new Schema.Field("nullField", nullOnlyUnion, null, null)
        ));

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new RecordBinder(icebergSchema, avroSchema));
        assertTrue(exception.getMessage().contains("UNION schema contains only NULL type"));
    }

    /**
     * Tests that null elements in Map-as-Array representation are skipped.
     */
    @Test
    public void testMapAsArrayWithNullElementsSkipped() {
        String avroSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"MapAsArrayRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"mapField\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"logicalType\": \"map\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"MapEntry\",\n" +
            "          \"fields\": [\n" +
            "            {\"name\": \"key\", \"type\": \"string\"},\n" +
            "            {\"name\": \"value\", \"type\": \"int\"}\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        Schema entrySchema = avroSchema.getField("mapField").schema().getElementType();

        @SuppressWarnings("unchecked")
        GenericData.Array<GenericRecord> arrayValue = new GenericData.Array<>(3, avroSchema.getField("mapField").schema());

        // Add valid entry
        GenericRecord entry1 = new GenericData.Record(entrySchema);
        entry1.put("key", new Utf8("key1"));
        entry1.put("value", 100);
        arrayValue.add(entry1);

        // Add null entry (should be skipped)
        arrayValue.add(null);

        // Add another valid entry
        GenericRecord entry2 = new GenericData.Record(entrySchema);
        entry2.put("key", new Utf8("key2"));
        entry2.put("value", 200);
        arrayValue.add(entry2);

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("mapField", arrayValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);
        Record icebergRecord = recordBinder.bind(avroRecord);

        @SuppressWarnings("unchecked")
        Map<CharSequence, Integer> mapField = (Map<CharSequence, Integer>) icebergRecord.getField("mapField");

        // Should only contain 2 entries (null entry skipped)
        assertEquals(2, mapField.size());
        assertEquals(100, mapField.get(new Utf8("key1")));
        assertEquals(200, mapField.get(new Utf8("key2")));
    }
}
