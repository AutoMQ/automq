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

import com.google.common.collect.ImmutableMap;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.CodecSetup;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AvroRecordBinderTypeTest {

    private static final String TEST_NAMESPACE = "kafka.automq.table.binder";

    private InMemoryCatalog catalog;
    private Table table;
    private TaskWriter<org.apache.iceberg.data.Record> writer;
    private int tableCounter;

    static {
        CodecSetup.setup();
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        tableCounter = 0;
    }

    // Test method for converting a single string field
    @Test
    public void testStringConversion() {
        assertFieldRoundTrips("String", "stringField",
            () -> Schema.create(Schema.Type.STRING),
            schema -> "test_string",
            value -> assertEquals("test_string", value.toString())
        );
    }

    // Test method for converting a single integer field
    @Test
    public void testIntegerConversion() {
        assertFieldRoundTrips("Int", "intField",
            () -> Schema.create(Schema.Type.INT),
            schema -> 42,
            value -> assertEquals(42, value)
        );
    }

    // Test method for converting a single long field
    @Test
    public void testLongConversion() {
        assertFieldRoundTrips("Long", "longField",
            () -> Schema.create(Schema.Type.LONG),
            schema -> 123456789L,
            value -> assertEquals(123456789L, value)
        );
    }

    // Test method for converting a single float field
    @Test
    public void testFloatConversion() {
        assertFieldRoundTrips("Float", "floatField",
            () -> Schema.create(Schema.Type.FLOAT),
            schema -> 3.14f,
            value -> assertEquals(3.14f, (Float) value)
        );
    }

    // Test method for converting a single double field
    @Test
    public void testDoubleConversion() {
        assertFieldRoundTrips("Double", "doubleField",
            () -> Schema.create(Schema.Type.DOUBLE),
            schema -> 6.28,
            value -> assertEquals(6.28, value)
        );
    }

    // Test method for converting a single boolean field
    @Test
    public void testBooleanConversion() {
        assertFieldRoundTrips("Boolean", "booleanField",
            () -> Schema.create(Schema.Type.BOOLEAN),
            schema -> true,
            value -> assertEquals(true, value)
        );
    }

    // Test method for converting a single date field (number of days from epoch)
    @Test
    public void testDateConversion() {
        LocalDate localDate = LocalDate.of(2020, 1, 1);
        int epochDays = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), localDate);
        assertFieldRoundTrips("Date", "dateField",
            () -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
            schema -> epochDays,
            value -> assertEquals(localDate, value)
        );
    }

    // Test method for converting a single time field (number of milliseconds from midnight)
    @Test
    public void testTimeConversion() {
        LocalTime localTime = LocalTime.of(10, 0);
        long epochMicros = localTime.toNanoOfDay() / 1000;
        int epochMillis = (int) (localTime.toNanoOfDay() / 1_000_000);
        assertFieldRoundTrips("TimeMicros", "timeField",
            () -> LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
            schema -> epochMicros,
            value -> assertEquals(localTime, value)
        );

        assertFieldRoundTrips("TimeMillis", "timeField2",
            () -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)),
            schema -> epochMillis,
            value -> assertEquals(localTime, value)
        );
    }

    // Test method for converting a single timestamp field (number of milliseconds from epoch)
    // timestamp: Stores microseconds from 1970-01-01 00:00:00.000000. [1]
    // timestamptz: Stores microseconds from 1970-01-01 00:00:00.000000 UTC. [1]
    @Test
    public void testTimestampConversion() {
        Instant instant = Instant.parse("2020-01-01T12:34:56.123456Z");
        long timestampMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        long timestampMillis = instant.toEpochMilli();

        Supplier<Schema> timestampMicrosTzSchema = () -> {
            Schema schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            schema.addProp("adjust-to-utc", true);
            return schema;
        };

        Supplier<Schema> timestampMicrosSchema = () -> {
            Schema schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            schema.addProp("adjust-to-utc", false);
            return schema;
        };

        Supplier<Schema> timestampMillisTzSchema = () -> {
            Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            schema.addProp("adjust-to-utc", true);
            return schema;
        };

        Supplier<Schema> timestampMillisSchema = () -> {
            Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            schema.addProp("adjust-to-utc", false);
            return schema;
        };

        OffsetDateTime expectedMicrosTz = DateTimeUtil.timestamptzFromMicros(timestampMicros);
        LocalDateTime expectedMicros = DateTimeUtil.timestampFromMicros(timestampMicros);
        OffsetDateTime expectedMillisTz = DateTimeUtil.timestamptzFromMicros(timestampMillis * 1000);
        LocalDateTime expectedMillis = DateTimeUtil.timestampFromMicros(timestampMillis * 1000);

        assertFieldRoundTrips("TimestampMicrosTz", "timestampField1",
            timestampMicrosTzSchema,
            schema -> timestampMicros,
            value -> assertEquals(expectedMicrosTz, value)
        );

        assertFieldRoundTrips("TimestampMicros", "timestampField2",
            timestampMicrosSchema,
            schema -> timestampMicros,
            value -> assertEquals(expectedMicros, value)
        );

        assertFieldRoundTrips("TimestampMillisTz", "timestampField3",
            timestampMillisTzSchema,
            schema -> timestampMillis,
            value -> assertEquals(expectedMillisTz, value)
        );

        assertFieldRoundTrips("TimestampMillis", "timestampField4",
            timestampMillisSchema,
            schema -> timestampMillis,
            value -> assertEquals(expectedMillis, value)
        );
    }

    @Test
    public void testLocalTimestampConversion() {
        LocalDateTime localDateTime = LocalDateTime.of(2023, 6, 1, 8, 15, 30, 123456000);
        long micros = DateTimeUtil.microsFromTimestamp(localDateTime);
        long millis = DateTimeUtil.microsToMillis(micros);

        // For millis precision, we need to truncate to milliseconds
        LocalDateTime localDateTimeMillis = DateTimeUtil.timestampFromMicros(millis * 1000);

        Supplier<Schema> localTimestampMillisSchema = () ->
            LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Supplier<Schema> localTimestampMicrosSchema = () ->
            LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

        assertFieldRoundTrips("LocalTimestampMillis", "localTsMillis",
            localTimestampMillisSchema,
            schema -> millis,
            value -> {
                assertEquals(millis, value);
                assertEquals(localDateTimeMillis, DateTimeUtil.timestampFromMicros(((Long) value) * 1000));
            }
        );

        assertFieldRoundTrips("LocalTimestampMicros", "localTsMicros",
            localTimestampMicrosSchema,
            schema -> micros,
            value -> {
                assertEquals(micros, value);
                assertEquals(localDateTime, DateTimeUtil.timestampFromMicros((Long) value));
            }
        );
    }

    // Test method for converting a single binary field
    @Test
    public void testBinaryConversion() {
        String randomAlphabetic = RandomStringUtils.randomAlphabetic(64);
        assertFieldRoundTrips("Binary", "binaryField",
            () -> Schema.create(Schema.Type.BYTES),
            schema -> ByteBuffer.wrap(randomAlphabetic.getBytes(StandardCharsets.UTF_8)),
            value -> {
                ByteBuffer binaryField = (ByteBuffer) value;
                assertEquals(randomAlphabetic, new String(binaryField.array(), StandardCharsets.UTF_8));
            }
        );
    }

    // Test method for converting a single fixed field
    @Test
    public void testFixedConversion() {
        assertFieldRoundTrips("Fixed", "fixedField",
            () -> Schema.createFixed("FixedField", null, null, 3),
            schema -> new GenericData.Fixed(schema, "bar".getBytes(StandardCharsets.UTF_8)),
            value -> assertEquals("bar", new String((byte[]) value, StandardCharsets.UTF_8))
        );
    }

    // Test method for converting a single enum field
    @Test
    public void testEnumConversion() {
        assertFieldRoundTrips("Enum", "enumField",
            () -> Schema.createEnum("EnumField", null, null, Arrays.asList("A", "B", "C")),
            schema -> new GenericData.EnumSymbol(schema, "B"),
            value -> assertEquals("B", value.toString())
        );
    }

    // Test method for converting a single UUID field
    @Test
    public void testUUIDConversion() {
        UUID uuid = UUID.randomUUID();
        assertFieldRoundTrips("UUID", "uuidField",
            () -> LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)),
            schema -> new Conversions.UUIDConversion().toCharSequence(uuid, schema, LogicalTypes.uuid()),
            value -> assertEquals(uuid, UUIDUtil.convert((byte[]) value))
        );
    }

    // Test method for converting a single decimal field
    @Test
    public void testDecimalConversion() {
        BigDecimal bigDecimal = BigDecimal.valueOf(1000.00).setScale(2);
        assertFieldRoundTrips("Decimal", "decimalField",
            () -> LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
            schema -> {
                LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
                return new Conversions.DecimalConversion().toBytes(bigDecimal, schema, decimalType);
            },
            value -> assertEquals(bigDecimal, value)
        );
    }

    @Test
    public void testStructFieldConversion() {
        Schema structSchema = SchemaBuilder.record("NestedStruct")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        GenericRecord expected = new GenericData.Record(structSchema);
        expected.put("field1", "nested_value");
        expected.put("field2", 99);

        assertFieldRoundTrips("StructField", "structField",
            () -> structSchema,
            schema -> cloneStruct(expected, schema),
            value -> assertStructEquals(expected, (Record) value)
        );
    }

    // Test method for converting a list field
    @Test
    public void testListConversion() {
        List<String> expected = Arrays.asList("a", "b", "c");
        assertFieldRoundTrips("List", "listField",
            () -> Schema.createArray(Schema.create(Schema.Type.STRING)),
            schema -> new ArrayList<>(expected),
            value -> assertEquals(expected, normalizeValue(value))
        );
    }

    // Test method for converting a list of structs
    @Test
    public void testListStructConversion() {
        Schema structSchema = SchemaBuilder.record("Struct")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        List<GenericRecord> expectedList = new ArrayList<>();

        GenericRecord struct1 = new GenericData.Record(structSchema);
        struct1.put("field1", "value1");
        struct1.put("field2", 1);
        expectedList.add(struct1);

        GenericRecord struct2 = new GenericData.Record(structSchema);
        struct2.put("field1", "value2");
        struct2.put("field2", 2);
        expectedList.add(struct2);

        assertFieldRoundTrips("StructList", "listField",
            () -> Schema.createArray(structSchema),
            schema -> new ArrayList<>(expectedList),
            value -> assertStructListEquals(expectedList, value)
        );
    }

    // Test method for converting a list with nullable elements
    @Test
    public void testListWithNullableElementsConversion() {
        assertFieldRoundTrips("ListNullableElements", "listField",
            () -> Schema.createArray(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)
            ))),
            schema -> {
                @SuppressWarnings("unchecked")
                GenericData.Array<Object> listValue = new GenericData.Array<>(3, schema);
                listValue.add(new Utf8("a"));
                listValue.add(null);
                listValue.add(new Utf8("c"));
                return listValue;
            },
            value -> assertEquals(Arrays.asList("a", null, "c"), normalizeValue(value))
        );
    }

    @Test
    public void testMapWithNonStringKeysConversion() {
        Map<Integer, String> expected = new LinkedHashMap<>();
        expected.put(1, "one");
        expected.put(2, "two");

        Schema logicalMapSchema = createLogicalMapSchema("IntStringEntry",
            Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));

        assertFieldRoundTrips("IntKeyLogicalMap", "mapField",
            () -> logicalMapSchema,
            schema -> createLogicalMapArrayValue(schema, expected),
            value -> {
                Map<?, ?> actual = (Map<?, ?>) value;
                Map<Integer, String> normalized = new LinkedHashMap<>();
                actual.forEach((k, v) -> normalized.put((Integer) k, v == null ? null : v.toString()));
                assertEquals(expected, normalized);
            }
        );
    }

    // Test method for converting a map with string values
    @Test
    public void testStringMapConversion() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        assertFieldRoundTrips("StringMap", "mapField",
            () -> Schema.createMap(Schema.create(Schema.Type.STRING)),
            schema -> new HashMap<>(map),
            value -> assertEquals(map, normalizeValue(value))
        );
    }

    // Test method for converting a map with integer values
    @Test
    public void testIntMapConversion() {
        Map<String, Integer> map = new HashMap<>();
        map.put("key1", 1);
        map.put("key2", 2);
        assertFieldRoundTrips("IntMap", "mapField",
            () -> Schema.createMap(Schema.create(Schema.Type.INT)),
            schema -> new HashMap<>(map),
            value -> assertEquals(map, normalizeValue(value))
        );
    }

    // Test method for converting a map with struct values
    @Test
    public void testStructMapConversion() {
        Schema structSchema = SchemaBuilder.record("Struct")
            .fields()
            .name("field1").type().stringType().noDefault()
            .name("field2").type().intType().noDefault()
            .endRecord();

        Map<String, GenericRecord> map = new HashMap<>();
        GenericRecord struct1 = new GenericData.Record(structSchema);
        struct1.put("field1", "value1");
        struct1.put("field2", 1);
        map.put("key1", struct1);

        GenericRecord struct2 = new GenericData.Record(structSchema);
        struct2.put("field1", "value2");
        struct2.put("field2", 2);
        map.put("key2", struct2);

        assertFieldRoundTrips("StructMap", "mapField",
            () -> Schema.createMap(structSchema),
            schema -> new HashMap<>(map),
            value -> assertStructMapEquals(map, value)
        );
    }

    // Test method for converting a map with nullable values
    @Test
    public void testMapWithNullableValuesConversion() {
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", null);

        assertFieldRoundTrips("NullableValueMap", "mapField",
            () -> Schema.createMap(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)
            ))),
            schema -> new HashMap<>(expectedMap),
            value -> assertEquals(expectedMap, normalizeValue(value))
        );
    }


    @Test
    public void testBinaryFieldBackedByFixedConversion() {
        Schema fixedSchema = Schema.createFixed("FixedBinary", null, null, 4);
        Schema recordSchema = SchemaBuilder.builder()
            .record("FixedBinaryRecord")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("binaryField").type(fixedSchema).noDefault()
            .endRecord();

        Types.StructType structType = Types.StructType.of(
            Types.NestedField.required(1, "binaryField", Types.BinaryType.get())
        );
        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(structType.fields());

        runRoundTrip(recordSchema, icebergSchema,
            record -> record.put("binaryField", new GenericData.Fixed(fixedSchema, new byte[]{1, 2, 3, 4})),
            icebergRecord -> {
                ByteBuffer buffer = (ByteBuffer) icebergRecord.getField("binaryField");
                byte[] actual = new byte[buffer.remaining()];
                buffer.get(actual);
                assertArrayEquals(new byte[]{1, 2, 3, 4}, actual);
            }
        );
    }

    // Test method for deeply nested struct (3+ levels)
    @Test
    public void testDeeplyNestedStructConversion() {
        Schema innerMostStruct = SchemaBuilder.record("InnerMostStruct")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("deepValue").type().intType().noDefault()
            .endRecord();

        Schema middleStruct = SchemaBuilder.record("MiddleStruct")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("middleField").type().stringType().noDefault()
            .name("innerMost").type(innerMostStruct).noDefault()
            .endRecord();

        Schema outerStruct = SchemaBuilder.record("OuterStruct")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("outerField").type().stringType().noDefault()
            .name("middle").type(middleStruct).noDefault()
            .endRecord();

        Schema recordSchema = SchemaBuilder.builder()
            .record("DeeplyNestedRecord")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("topLevel").type().stringType().noDefault()
            .name("nested").type(outerStruct).noDefault()
            .endRecord();

        GenericRecord innerMostRecord = new GenericData.Record(innerMostStruct);
        innerMostRecord.put("deepValue", 42);

        GenericRecord middleRecord = new GenericData.Record(middleStruct);
        middleRecord.put("middleField", "middle");
        middleRecord.put("innerMost", innerMostRecord);

        GenericRecord outerRecord = new GenericData.Record(outerStruct);
        outerRecord.put("outerField", "outer");
        outerRecord.put("middle", middleRecord);

        runRoundTrip(recordSchema,
            record -> {
                record.put("topLevel", "top");
                record.put("nested", outerRecord);
            },
            icebergRecord -> {
                assertEquals("top", icebergRecord.getField("topLevel").toString());
                Record nestedRecord = (Record) icebergRecord.getField("nested");
                assertNotNull(nestedRecord);
                assertEquals("outer", nestedRecord.getField("outerField").toString());

                Record middleResult = (Record) nestedRecord.getField("middle");
                assertNotNull(middleResult);
                assertEquals("middle", middleResult.getField("middleField").toString());

                Record innerMostResult = (Record) middleResult.getField("innerMost");
                assertNotNull(innerMostResult);
                assertEquals(42, innerMostResult.getField("deepValue"));
            }
        );
    }

    // Test method for converting a record with default values
    @Test
    public void testDefaultFieldConversion() {
        Schema recordSchema = SchemaBuilder.builder()
            .record("DefaultValueRecord")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("defaultStringField").type().stringType().stringDefault("default_string")
            .name("defaultIntField").type().intType().intDefault(42)
            .name("defaultBoolField").type().booleanType().booleanDefault(true)
            .endRecord();

        // Test with default values
        runRoundTrip(recordSchema,
            record -> {
                Schema.Field defaultStringField = recordSchema.getField("defaultStringField");
                Schema.Field defaultIntField = recordSchema.getField("defaultIntField");
                Schema.Field defaultBoolField = recordSchema.getField("defaultBoolField");
                record.put("defaultStringField", defaultStringField.defaultVal());
                record.put("defaultIntField", defaultIntField.defaultVal());
                record.put("defaultBoolField", defaultBoolField.defaultVal());
            },
            icebergRecord -> {
                assertEquals("default_string", icebergRecord.getField("defaultStringField").toString());
                assertEquals(42, icebergRecord.getField("defaultIntField"));
                assertEquals(true, icebergRecord.getField("defaultBoolField"));
            }
        );

        // Test with non-default values
        runRoundTrip(recordSchema,
            record -> {
                record.put("defaultStringField", "custom_value");
                record.put("defaultIntField", 100);
                record.put("defaultBoolField", false);
            },
            icebergRecord -> {
                assertEquals("custom_value", icebergRecord.getField("defaultStringField").toString());
                assertEquals(100, icebergRecord.getField("defaultIntField"));
                assertEquals(false, icebergRecord.getField("defaultBoolField"));
            }
        );
    }

    // Test that non-optional unions with multiple non-NULL types throw UnsupportedOperationException
    @Test
    public void testNonOptionalUnionThrowsException() {
        // Test case 1: {null, string, int} at record level
        Schema unionSchema1 = Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING),
            Schema.create(Schema.Type.INT)
        ));

        try {
            RecordBinder binder = new RecordBinder(AvroSchemaUtil.toIceberg(unionSchema1), unionSchema1);
            org.junit.jupiter.api.Assertions.fail("Expected UnsupportedOperationException for non-optional union {null, string, int}");
        } catch (UnsupportedOperationException e) {
            assertEquals(true, e.getMessage().contains("Non-optional UNION with multiple non-NULL types is not supported"));
            assertEquals(true, e.getMessage().contains("Found 2 non-NULL types"));
        }

        // Test case 2: {null, struct1, struct2} at record level
        Schema struct1Schema = SchemaBuilder.record("Struct1")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("field1").type().stringType().noDefault()
            .endRecord();

        Schema struct2Schema = SchemaBuilder.record("Struct2")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("field2").type().intType().noDefault()
            .endRecord();

        Schema unionSchema2 = Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.NULL),
            struct1Schema,
            struct2Schema
        ));

        try {
            RecordBinder binder = new RecordBinder(AvroSchemaUtil.toIceberg(unionSchema2), unionSchema2);
            org.junit.jupiter.api.Assertions.fail("Expected UnsupportedOperationException for non-optional union {null, struct1, struct2}");
        } catch (UnsupportedOperationException e) {
            assertEquals(true, e.getMessage().contains("Non-optional UNION with multiple non-NULL types is not supported"));
            assertEquals(true, e.getMessage().contains("Found 2 non-NULL types"));
        }

        // Test case 3: Union in field with multiple non-NULL types
        Schema unionFieldSchema = Schema.createUnion(Arrays.asList(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING),
            Schema.create(Schema.Type.INT)
        ));

        Schema recordSchema = SchemaBuilder.builder()
            .record("RecordWithUnionField")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name("id").type().intType().noDefault()
            .name("unionField").type(unionFieldSchema).withDefault(null)
            .endRecord();

        try {
            RecordBinder binder = new RecordBinder(AvroSchemaUtil.toIceberg(recordSchema), recordSchema);
            org.junit.jupiter.api.Assertions.fail("Expected UnsupportedOperationException for field with non-optional union");
        } catch (UnsupportedOperationException e) {
            assertEquals(true, e.getMessage().contains("Non-optional UNION with multiple non-NULL types is not supported"));
            assertEquals(true, e.getMessage().contains("Found 2 non-NULL types"));
        }
    }


    private void testSendRecord(org.apache.iceberg.Schema schema, org.apache.iceberg.data.Record record) {
        String tableName = "test_" + tableCounter++;
        table = catalog.createTable(TableIdentifier.of(Namespace.of("default"), tableName), schema);
        writer = createTableWriter(table);
        try {
            writer.write(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static TaskWriter<org.apache.iceberg.data.Record> createTableWriter(Table table) {
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            null, null, null)
            .setAll(new HashMap<>(table.properties()))
            .set(PARQUET_ROW_GROUP_SIZE_BYTES, "1");

        OutputFileFactory fileFactory =
            OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(FileFormat.PARQUET)
                .build();

        return new UnpartitionedWriter<>(
            table.spec(),
            FileFormat.PARQUET,
            appenderFactory,
            fileFactory,
            table.io(),
            WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );
    }

    private static GenericRecord serializeAndDeserialize(GenericRecord record, Schema schema) {
        try {
            // Serialize the avro record to a byte array
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(record, encoder);
            encoder.flush();
            outputStream.close();

            byte[] serializedBytes = outputStream.toByteArray();

            // Deserialize the byte array back to an avro record
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedBytes);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static Schema createOptionalSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            boolean hasNull = schema.getTypes().stream()
                .anyMatch(type -> type.getType() == Schema.Type.NULL);
            if (hasNull) {
                return schema;
            }
            List<Schema> updatedTypes = new ArrayList<>();
            updatedTypes.add(Schema.create(Schema.Type.NULL));
            updatedTypes.addAll(schema.getTypes());
            return Schema.createUnion(updatedTypes);
        }
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

    private static Schema ensureNonNullBranch(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
            .filter(type -> type.getType() != Schema.Type.NULL)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Union schema lacks non-null branch: " + schema));
    }

    private void runRoundTrip(Schema recordSchema, Consumer<GenericRecord> avroPopulator, Consumer<Record> assertions) {
        runRoundTrip(recordSchema, AvroSchemaUtil.toIceberg(recordSchema), avroPopulator, assertions);
    }

    private void runRoundTrip(Schema recordSchema,
                              org.apache.iceberg.Schema icebergSchema,
                              Consumer<GenericRecord> avroPopulator,
                              Consumer<Record> assertions) {
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroPopulator.accept(avroRecord);
        GenericRecord roundTripRecord = serializeAndDeserialize(avroRecord, recordSchema);

        Record icebergRecord = new RecordBinder(icebergSchema, recordSchema).bind(roundTripRecord);

        assertions.accept(icebergRecord);
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Helper method to test round-trip conversion for a single field
    private void assertFieldRoundTrips(String recordPrefix,
                                       String fieldName,
                                       Supplier<Schema> fieldSchemaSupplier,
                                       Function<Schema, Object> avroValueSupplier,
                                       Consumer<Object> valueAssertion) {
        Schema baseFieldSchema = fieldSchemaSupplier.get();
        Schema baseRecordSchema = SchemaBuilder.builder()
            .record(recordPrefix + "Base")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name(fieldName).type(baseFieldSchema).noDefault()
            .endRecord();

        // Direct field
        runRoundTrip(baseRecordSchema,
            record -> record.put(fieldName, avroValueSupplier.apply(baseFieldSchema)),
            icebergRecord -> valueAssertion.accept(icebergRecord.getField(fieldName))
        );

        Schema optionalFieldSchema = createOptionalSchema(fieldSchemaSupplier.get());
        Schema unionRecordSchema = SchemaBuilder.builder()
            .record(recordPrefix + "Union")
            .namespace(TEST_NAMESPACE)
            .fields()
            .name(fieldName).type(optionalFieldSchema).withDefault(null)
            .endRecord();
        Schema nonNullBranch = ensureNonNullBranch(optionalFieldSchema);

        // Optional field with non-null value
        runRoundTrip(unionRecordSchema,
            record -> record.put(fieldName, avroValueSupplier.apply(nonNullBranch)),
            icebergRecord -> valueAssertion.accept(icebergRecord.getField(fieldName))
        );

        // Optional field with null value
        runRoundTrip(unionRecordSchema,
            record -> record.put(fieldName, null),
            icebergRecord -> assertNull(icebergRecord.getField(fieldName))
        );
    }


    private static Map<String, Object> toStringKeyMap(Object value) {
        if (value == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        Map<String, Object> result = new HashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey() == null ? null : entry.getKey().toString();
            result.put(key, normalizeValue(entry.getValue()));
        }
        return result;
    }

    private static GenericRecord cloneStruct(GenericRecord source, Schema schema) {
        GenericRecord target = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            target.put(field.name(), source.get(field.name()));
        }
        return target;
    }

    private static Schema createLogicalMapSchema(String entryName, Schema keySchema, Schema valueSchema) {
        Schema.Field keyField = new Schema.Field("key", keySchema, null, null);
        Schema.Field valueField = new Schema.Field("value", valueSchema, null, null);
        Schema entrySchema = Schema.createRecord(entryName, null, null, false);
        entrySchema.setFields(Arrays.asList(keyField, valueField));
        Schema arraySchema = Schema.createArray(entrySchema);
        return CodecSetup.getLogicalMap().addToSchema(arraySchema);
    }

    private static GenericData.Array<GenericRecord> createLogicalMapArrayValue(Schema schema, Map<?, ?> values) {
        Schema nonNullSchema = ensureNonNullBranch(schema);
        if (nonNullSchema.getType() != Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Expected array schema for logical map but got: " + nonNullSchema);
        }
        Schema entrySchema = nonNullSchema.getElementType();
        Schema.Field keyField = entrySchema.getField("key");
        Schema.Field valueField = entrySchema.getField("value");
        GenericData.Array<GenericRecord> entries = new GenericData.Array<>(values.size(), nonNullSchema);
        for (Map.Entry<?, ?> entry : values.entrySet()) {
            GenericRecord kv = new GenericData.Record(entrySchema);
            kv.put(keyField.name(), toAvroValue(entry.getKey(), keyField.schema()));
            kv.put(valueField.name(), toAvroValue(entry.getValue(), valueField.schema()));
            entries.add(kv);
        }
        return entries;
    }

    private static Object toAvroValue(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        Schema actualSchema = ensureNonNullBranch(schema);
        switch (actualSchema.getType()) {
            case STRING:
                return value instanceof CharSequence ? value : new Utf8(value.toString());
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return value;
            case RECORD:
                return value;
            default:
                return value;
        }
    }

    private static List<Record> toRecordList(Object value) {
        if (value == null) {
            return null;
        }
        List<?> list = (List<?>) value;
        List<Record> normalized = new ArrayList<>(list.size());
        for (Object element : list) {
            normalized.add((Record) element);
        }
        return normalized;
    }

    private static Map<String, Record> toRecordMap(Object value) {
        if (value == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        Map<String, Record> normalized = new HashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey() == null ? null : entry.getKey().toString();
            normalized.put(key, (Record) entry.getValue());
        }
        return normalized;
    }

    private static void assertStructListEquals(List<GenericRecord> expectedList, Object actualValue) {
        List<Record> actualList = toRecordList(actualValue);
        assertNotNull(actualList, "Actual list is null");
        assertEquals(expectedList.size(), actualList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertStructEquals(expectedList.get(i), actualList.get(i));
        }
    }

    private static void assertStructMapEquals(Map<String, GenericRecord> expectedMap, Object actualValue) {
        Map<String, Record> actualMap = toRecordMap(actualValue);
        assertNotNull(actualMap, "Actual map is null");
        assertEquals(expectedMap.keySet(), actualMap.keySet());
        for (Map.Entry<String, GenericRecord> entry : expectedMap.entrySet()) {
            assertStructEquals(entry.getValue(), actualMap.get(entry.getKey()));
        }
    }

    private static void assertStructEquals(GenericRecord expected, Record actual) {
        assertNotNull(actual, "Actual struct record is null");
        for (Schema.Field field : expected.getSchema().getFields()) {
            Object expectedValue = normalizeValue(expected.get(field.name()));
            Object actualValue = normalizeValue(actual.getField(field.name()));
            assertEquals(expectedValue, actualValue, "Mismatch on field " + field.name());
        }
    }

    private static Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            List<Object> normalized = new ArrayList<>(list.size());
            for (Object element : list) {
                normalized.add(normalizeValue(element));
            }
            return normalized;
        }
        if (value instanceof Map<?, ?>) {
            return toStringKeyMap(value);
        }
        return value;
    }

}
