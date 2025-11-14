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
import org.junit.jupiter.api.Tag;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class AvroRecordBinderTest {

    private static final String TEST_NAMESPACE = "kafka.automq.table.binder";

    private static Schema avroSchema;
    private InMemoryCatalog catalog;
    private Table table;
    private TaskWriter<Record> writer;
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

    private void testSendRecord(org.apache.iceberg.Schema schema, Record record) {
        String tableName = "test_" + tableCounter++;
        table = catalog.createTable(TableIdentifier.of(Namespace.of("default"), tableName), schema);
        writer = createTableWriter(table);
        try {
            writer.write(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TaskWriter<Record> createTableWriter(Table table) {
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

    private static <K> Map<K, Object> normalizeMapValues(Object value) {
        if (value == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        Map<K, Object> result = new HashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            @SuppressWarnings("unchecked")
            K key = (K) entry.getKey();
            result.put(key, normalizeValue(entry.getValue()));
        }
        return result;
    }

    private static Schema createOptionalSchema(Schema nonNullSchema) {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), nonNullSchema));
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
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroPopulator.accept(avroRecord);
        GenericRecord roundTripRecord = serializeAndDeserialize(avroRecord, recordSchema);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(recordSchema);
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

    @Test
    public void testSchemaEvolution() {
        // Original Avro schema with 3 fields
        String originalAvroSchemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"long\"},"
            + "  {\"name\": \"name\", \"type\": \"string\"},"
            + "  {\"name\": \"email\", \"type\": \"string\"}"
            + "]}";

        // Evolved Iceberg schema: added age field, removed email field
        org.apache.iceberg.Schema evolvedIcebergSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(4, "age", Types.IntegerType.get()) // New field
            // email field removed
        );

        Schema avroSchema = new Schema.Parser().parse(originalAvroSchemaJson);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", 12345L);
        avroRecord.put("name", new Utf8("John Doe"));
        avroRecord.put("email", new Utf8("john@example.com"));

        // Test wrapper with evolved schema
        RecordBinder recordBinder = new RecordBinder(evolvedIcebergSchema, avroSchema);
        Record bind = recordBinder.bind(avroRecord);

        assertEquals(12345L, bind.get(0)); // id
        assertEquals("John Doe", bind.get(1).toString()); // name
        assertNull(bind.get(2)); // age - doesn't exist in Avro record
    }


    @Test
    public void testWrapperReusability() {
        // Test that the same wrapper can be reused for multiple records
        String avroSchemaJson = "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"long\"},"
            + "  {\"name\": \"name\", \"type\": \"string\"}"
            + "]}";
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get())
        );

        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);


        // First record
        GenericRecord record1 = new GenericData.Record(avroSchema);
        record1.put("id", 1L);
        record1.put("name", new Utf8("Alice"));

        Record bind1 = recordBinder.bind(record1);
        assertEquals(1L, bind1.get(0));
        assertEquals("Alice", bind1.get(1).toString());

        // Reuse wrapper for second record
        GenericRecord record2 = new GenericData.Record(avroSchema);
        record2.put("id", 2L);
        record2.put("name", new Utf8("Bob"));

        Record bind2 = recordBinder.bind(record2);
        assertEquals(2L, bind2.get(0));
        assertEquals("Bob", bind2.get(1).toString());
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
    public void testListOfRecordsConversion() {
        String avroSchemaJson = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"ListRecordContainer\",\n"
            + "  \"namespace\": \"" + TEST_NAMESPACE + "\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"listField\",\n"
            + "      \"type\": {\n"
            + "        \"type\": \"array\",\n"
            + "        \"items\": {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"ListRecordEntry\",\n"
            + "          \"fields\": [\n"
            + "            {\"name\": \"innerString\", \"type\": \"string\"},\n"
            + "            {\"name\": \"innerInt\", \"type\": \"int\"}\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);

        Schema listFieldSchema = avroSchema.getField("listField").schema();
        Schema listEntrySchema = listFieldSchema.getElementType();

        @SuppressWarnings("unchecked")
        GenericData.Array<GenericRecord> listValue = new GenericData.Array<>(2, listFieldSchema);

        GenericRecord firstEntry = new GenericData.Record(listEntrySchema);
        firstEntry.put("innerString", new Utf8("first"));
        firstEntry.put("innerInt", 1);
        listValue.add(firstEntry);

        GenericRecord secondEntry = new GenericData.Record(listEntrySchema);
        secondEntry.put("innerString", new Utf8("second"));
        secondEntry.put("innerInt", 2);
        listValue.add(secondEntry);

        avroRecord.put("listField", listValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema)
            .bind(serializeAndDeserialize(avroRecord, avroSchema));

        @SuppressWarnings("unchecked")
        List<Record> boundList = (List<Record>) icebergRecord.getField("listField");
        assertEquals(2, boundList.size());
        assertEquals("first", boundList.get(0).getField("innerString").toString());
        assertEquals(1, boundList.get(0).getField("innerInt"));
        assertEquals("second", boundList.get(1).getField("innerString").toString());
        assertEquals(2, boundList.get(1).getField("innerInt"));

        testSendRecord(icebergSchema, icebergRecord);
    }

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
        Record icebergRecord = new RecordBinder(icebergSchema, parent)
            .bind(serializeAndDeserialize(parentRecord, parent));

        Record directField = (Record) icebergRecord.getField("directField");
        assertEquals("direct", directField.getField("directOnly").toString());

        @SuppressWarnings("unchecked")
        List<Record> boundList = (List<Record>) icebergRecord.getField("listField");
        assertEquals(1, boundList.size());
        assertEquals(42, boundList.get(0).getField("listOnly"));
    }

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
        Record icebergRecord = new RecordBinder(icebergSchema, parent)
            .bind(serializeAndDeserialize(parentRecord, parent));

        Record directField = (Record) icebergRecord.getField("directField");
        assertEquals("direct", directField.getField("directOnly").toString());

        @SuppressWarnings("unchecked")
        Map<CharSequence, Record> boundMap = (Map<CharSequence, Record>) icebergRecord.getField("mapField");
        assertEquals(1, boundMap.size());
        assertEquals(123L, boundMap.get(new Utf8("key")).getField("mapOnly"));
    }

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

    @Test
    public void testNestedStructsBindRecursively() {
        Schema innerStruct = Schema.createRecord("InnerStruct", null, TEST_NAMESPACE, false);
        innerStruct.setFields(Arrays.asList(
            new Schema.Field("innerField", Schema.create(Schema.Type.INT), null, null)
        ));

        Schema middleStruct = Schema.createRecord("MiddleStruct", null, TEST_NAMESPACE, false);
        middleStruct.setFields(Arrays.asList(
            new Schema.Field("middleField", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("inner", innerStruct, null, null)
        ));

        Schema outerStruct = Schema.createRecord("OuterStruct", null, TEST_NAMESPACE, false);
        outerStruct.setFields(Arrays.asList(
            new Schema.Field("outerField", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("middle", middleStruct, null, null)
        ));

        GenericRecord innerRecord = new GenericData.Record(innerStruct);
        innerRecord.put("innerField", 7);

        GenericRecord middleRecord = new GenericData.Record(middleStruct);
        middleRecord.put("middleField", new Utf8("mid"));
        middleRecord.put("inner", innerRecord);

        GenericRecord outerRecord = new GenericData.Record(outerStruct);
        outerRecord.put("outerField", new Utf8("out"));
        outerRecord.put("middle", middleRecord);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(outerStruct);
        Record icebergRecord = new RecordBinder(icebergSchema, outerStruct)
            .bind(serializeAndDeserialize(outerRecord, outerStruct));

        Record middleResult = (Record) icebergRecord.getField("middle");
        assertEquals("mid", middleResult.getField("middleField").toString());
        Record innerResult = (Record) middleResult.getField("inner");
        assertEquals(7, innerResult.getField("innerField"));
    }

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
        Record icebergRecord = new RecordBinder(icebergSchema, parent)
            .bind(serializeAndDeserialize(parentRecord, parent));

        Record directRecord = (Record) icebergRecord.getField("directField");
        assertEquals(1L, directRecord.getField("value"));

        @SuppressWarnings("unchecked")
        List<Record> boundList = (List<Record>) icebergRecord.getField("listField");
        assertEquals(2, boundList.size());
        assertEquals(2L, boundList.get(0).getField("value"));
        assertEquals(3L, boundList.get(1).getField("value"));
    }

    // Test method for converting a map field
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

    @Test
    public void testMapWithRecordValuesConversion() {
        String avroSchemaJson = "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MapRecordContainer\",\n"
            + "  \"namespace\": \"" + TEST_NAMESPACE + "\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"mapField\",\n"
            + "      \"type\": {\n"
            + "        \"type\": \"map\",\n"
            + "        \"values\": {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"MapValueRecord\",\n"
            + "          \"fields\": [\n"
            + "            {\"name\": \"innerString\", \"type\": \"string\"},\n"
            + "            {\"name\": \"innerLong\", \"type\": \"long\"}\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);

        Schema mapFieldSchema = avroSchema.getField("mapField").schema();
        Schema mapValueSchema = mapFieldSchema.getValueType();

        Map<String, GenericRecord> mapValue = new HashMap<>();
        GenericRecord firstValue = new GenericData.Record(mapValueSchema);
        firstValue.put("innerString", new Utf8("first"));
        firstValue.put("innerLong", 10L);
        mapValue.put("key1", firstValue);

        GenericRecord secondValue = new GenericData.Record(mapValueSchema);
        secondValue.put("innerString", new Utf8("second"));
        secondValue.put("innerLong", 20L);
        mapValue.put("key2", secondValue);

        avroRecord.put("mapField", mapValue);

        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema)
            .bind(serializeAndDeserialize(avroRecord, avroSchema));

        Map<String, Object> boundMap = normalizeMapValues(icebergRecord.getField("mapField"));
        assertEquals(2, boundMap.size());

        Record key1Record = (Record) boundMap.get(new Utf8("key1"));
        assertEquals("first", key1Record.getField("innerString").toString());
        assertEquals(10L, key1Record.getField("innerLong"));

        Record key2Record = (Record) boundMap.get(new Utf8("key2"));
        assertEquals("second", key2Record.getField("innerString").toString());
        assertEquals(20L, key2Record.getField("innerLong"));

        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a map field
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

    // Test method for converting a map field with non-string keys
    // Maps with non-string keys must use an array representation with the map logical type.
    // The array representation or Avro’s map type may be used for maps with string keys.
    @Test
    public void testMapWithNonStringKeysConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\n" +
            "          \"name\": \"mapField\",\n" +
            "          \"type\": {\n" +
            "            \"type\": \"array\",\n" +
            "            \"logicalType\": \"map\",\n" +
            "            \"items\": {\n" +
            "              \"type\": \"record\",\n" +
            "              \"name\": \"MapEntry\",\n" +
            "              \"fields\": [\n" +
            "                {\"name\": \"key\", \"type\": \"int\"},\n" +
            "                {\"name\": \"value\", \"type\": \"string\"}\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        Map<Integer, String> expectedMap = new HashMap<>();
        expectedMap.put(1, "value1");
        expectedMap.put(2, "value2");
        expectedMap.put(3, "value3");

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        List<GenericRecord> mapEntries = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : expectedMap.entrySet()) {
            GenericRecord mapEntry = new GenericData.Record(avroSchema.getField("mapField").schema().getElementType());
            mapEntry.put("key", entry.getKey());
            mapEntry.put("value", entry.getValue());
            mapEntries.add(mapEntry);
        }
        avroRecord.put("mapField", mapEntries);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Convert the list of records back to a map
        Map<Integer, Object> mapField = normalizeMapValues(icebergRecord.getField("mapField"));
        // Verify the field value
        assertEquals(expectedMap, mapField);

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

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

    // Test method for converting a record with nested fields
    @Test
    public void testNestedRecordConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\n" +
            "          \"name\": \"nestedField\",\n" +
            "          \"type\": {\n" +
            "            \"type\": \"record\",\n" +
            "            \"name\": \"NestedRecord\",\n" +
            "            \"fields\": [\n" +
            "              {\"name\": \"nestedStringField\", \"type\": \"string\"},\n" +
            "              {\"name\": \"nestedIntField\", \"type\": \"int\"}\n" +
            "            ]\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord nestedRecord = new GenericData.Record(avroSchema.getField("nestedField").schema());
        nestedRecord.put("nestedStringField", "nested_string");
        nestedRecord.put("nestedIntField", 42);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("nestedField", nestedRecord);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field values
        Record nestedIcebergRecord = (Record) icebergRecord.getField("nestedField");
        assertEquals("nested_string", nestedIcebergRecord.getField("nestedStringField").toString());
        assertEquals(42, nestedIcebergRecord.getField("nestedIntField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a record with optional fields
    // Optional fields must always set the Avro field default value to null.
    @Test
    public void testOptionalFieldConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"optionalStringField\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "        {\"name\": \"optionalIntField\", \"type\": [\"null\", \"int\"], \"default\": null},\n" +
            "        {\"name\": \"optionalStringNullField\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "        {\"name\": \"optionalIntNullField\", \"type\": [\"null\", \"int\"], \"default\": null}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("optionalStringField", "optional_string");
        avroRecord.put("optionalIntField", 42);
        avroRecord.put("optionalStringNullField", null);
        avroRecord.put("optionalIntNullField", null);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field values
        assertEquals("optional_string", icebergRecord.getField("optionalStringField").toString());
        assertEquals(42, icebergRecord.getField("optionalIntField"));
        assertNull(icebergRecord.getField("optionalStringNullField"));
        assertNull(icebergRecord.getField("optionalIntNullField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a record with default values
    @Test
    public void testDefaultFieldConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"defaultStringField\", \"type\": \"string\", \"default\": \"default_string\"},\n" +
            "        {\"name\": \"defaultIntField\", \"type\": \"int\", \"default\": 42}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Schema.Field defaultStringField = avroSchema.getField("defaultStringField");
        Schema.Field defaultIntField = avroSchema.getField("defaultIntField");
        avroRecord.put("defaultStringField", defaultStringField.defaultVal());
        avroRecord.put("defaultIntField", defaultIntField.defaultVal());

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field values
        assertEquals("default_string", icebergRecord.getField("defaultStringField").toString());
        assertEquals(42, icebergRecord.getField("defaultIntField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a record with union fields
    // Optional fields, array elements, and map values must be wrapped in an Avro union with null.
    // This is the only union type allowed in Iceberg data files.
    @Test
    public void testUnionFieldConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\n" +
            "          \"name\": \"unionField1\",\n" +
            "          \"type\": [\"null\", \"string\"]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionField2\",\n" +
            "          \"type\": [\"null\", \"int\"]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionField3\",\n" +
            "          \"type\": [\"null\", \"boolean\"]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionField4\",\n" +
            "          \"type\": [\"null\", \"string\"]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionListField\",\n" +
            "          \"type\": [\n" +
            "            \"null\",\n" +
            "            {\n" +
            "              \"type\": \"array\",\n" +
            "              \"items\": \"string\"\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionMapField\",\n" +
            "          \"type\": [\n" +
            "            \"null\",\n" +
            "            {\n" +
            "              \"type\": \"map\",\n" +
            "              \"values\": \"int\"\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"unionStructField\",\n" +
            "          \"type\": [\n" +
            "            \"null\",\n" +
            "            {\n" +
            "              \"type\": \"record\",\n" +
            "              \"name\": \"UnionStruct\",\n" +
            "              \"fields\": [\n" +
            "                {\"name\": \"innerString\", \"type\": \"string\"},\n" +
            "                {\"name\": \"innerInt\", \"type\": \"int\"}\n" +
            "              ]\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("unionField1", "union_string");
        avroRecord.put("unionField2", 42);
        avroRecord.put("unionField3", true);
        List<String> unionList = Arrays.asList("item1", "item2");
        avroRecord.put("unionListField", unionList);
        Map<String, Integer> unionMap = new HashMap<>();
        unionMap.put("one", 1);
        unionMap.put("two", 2);
        avroRecord.put("unionMapField", unionMap);
        Schema unionStructSchema = avroSchema.getField("unionStructField").schema().getTypes().get(1);
        GenericRecord unionStruct = new GenericData.Record(unionStructSchema);
        unionStruct.put("innerString", "nested");
        unionStruct.put("innerInt", 99);
        avroRecord.put("unionStructField", unionStruct);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        Object unionField1 = icebergRecord.getField("unionField1");
        assertEquals("union_string", unionField1.toString());

        Object unionField2 = icebergRecord.getField("unionField2");
        assertEquals(42, unionField2);

        Object unionField3 = icebergRecord.getField("unionField3");
        assertEquals(true, unionField3);

        assertNull(icebergRecord.getField("unionField4"));

        assertEquals(unionList, normalizeValue(icebergRecord.getField("unionListField")));
        assertEquals(unionMap, normalizeValue(icebergRecord.getField("unionMapField")));

        Record unionStructRecord = (Record) icebergRecord.getField("unionStructField");
        assertEquals("nested", unionStructRecord.getField("innerString").toString());
        assertEquals(99, unionStructRecord.getField("innerInt"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    @Test
    public void testBindWithNestedOptionalRecord() {
        // Schema representing a record with an optional nested record field, similar to Debezium envelopes.
        String avroSchemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Envelope\",\n" +
            "  \"namespace\": \"inventory.inventory.customers\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"before\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"Value\",\n" +
            "          \"fields\": [\n" +
            "            { \"name\": \"id\", \"type\": \"int\" },\n" +
            "            { \"name\": \"first_name\", \"type\": \"string\" }\n" +
            "          ]\n" +
            "        }\n" +
            "      ],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

        // Corresponding Iceberg Schema
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        // This binder will recursively create a nested binder for the 'before' field.
        // The nested binder will receive a UNION schema, which is what our fix addresses.
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        // --- Test Case 1: Nested record is present ---
        Schema valueSchema = avroSchema.getField("before").schema().getTypes().get(1);
        GenericRecord valueRecord = new GenericData.Record(valueSchema);
        valueRecord.put("id", 101);
        valueRecord.put("first_name", "John");

        GenericRecord envelopeRecord = new GenericData.Record(avroSchema);
        envelopeRecord.put("before", valueRecord);

        Record boundRecord = recordBinder.bind(envelopeRecord);
        Record nestedBoundRecord = (Record) boundRecord.getField("before");

        assertEquals(101, nestedBoundRecord.getField("id"));
        assertEquals("John", nestedBoundRecord.getField("first_name"));

        // --- Test Case 2: Nested record is null ---
        GenericRecord envelopeRecordWithNull = new GenericData.Record(avroSchema);
        envelopeRecordWithNull.put("before", null);

        Record boundRecordWithNull = recordBinder.bind(envelopeRecordWithNull);
        assertNull(boundRecordWithNull.getField("before"));
    }

    // Test method for field count statistics
    @Test
    public void testFieldCountStatistics() {
        // Test different field types and their count calculations
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

        // Create test record with different field sizes
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("smallString", "small"); // 5 chars = 3 field
        avroRecord.put("largeString", "a".repeat(50)); // 50 chars = 3 + 50/32 = 4
        avroRecord.put("intField", 42); // primitive = 1 field
        avroRecord.put("binaryField", ByteBuffer.wrap("test".repeat(10).getBytes())); // 5
        avroRecord.put("optionalStringField", "optional");

        // Bind record - this should trigger field counting
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

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(16, recordBinder.getAndResetFieldCount());
    }

    @Test
    public void testFieldCountWithComplexTypes() {
        // Test field counting for LIST and MAP types
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
        // List with 3 small strings: 1 (list itself) + 3 * 3 * 1 = 10 fields
        avroRecord.put("stringList", Arrays.asList("a", "b", "c"));

        // Map with 2 entries: 1 (map itself) + 2 * (3 key + 3 value) = 13 fields
        Map<String, String> map = new HashMap<>();
        map.put("key1", "val1");
        map.put("key2", "val2");
        avroRecord.put("stringMap", map);

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access fields to trigger counting
        assertEquals(Arrays.asList("a", "b", "c"), normalizeValue(icebergRecord.getField("stringList")));
        assertEquals(map, normalizeValue(icebergRecord.getField("stringMap")));

        // Total: 10 (list) + 13 (map) = 23 fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(23, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(23, recordBinder.getAndResetFieldCount());
    }

    @Test
    public void testFieldCountWithNestedStructure() {
        // Test field counting for nested records
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

        // Create nested record
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
        // Note: STRUCT type itself doesn't add to count, only its leaf fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(8, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(8, recordBinder.getAndResetFieldCount());
    }

    @Test
    public void testFieldCountBatchAccumulation() {
        // Test that field counts accumulate across multiple record bindings
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
            avroRecord.put("stringField", "test" + i); // 1 field each
            avroRecord.put("intField", i); // 1 field each

            Record icebergRecord = recordBinder.bind(avroRecord);
            // Access fields to trigger counting
            icebergRecord.getField("stringField");
            icebergRecord.getField("intField");
        }

        // Total: 3 records * 4 fields each = 12 fields
        long totalFieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(12, totalFieldCount);
    }

    @Test
    public void testFieldCountWithNullValues() {
        // Test that null values don't contribute to field count
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
        avroRecord.put("nonNullField", "value"); // 1 field
        avroRecord.put("nullField", null); // 0 fields

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access both fields
        assertEquals("value", icebergRecord.getField("nonNullField"));
        assertNull(icebergRecord.getField("nullField"));

        // Only the non-null field should count
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(3, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(3, recordBinder.getAndResetFieldCount());
    }

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

        GenericRecord nonNullRecord = new GenericData.Record(avroSchema);
        nonNullRecord.put("optionalString", "value");

        Record icebergRecord = recordBinder.bind(nonNullRecord);
        assertEquals("value", icebergRecord.getField("optionalString").toString());

        assertEquals(3, recordBinder.getAndResetFieldCount());

        GenericRecord nullRecord = new GenericData.Record(avroSchema);
        nullRecord.put("optionalString", null);

        Record nullIcebergRecord = recordBinder.bind(nullRecord);
        assertNull(nullIcebergRecord.getField("optionalString"));
        assertEquals(0, recordBinder.getAndResetFieldCount());
    }
}
