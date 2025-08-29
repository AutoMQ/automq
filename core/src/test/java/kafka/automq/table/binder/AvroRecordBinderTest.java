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

import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("S3Unit")
public class AvroRecordBinderTest {

    private static Schema avroSchema;
    private InMemoryCatalog catalog;
    private Table table;
    private TaskWriter<Record> writer;

    static {
        CodecSetup.setup();
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
    }

    private void testSendRecord(org.apache.iceberg.Schema schema, Record record) {
        table = catalog.createTable(TableIdentifier.of(Namespace.of("default"), "test"), schema);
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
        assertEquals("John Doe", bind.get(1)); // name
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
        assertEquals("Alice", bind1.get(1));

        // Reuse wrapper for second record
        GenericRecord record2 = new GenericData.Record(avroSchema);
        record2.put("id", 2L);
        record2.put("name", new Utf8("Bob"));

        Record bind2 = recordBinder.bind(record2);
        assertEquals(2L, bind2.get(0));
        assertEquals("Bob", bind2.get(1));
    }


    // Test method for converting a single string field
    @Test
    public void testStringConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"stringField\", \"type\": \"string\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("stringField", "test_string");

        GenericRecord record = serializeAndDeserialize(avroRecord, avroSchema);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(record);

        // Verify the field value
        assertEquals("test_string", icebergRecord.getField("stringField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single integer field
    @Test
    public void testIntegerConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"intField\", \"type\": \"int\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("intField", 42);

        GenericRecord record = serializeAndDeserialize(avroRecord, avroSchema);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(record);

        // Verify the field value
        assertEquals(42, icebergRecord.getField("intField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single long field
    @Test
    public void testLongConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"longField\", \"type\": \"long\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("longField", 123456789L);

        GenericRecord record = serializeAndDeserialize(avroRecord, avroSchema);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(record);

        // Verify the field value
        assertEquals(123456789L, icebergRecord.getField("longField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single float field
    @Test
    public void testFloatConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"floatField\", \"type\": \"float\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("floatField", 3.14f);

        GenericRecord record = serializeAndDeserialize(avroRecord, avroSchema);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(record);

        // Verify the field value
        assertEquals(3.14f, icebergRecord.getField("floatField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single double field
    @Test
    public void testDoubleConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"doubleField\", \"type\": \"double\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("doubleField", 6.28);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(6.28, icebergRecord.getField("doubleField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single boolean field
    @Test
    public void testBooleanConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"booleanField\", \"type\": \"boolean\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("booleanField", true);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(true, icebergRecord.getField("booleanField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single date field (number of days from epoch)
    @Test
    public void testDateConversion() {
        // Define Avro schema
        String avroSchemaStr = "{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [ {\"name\": \"dateField\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}} ] }";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        LocalDate localDate = LocalDate.of(2020, 1, 1);
        int epochDays = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), localDate);
        avroRecord.put("dateField", epochDays); // Represents 2020-01-01

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(localDate, icebergRecord.getField("dateField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single time field (number of milliseconds from midnight)
    @Test
    public void testTimeConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"timeField\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}},\n" +
            "        {\"name\": \"timeField2\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        LocalTime localTime = LocalTime.of(10, 0);
        long epochMicros = localTime.toNanoOfDay() / 1000;
        avroRecord.put("timeField", epochMicros); // Represents 10:00 AM

        int epochMillis = (int) (localTime.toNanoOfDay() / 1_000_000);
        avroRecord.put("timeField2", epochMillis);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(localTime, icebergRecord.getField("timeField"));
        assertEquals(localTime, icebergRecord.getField("timeField2"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single timestamp field (number of milliseconds from epoch)
    // timestamp: Stores microseconds from 1970-01-01 00:00:00.000000. [1]
    // timestamptz: Stores microseconds from 1970-01-01 00:00:00.000000 UTC. [1]
    @Test
    public void testTimestampConversion() {
        // Define Avro schema
        // Avro type annotation adjust-to-utc is an Iceberg convention; default value is false if not present.
        // Avro logical type timestamp-nanos is an Iceberg convention; the Avro specification does not define this type.
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"timestampField1\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\", \"adjust-to-utc\": true}},\n" +
            "        {\"name\": \"timestampField2\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\", \"adjust-to-utc\": false}},\n" +
            "        {\"name\": \"timestampField3\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\", \"adjust-to-utc\": true}},\n" +
            "        {\"name\": \"timestampField4\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\", \"adjust-to-utc\": false}}\n" +
            "      ]\n" +
            "    }\n";
        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);

        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Instant instant = Instant.now();
        long timestampMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        long timestampMillis = instant.toEpochMilli();

        avroRecord.put("timestampField1", timestampMicros);
        avroRecord.put("timestampField2", timestampMicros);
        avroRecord.put("timestampField3", timestampMillis);
        avroRecord.put("timestampField4", timestampMillis);

        // Serialize and deserialize
        GenericRecord deserializedRecord = serializeAndDeserialize(avroRecord, avroSchema);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(deserializedRecord);

        // Verify the field value
        OffsetDateTime timestampField1 = (OffsetDateTime) icebergRecord.getField("timestampField1");
        assertEquals(DateTimeUtil.timestamptzFromMicros(timestampMicros), timestampField1);

        LocalDateTime timestampField2 = (LocalDateTime) icebergRecord.getField("timestampField2");
        assertEquals(DateTimeUtil.timestampFromMicros(timestampMicros), timestampField2);

        OffsetDateTime timestampField3 = (OffsetDateTime) icebergRecord.getField("timestampField3");
        assertEquals(DateTimeUtil.timestamptzFromMicros(timestampMillis * 1000), timestampField3);

        LocalDateTime timestampField4 = (LocalDateTime) icebergRecord.getField("timestampField4");
        assertEquals(DateTimeUtil.timestampFromMicros(timestampMillis * 1000), timestampField4);

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single binary field
    @Test
    public void testBinaryConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"binaryField\", \"type\": \"bytes\"}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        String randomAlphabetic = RandomStringUtils.randomAlphabetic(64);
        avroRecord.put("binaryField", ByteBuffer.wrap(randomAlphabetic.getBytes(StandardCharsets.UTF_8)));

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        ByteBuffer binaryField = (ByteBuffer) icebergRecord.getField("binaryField");
        assertEquals(randomAlphabetic, new String(binaryField.array(), StandardCharsets.UTF_8));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single fixed field
    @Test
    public void testFixedConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\n" +
            "          \"name\": \"fixedField\",\n" +
            "          \"type\": {\n" +
            "            \"type\": \"fixed\",\n" +
            "            \"name\": \"FixedField\",\n" +
            "            \"size\": 3\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        byte[] fixedBytes = "bar".getBytes(StandardCharsets.UTF_8);
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("fixedField", new GenericData.Fixed(avroSchema.getField("fixedField").schema(), fixedBytes));

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        byte[] result = (byte[]) icebergRecord.getField("fixedField");
        assertEquals("bar", new String(result, StandardCharsets.UTF_8));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single enum field
    @Test
    public void testEnumConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\n" +
            "          \"name\": \"enumField\",\n" +
            "          \"type\": {\n" +
            "            \"type\": \"enum\",\n" +
            "            \"name\": \"EnumField\",\n" +
            "            \"symbols\": [\"A\", \"B\", \"C\"]\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("enumField", new GenericData.EnumSymbol(avroSchema.getField("enumField").schema(), "B"));

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals("B", icebergRecord.getField("enumField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single UUID field
    @Test
    public void testUUIDConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"uuidField\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        UUID uuid = UUID.randomUUID();
        CharSequence charSequence = new Conversions.UUIDConversion().toCharSequence(uuid, avroSchema, LogicalTypes.uuid());

        avroRecord.put("uuidField", charSequence);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(uuid, UUIDUtil.convert((byte[]) icebergRecord.getField("uuidField")));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a single decimal field
    @Test
    public void testDecimalConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"decimalField\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 9, \"scale\": 2}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        BigDecimal bigDecimal = BigDecimal.valueOf(1000.00).setScale(2);
        LogicalTypes.Decimal decimalType = LogicalTypes.decimal(9, 2);
        byte[] decimalBytes = new Conversions.DecimalConversion().toBytes(bigDecimal, avroSchema, decimalType).array();
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("decimalField", ByteBuffer.wrap(decimalBytes));

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(bigDecimal, icebergRecord.getField("decimalField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a list field
    @Test
    public void testListConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"listField\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("listField", Arrays.asList("a", "b", "c"));

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(Arrays.asList("a", "b", "c"), icebergRecord.getField("listField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a map field
    @Test
    public void testStringMapConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"mapField\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        avroRecord.put("mapField", map);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(map, icebergRecord.getField("mapField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
    }

    // Test method for converting a map field
    @Test
    public void testIntMapConversion() {
        // Define Avro schema
        String avroSchemaStr = "    {\n" +
            "      \"type\": \"record\",\n" +
            "      \"name\": \"TestRecord\",\n" +
            "      \"fields\": [\n" +
            "        {\"name\": \"mapField\", \"type\": {\"type\": \"map\", \"values\": \"int\"}}\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Map<String, Integer> map = new HashMap<>();
        map.put("key1", 1);
        map.put("key2", 2);
        avroRecord.put("mapField", map);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        assertEquals(map, icebergRecord.getField("mapField"));

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
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
        @SuppressWarnings("unchecked")
        Map<Integer, String> mapField = (Map<Integer, String>) icebergRecord.getField("mapField");
        // Verify the field value
        assertEquals(expectedMap, mapField);

        // Send the record to the table
        testSendRecord(icebergSchema, icebergRecord);
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
        assertEquals("nested_string", nestedIcebergRecord.getField("nestedStringField"));
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
        assertEquals("optional_string", icebergRecord.getField("optionalStringField"));
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
        assertEquals("default_string", icebergRecord.getField("defaultStringField"));
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
            "        }\n" +
            "      ]\n" +
            "    }\n";
        avroSchema = new Schema.Parser().parse(avroSchemaStr);
        // Create Avro record
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("unionField1", "union_string");
        avroRecord.put("unionField2", 42);
        avroRecord.put("unionField3", true);

        // Convert Avro record to Iceberg record using the wrapper
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Record icebergRecord = new RecordBinder(icebergSchema, avroSchema).bind(serializeAndDeserialize(avroRecord, avroSchema));

        // Verify the field value
        Object unionField1 = icebergRecord.getField("unionField1");
        assertEquals("union_string", unionField1);

        Object unionField2 = icebergRecord.getField("unionField2");
        assertEquals(42, unionField2);

        Object unionField3 = icebergRecord.getField("unionField3");
        assertEquals(true, unionField3);

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
            "    {\"name\": \"binaryField\", \"type\": \"bytes\"}\n" +
            "  ]\n" +
            "}";

        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        RecordBinder recordBinder = new RecordBinder(icebergSchema, avroSchema);

        // Create test record with different field sizes
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("smallString", "small"); // 5 chars = 1 field (5+23)/24 = 1
        avroRecord.put("largeString", "a".repeat(50)); // 50 chars = 3 fields (50+23)/24 = 3
        avroRecord.put("intField", 42); // primitive = 1 field
        avroRecord.put("binaryField", ByteBuffer.wrap("test".repeat(10).getBytes())); // 40 bytes = 2 fields (40+31)/32 = 2

        // Bind record - this should trigger field counting
        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access all fields to trigger counting
        assertEquals("small", icebergRecord.getField("smallString"));
        assertEquals("a".repeat(50), icebergRecord.getField("largeString"));
        assertEquals(42, icebergRecord.getField("intField"));
        assertEquals("test".repeat(10), new String(((ByteBuffer) icebergRecord.getField("binaryField")).array()));

        // Check field count: 1 + 3 + 1 + 2 = 7 fields total
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(7, fieldCount);

        // Second call should return 0 (reset)
        assertEquals(0, recordBinder.getAndResetFieldCount());

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(7, recordBinder.getAndResetFieldCount());
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
        // List with 3 small strings: 1 (list itself) + 3 * 1 = 4 fields
        avroRecord.put("stringList", Arrays.asList("a", "b", "c"));

        // Map with 2 entries: 1 (map itself) + 2 * (1 key + 1 value) = 5 fields
        Map<String, String> map = new HashMap<>();
        map.put("key1", "val1");
        map.put("key2", "val2");
        avroRecord.put("stringMap", map);

        Record icebergRecord = recordBinder.bind(avroRecord);

        // Access fields to trigger counting
        assertEquals(Arrays.asList("a", "b", "c"), icebergRecord.getField("stringList"));
        assertEquals(map, icebergRecord.getField("stringMap"));

        // Total: 4 (list) + 5 (map) = 9 fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(9, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(9, recordBinder.getAndResetFieldCount());
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
        nestedRecord.put("nestedString", "nested"); // 1 field
        nestedRecord.put("nestedInt", 123); // 1 field

        GenericRecord mainRecord = new GenericData.Record(avroSchema);
        mainRecord.put("simpleField", "simple"); // 1 field
        mainRecord.put("nestedField", nestedRecord); // STRUCT fields are counted when accessed

        Record icebergRecord = recordBinder.bind(mainRecord);

        // Access all fields including nested ones
        assertEquals("simple", icebergRecord.getField("simpleField"));
        Record nested = (Record) icebergRecord.getField("nestedField");
        assertEquals("nested", nested.getField("nestedString"));
        assertEquals(123, nested.getField("nestedInt"));

        // Total: 1 (simple) + 1(struct) + 1 (nested string) + 1 (nested int) = 4 fields
        // Note: STRUCT type itself doesn't add to count, only its leaf fields
        long fieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(4, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(4, recordBinder.getAndResetFieldCount());
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

        // Total: 3 records * 2 fields each = 6 fields
        long totalFieldCount = recordBinder.getAndResetFieldCount();
        assertEquals(6, totalFieldCount);
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
        assertEquals(1, fieldCount);

        testSendRecord(icebergSchema.asStruct().asSchema(), icebergRecord);
        assertEquals(1, recordBinder.getAndResetFieldCount());
    }
}
