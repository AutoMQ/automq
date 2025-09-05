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

import kafka.automq.table.process.DefaultRecordProcessor;
import kafka.automq.table.process.RecordProcessor;
import kafka.automq.table.process.convert.AvroRegistryConverter;
import kafka.automq.table.process.convert.StringConverter;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class IcebergWriterSchemaEvolutionTest {
    private static final String TOPIC = "test-topic";
    private static final byte MAGIC_BYTE = 0x0;

    @Mock
    private KafkaAvroDeserializer kafkaAvroDeserializer;

    private InMemoryCatalog catalog;
    private IcebergWriter writer;
    private TableIdentifier tableId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        String tableName = generateRandomTableName();
        tableId = TableIdentifier.of("default", tableName);
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.partitionBy()).thenReturn(Collections.emptyList());
        IcebergTableManager tableManager = new IcebergTableManager(catalog, tableId, config);

        AvroRegistryConverter registryConverter = new AvroRegistryConverter(kafkaAvroDeserializer, null);
        RecordProcessor processor = new DefaultRecordProcessor(TOPIC, StringConverter.INSTANCE, registryConverter);
        writer = new IcebergWriter(tableManager, processor, config);
        writer.setOffset(0, 0);
    }

    private String generateRandomTableName() {
        int randomNum = ThreadLocalRandom.current().nextInt(1000, 10000);
        return "test_table_" + randomNum;
    }

    @Test
    void testSchemaEvolutionAddColumn() throws IOException {
        // Given: Initial Avro schema (v1)
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "test");

        // Given: Updated Avro schema (v2) with new email field
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV2.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        Schema emailSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        fieldsV2.add(new Schema.Field("email", emailSchema, null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", "test2");
        avroRecordV2.put("email", "test@example.com");

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        writer.write(0, kafkaRecordV1);

        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        writer.write(0, kafkaRecordV2);

        // Verify schema evolution
        Table table = catalog.loadTable(tableId);
        assertNotNull(table);
        assertEquals(4, table.schema().columns().size());
        assertNotNull(table.schema().findField("_kafka_value.email"));
    }

    @Test
    void testSchemaEvolutionMakeColumnOptional() throws IOException {
        // Given: Initial Avro schema with required fields
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "test");

        // Given: Updated schema with optional name field
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        Schema optionalName = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        fieldsV2.add(new Schema.Field("name", optionalName, null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", null);

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        writer.write(0, kafkaRecordV1);

        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        writer.write(0, kafkaRecordV2);

        // Verify schema evolution
        Table table = catalog.loadTable(tableId);
        assertNotNull(table);
        assertEquals(false, table.schema().findField("_kafka_value.name").isRequired());
    }

    @Test
    void testSchemaEvolutionChangeColumnType() throws IOException {
        // Given: Initial Avro schema with integer type
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("count", Schema.create(Schema.Type.INT), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("count", 100);

        // Given: Updated schema with long type
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV2.add(new Schema.Field("count", Schema.create(Schema.Type.LONG), null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("count", 1000L);

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        writer.write(0, kafkaRecordV1);

        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        writer.write(0, kafkaRecordV2);

        // Verify schema evolution
        Table table = catalog.loadTable(tableId);
        assertNotNull(table);
        assertEquals(Types.LongType.get(), table.schema().findField("_kafka_value.count").type());
    }

    @Test
    void testSchemaEvolutionDropColumn() throws IOException {
        // Given: Initial Avro schema (v1)
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        fieldsV1.add(new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "test");
        avroRecordV1.put("email", "test@example.com");

        // Given: Updated Avro schema (v2) with dropped email field
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV2.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", "test2");

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        writer.write(0, kafkaRecordV1);

        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        writer.write(0, kafkaRecordV2);

        // Verify schema evolution
        Table table = catalog.loadTable(tableId);
        assertNotNull(table);
        assertEquals(4, table.schema().columns().size());
        assertNotNull(table.schema().findField("_kafka_value.email"));
        assertEquals(false, table.schema().findField("_kafka_value.email").isRequired());
    }

    @Test
    void testSchemaEvolutionReorderColumn() throws IOException {
        // Given: Initial Avro schema (v1)
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        fieldsV1.add(new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "test");
        avroRecordV1.put("email", "test@example.com");

        // Given: Updated Avro schema (v2) with reordered fields
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null));
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV2.add(new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("name", "test2");
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("email", "test2@example.com");

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        writer.write(0, kafkaRecordV1);

        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        writer.write(0, kafkaRecordV2);

        // Verify schema evolution
        Table table = catalog.loadTable(tableId);
        assertNotNull(table);
        assertEquals(4, table.schema().columns().size());
        assertNotNull(table.schema().findField("_kafka_value.id"));
        assertNotNull(table.schema().findField("_kafka_value.name"));
        assertNotNull(table.schema().findField("_kafka_value.email"));
    }

    @Test
    void testSchemaEvolutionIncompatibleTypeChange() throws IOException {
        // Given: Initial Avro schema with string type
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV1 = new ArrayList<>();
        fieldsV1.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV1.add(new Schema.Field("age", Schema.create(Schema.Type.INT), null, null));
        avroSchemaV1.setFields(fieldsV1);

        // Create v1 record
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 2L);
        avroRecordV1.put("age", 30);

        // Given: Updated schema with incompatible string type for age
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fieldsV2 = new ArrayList<>();
        fieldsV2.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
        fieldsV2.add(new Schema.Field("age", Schema.create(Schema.Type.STRING), null, null));
        avroSchemaV2.setFields(fieldsV2);

        // Create v2 record
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 1L);
        avroRecordV2.put("age", "twenty");

        // Mock deserializer behavior
        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        // Write records
        Record kafkaRecordV1 = createMockKafkaRecord(1, 0);
        assertDoesNotThrow(() -> writer.write(0, kafkaRecordV1));

        // Verify that writing the second record with an incompatible schema throws an exception
        Record kafkaRecordV2 = createMockKafkaRecord(2, 1);
        assertThrows(IOException.class, () -> writer.write(0, kafkaRecordV2));
    }

    private Record createMockKafkaRecord(int schemaId, int offset) {
        ByteBuffer value = ByteBuffer.allocate(5 + 10); // 1 byte magic + 4 bytes schema ID + some space for data
        value.put(MAGIC_BYTE);
        value.putInt(schemaId);
        // Add some dummy data
        value.put("test".getBytes());

        return new Record() {
            @Override
            public long offset() {
                return offset;
            }

            @Override
            public int sequence() {
                return 0;
            }

            @Override
            public int sizeInBytes() {
                return value.array().length;
            }

            @Override
            public long timestamp() {
                return System.currentTimeMillis();
            }

            @Override
            public void ensureValid() {
            }

            @Override
            public int keySize() {
                return 0;
            }

            @Override
            public boolean hasKey() {
                return false;
            }

            @Override
            public ByteBuffer key() {
                return null;
            }

            @Override
            public int valueSize() {
                return value.array().length;
            }

            @Override
            public boolean hasValue() {
                return true;
            }

            @Override
            public ByteBuffer value() {
                return ByteBuffer.wrap(value.array());
            }

            @Override
            public boolean hasMagic(byte b) {
                return b == MAGIC_BYTE;
            }

            @Override
            public boolean isCompressed() {
                return false;
            }

            @Override
            public boolean hasTimestampType(TimestampType timestampType) {
                return false;
            }

            @Override
            public Header[] headers() {
                return new Header[0];
            }
        };
    }
}
