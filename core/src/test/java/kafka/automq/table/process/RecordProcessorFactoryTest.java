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
package kafka.automq.table.process;

import kafka.automq.table.deserializer.proto.CustomProtobufSchema;
import kafka.automq.table.deserializer.proto.ProtobufSchemaProvider;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.ProcessorInitializationException;
import kafka.automq.table.process.exception.TransformException;
import kafka.automq.table.process.proto.PersonProto;
import kafka.automq.table.worker.WorkerConfig;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.server.record.TableTopicConvertType;
import org.apache.kafka.server.record.TableTopicTransformType;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordProcessorFactoryTest {

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_PARTITION = 0;
    private static final long TEST_OFFSET = 123L;
    private static final long TEST_TIMESTAMP = System.currentTimeMillis();

    private static final Schema USER_SCHEMA = SchemaBuilder.record("User")
        .namespace("kafka.automq.table.process")
        .fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().noDefault()
        .endRecord();

    private static final Schema PRODUCT_SCHEMA = SchemaBuilder.record("Product")
        .namespace("kafka.automq.table.process")
        .fields()
        .name("product_id").type().longType().noDefault()
        .name("product_name").type().stringType().noDefault()
        .name("price").type().doubleType().noDefault()
        .endRecord();

    private static final Schema DEBEZIUM_ENVELOPE_SCHEMA = SchemaBuilder.record("Envelope")
        .namespace("io.debezium.connector.mysql")
        .fields()
        .name("before").type().unionOf().nullType().and().type(PRODUCT_SCHEMA).endUnion().noDefault()
        .name("after").type().unionOf().nullType().and().type(PRODUCT_SCHEMA).endUnion().noDefault()
        .name("source").type(SchemaBuilder.record("Source")
            .fields()
            .name("db").type().stringType().noDefault()
            .name("table").type().stringType().noDefault()
            .endRecord())
        .noDefault()
        .name("op").type().stringType().noDefault()
        .name("ts_ms").type().longType().noDefault()
        .endRecord();

    private SchemaRegistryClient schemaRegistryClient;
    private RecordProcessorFactory recordProcessorFactory;
    private KafkaAvroSerializer avroSerializer;

    @BeforeEach
    void setUp() {
        schemaRegistryClient = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        recordProcessorFactory = new RecordProcessorFactory("http://mock:8081", schemaRegistryClient);
        avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        avroSerializer.configure(Map.of("schema.registry.url", "http://mock:8081"), false);
    }

    // --- Test Group 1: RAW Converter ---

    @Test
    void testRawConverterWithoutTransforms() {
        // Arrange
        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.RAW);
        when(mockConfig.transformTypes()).thenReturn(Collections.emptyList());

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, value, key);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess(), "Processing should be successful");
        assertNull(result.getError(), "Error should be null");

        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertEquals(Converter.CONVERSION_RECORD_NAME, finalRecord.getSchema().getName());
        assertEquals(ByteBuffer.wrap(key), ByteBuffer.wrap((byte[]) finalRecord.get(Converter.KEY_FIELD_NAME)));
        assertEquals(ByteBuffer.wrap(value), ByteBuffer.wrap((byte[]) finalRecord.get(Converter.VALUE_FIELD_NAME)));
        assertEquals(TEST_TIMESTAMP, finalRecord.get(Converter.TIMESTAMP_FIELD_NAME));
    }

    @Test
    void testRawConverterWithMetadataTransform() {
        // Arrange
        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.RAW);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.KAFKA_METADATA));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        byte[] value = "metadata-test".getBytes();
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, value, "test-key".getBytes());

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertTrue(finalRecord.hasField("_kafka_metadata"));
        GenericRecord metadataRecord = (GenericRecord) finalRecord.get("_kafka_metadata");
        assertEquals(TEST_PARTITION, metadataRecord.get("partition"));
        assertEquals(TEST_OFFSET, metadataRecord.get("offset"));
        assertEquals(TEST_TIMESTAMP, metadataRecord.get("timestamp"));
        assertEquals(ByteBuffer.wrap(value), ByteBuffer.wrap((byte[]) finalRecord.get(Converter.VALUE_FIELD_NAME)));
    }

    // --- Test Group 2: BY_SCHEMA_ID Converter ---

    @Test
    void testBySchemaIdWithAvro() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        Schema schema = Schema.create(Schema.Type.STRING);
        int schemaId = registerSchema(subject, schema);

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of());

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, "test123", "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));
        assertEquals(String.valueOf(schemaId), result.getFinalSchemaIdentity());
        assertNotNull(result.getFinalRecord());
        assertTrue(result.getFinalRecord().hasField("key"));
        assertEquals("test-key", result.getFinalRecord().get("key"));
        assertTrue(result.getFinalRecord().hasField("value"));
        assertEquals("test123", result.getFinalRecord().get("value"));
    }

    @Test
    void testBySchemaIdWithUnwrap() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, USER_SCHEMA);

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        GenericRecord userRecord = new GenericRecordBuilder(USER_SCHEMA)
            .set("name", "test-user")
            .set("age", 30)
            .build();
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, userRecord, "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(String.valueOf(schemaId), result.getFinalSchemaIdentity());
        assertEquals(userRecord, result.getFinalRecord());
        assertEquals(USER_SCHEMA, result.getFinalRecord().getSchema());
    }

    @Test
    void testBySchemaIdWithUnwrapAndMetadata() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, USER_SCHEMA);

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP, TableTopicTransformType.KAFKA_METADATA));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        GenericRecord userRecord = new GenericRecordBuilder(USER_SCHEMA)
            .set("name", "test-user-meta")
            .set("age", 31)
            .build();
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, userRecord, "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertEquals("test-user-meta", finalRecord.get("name").toString());
        assertEquals(31, finalRecord.get("age"));

        assertTrue(finalRecord.hasField("_kafka_metadata"));
        GenericRecord metadataRecord = (GenericRecord) finalRecord.get("_kafka_metadata");
        assertEquals(TEST_PARTITION, metadataRecord.get("partition"));
        assertEquals(TEST_OFFSET, metadataRecord.get("offset"));
        assertEquals(TEST_TIMESTAMP, metadataRecord.get("timestamp"));
    }

    // --- Test Group 3: Debezium Unwrap ---

    @Test
    void testBySchemaIdWithDebeziumUnwrap() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, DEBEZIUM_ENVELOPE_SCHEMA);

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP, TableTopicTransformType.DEBEZIUM_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        GenericRecord productRecord = new GenericRecordBuilder(PRODUCT_SCHEMA)
            .set("product_id", 1L)
            .set("product_name", "test-product")
            .set("price", 99.99)
            .build();

        GenericRecord sourceRecord = new GenericRecordBuilder(DEBEZIUM_ENVELOPE_SCHEMA.getField("source").schema())
            .set("db", "test_db")
            .set("table", "products")
            .build();

        GenericRecord debeziumRecord = new GenericRecordBuilder(DEBEZIUM_ENVELOPE_SCHEMA)
            .set("before", null)
            .set("after", productRecord)
            .set("source", sourceRecord)
            .set("op", "c")
            .set("ts_ms", TEST_TIMESTAMP)
            .build();

        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, debeziumRecord, "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertEquals(1L, finalRecord.get("product_id"));
        assertEquals("test-product", finalRecord.get("product_name").toString());

        assertTrue(finalRecord.hasField("_cdc"));
        GenericRecord cdcRecord = (GenericRecord) finalRecord.get("_cdc");
        assertEquals("I", cdcRecord.get("op").toString());
        assertEquals(TEST_TIMESTAMP, cdcRecord.get("ts"));
        assertEquals(TEST_OFFSET, cdcRecord.get("offset"));
        assertEquals("test_db.products", cdcRecord.get("source").toString());
    }

    @Test
    void testBySchemaIdWithDebeziumUnwrapAndMetadata() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, DEBEZIUM_ENVELOPE_SCHEMA);

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP, TableTopicTransformType.DEBEZIUM_UNWRAP, TableTopicTransformType.KAFKA_METADATA));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        GenericRecord productRecord = new GenericRecordBuilder(PRODUCT_SCHEMA)
            .set("product_id", 2L)
            .set("product_name", "test-product-2")
            .set("price", 199.99)
            .build();

        GenericRecord sourceRecord = new GenericRecordBuilder(DEBEZIUM_ENVELOPE_SCHEMA.getField("source").schema())
            .set("db", "test_db")
            .set("table", "products")
            .build();

        GenericRecord debeziumRecord = new GenericRecordBuilder(DEBEZIUM_ENVELOPE_SCHEMA)
            .set("before", null)
            .set("after", productRecord)
            .set("source", sourceRecord)
            .set("op", "u")
            .set("ts_ms", TEST_TIMESTAMP)
            .build();

        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, debeziumRecord, "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertTrue(finalRecord.hasField("_cdc"));
        GenericRecord cdcRecord = (GenericRecord) finalRecord.get("_cdc");
        assertEquals("U", cdcRecord.get("op").toString());

        assertTrue(finalRecord.hasField("_kafka_metadata"));
        GenericRecord metadataRecord = (GenericRecord) finalRecord.get("_kafka_metadata");
        assertEquals(TEST_PARTITION, metadataRecord.get("partition"));
        assertEquals(TEST_OFFSET, metadataRecord.get("offset"));

        assertEquals(2L, finalRecord.get("product_id"));
    }

    // --- Test Group 4: BY_SUBJECT_NAME Converter ---

    @Test
    void testBySubjectNameThrowsExceptionForNonProtobufSchema() throws Exception {
        // Arrange
        String subject = "avro-subject";
        int schemaId = registerSchema(subject, USER_SCHEMA); // Register an AVRO schema

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SUBJECT_NAME);
        when(mockConfig.convertBySubjectNameSubject()).thenReturn(subject);

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, "any-topic");
        Record kafkaRecord = createKafkaRecord("any-topic", "dummy-payload".getBytes(), "test-key");

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError().getCause() instanceof ProcessorInitializationException, "Cause should be ProcessorInitializationException");
        assertTrue(result.getError().getCause().getMessage().contains("by_subject_name is only supported for PROTOBUF"));
    }

    @Test
    void testBySubjectNameWithProtobufSchema() throws Exception {
        // Arrange
        String subject = "proto-person-subject";
        String protoFileContent = Files.readString(Path.of("src/test/resources/proto/person.proto"));

        CustomProtobufSchema person = new CustomProtobufSchema("Person", -1, null, null, protoFileContent, List.of(), Map.of());
        int schemaId = schemaRegistryClient.register(subject, person);

        String messageFullName = "kafka.automq.table.process.proto.Person";

        PersonProto.Address address = PersonProto.Address.newBuilder()
            .setStreet("123 Main St")
            .setCity("Anytown")
            .build();

        long now = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(now / 1000).setNanos((int) ((now % 1000) * 1000000)).build();

        PersonProto.Person personMessage = PersonProto.Person.newBuilder()
            .setId(1L)
            .setName("Proto User")
            .setIsActive(true)
            .setAddress(address)
            .addRoles("admin")
            .addRoles("user")
            .putAttributes("team", "backend")
            .setLastUpdated(timestamp)
            .build();

        byte[] value = personMessage.toByteArray();
        Record kafkaRecord = createKafkaRecord(subject, value, "test-key".getBytes());

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SUBJECT_NAME);
        when(mockConfig.convertBySubjectNameSubject()).thenReturn(subject);
        when(mockConfig.convertBySubjectNameMessageFullName()).thenReturn(messageFullName);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, subject);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);

        assertEquals(1L, finalRecord.get("id"));
        assertEquals("Proto User", finalRecord.get("name").toString());
        assertEquals(true, finalRecord.get("is_active"));

        // Check nested record
        GenericRecord addressRecord = (GenericRecord) finalRecord.get("address");
        assertEquals("123 Main St", addressRecord.get("street").toString());

        // Check repeated field (list)
        List<CharSequence> roles = (List<CharSequence>) finalRecord.get("roles");
        assertEquals(2, roles.size());
        assertEquals("admin", roles.get(0).toString());

        // Check map (which is converted to a list of records)
        @SuppressWarnings("unchecked")
        List<GenericRecord> attributesList = (List<GenericRecord>) finalRecord.get("attributes");
        assertNotNull(attributesList);
        assertEquals(1, attributesList.size());
        GenericRecord attributeEntry = attributesList.get(0);
        assertEquals("team", attributeEntry.get("key").toString());
        assertEquals("backend", attributeEntry.get("value").toString());

        // Check timestamp (converted to long in microseconds)
        long expectedTimestampMicros = timestamp.getSeconds() * 1_000_000 + timestamp.getNanos() / 1000;
        assertEquals(expectedTimestampMicros, finalRecord.get("last_updated"));
    }


    @Test
    void testBySubjectNameWithFirstProtobufSchema() throws Exception {
        // Arrange
        String subject = "proto-address-subject";
        String protoFileContent = Files.readString(Path.of("src/test/resources/proto/person.proto"));

        CustomProtobufSchema addressSchema = new CustomProtobufSchema("Address", -1, null, null, protoFileContent, List.of(), Map.of());
        int schemaId = schemaRegistryClient.register(subject, addressSchema);

        String messageFullName = null;

        PersonProto.Address address = PersonProto.Address.newBuilder()
            .setStreet("123 Main St")
            .setCity("Anytown")
            .build();

        byte[] value = address.toByteArray();
        Record kafkaRecord = createKafkaRecord(subject, value, "test-key".getBytes());

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SUBJECT_NAME);
        when(mockConfig.convertBySubjectNameSubject()).thenReturn(subject);
        when(mockConfig.convertBySubjectNameMessageFullName()).thenReturn(messageFullName);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, subject);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertEquals(result.getFinalSchemaIdentity(), String.valueOf(schemaId));

        GenericRecord addressRecord = result.getFinalRecord();
        assertEquals("123 Main St", addressRecord.get("street").toString());
    }

    // --- Test Group 5: Error Handling ---

    @Test
    void testConvertErrorOnUnknownSchemaId() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, USER_SCHEMA);
        GenericRecord userRecord = new GenericRecordBuilder(USER_SCHEMA).set("name", "a").set("age", 1).build();
        byte[] validPayload = avroSerializer.serialize(TEST_TOPIC, userRecord);

        ByteBuffer buffer = ByteBuffer.wrap(validPayload);
        buffer.get(); // Magic byte
        buffer.putInt(9999); // Non-existent schema ID
        byte[] invalidPayload = buffer.array();

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, invalidPayload, "test-key".getBytes());

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertEquals(DataError.ErrorType.CONVERT_ERROR, result.getError().getType());
        assertTrue(result.getError().getCause() instanceof ConverterException);
    }

    @Test
    void testTransformErrorOnMismatchedData() throws Exception {
        // Arrange
        String subject = TEST_TOPIC + "-value";
        int schemaId = registerSchema(subject, USER_SCHEMA);
        GenericRecord userRecord = new GenericRecordBuilder(USER_SCHEMA).set("name", "a").set("age", 1).build();
        Record kafkaRecord = createKafkaRecord(TEST_TOPIC, userRecord, "test-key");

        WorkerConfig mockConfig = mock(WorkerConfig.class);
        when(mockConfig.convertType()).thenReturn(TableTopicConvertType.BY_SCHEMA_ID);
        when(mockConfig.transformTypes()).thenReturn(List.of(TableTopicTransformType.VALUE_UNWRAP, TableTopicTransformType.DEBEZIUM_UNWRAP));

        RecordProcessor processor = recordProcessorFactory.create(mockConfig, TEST_TOPIC);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertEquals(DataError.ErrorType.TRANSFORMATION_ERROR, result.getError().getType());
        assertTrue(result.getError().getCause() instanceof TransformException);
    }

    // --- Helper Methods ---

    private Record createKafkaRecord(String topic, byte[] value, byte[] key) {
        return new SimpleRecord(TEST_OFFSET, TEST_TIMESTAMP, key, value);
    }

    private Record createKafkaRecord(String topic, Object avroRecord, String key) {
        byte[] value = avroSerializer.serialize(topic, avroRecord);
        byte[] keyBytes = key.getBytes();
        return createKafkaRecord(topic, value, keyBytes);
    }

    private int registerSchema(String subject, Schema schema) throws Exception {
        return schemaRegistryClient.register(subject, new io.confluent.kafka.schemaregistry.avro.AvroSchema(schema));
    }

    private byte[] serializeProtobuf(int schemaId, Message message) {
        byte[] payload = message.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + payload.length);

        buffer.put(payload);
        return buffer.array();
    }

    /**
     * A simplified implementation of the Record interface for testing purposes.
     */
    private static class SimpleRecord implements Record {
        private final long offset;
        private final long timestamp;
        private final byte[] key;
        private final byte[] value;

        public SimpleRecord(long offset, long timestamp, byte[] key, byte[] value) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int sequence() {
            return -1;
        }

        @Override
        public int sizeInBytes() {
            int size = 0;
            if (key != null) size += key.length;
            if (value != null) size += value.length;
            return size;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public void ensureValid() {}

        @Override
        public int keySize() {
            return key != null ? key.length : -1;
        }

        @Override
        public boolean hasKey() {
            return key != null;
        }

        @Override
        public ByteBuffer key() {
            return key == null ? null : ByteBuffer.wrap(key);
        }

        @Override
        public int valueSize() {
            return value != null ? value.length : -1;
        }

        @Override
        public boolean hasValue() {
            return value != null;
        }

        @Override
        public ByteBuffer value() {
            return value == null ? null : ByteBuffer.wrap(value);
        }

        @Override
        public boolean hasMagic(byte b) {
            return false;
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
    }
}
