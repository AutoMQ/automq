
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

import kafka.automq.table.process.convert.AvroRegistryConverter;
import kafka.automq.table.process.convert.RawConverter;
import kafka.automq.table.process.convert.StringConverter;
import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;
import kafka.automq.table.process.exception.SchemaRegistrySystemException;
import kafka.automq.table.process.exception.TransformException;
import kafka.automq.table.process.transform.FlattenTransform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class DefaultRecordProcessorTest {

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_PARTITION = 0;
    private static final long TEST_OFFSET = 123L;
    private static final long TEST_TIMESTAMP = System.currentTimeMillis();

    private SchemaRegistryClient schemaRegistryClient;
    private KafkaAvroSerializer avroSerializer;

    private static final Schema USER_SCHEMA_V1 = SchemaBuilder.record("User")
        .namespace("kafka.automq.table.process")
        .fields()
        .name("name").type().stringType().noDefault()
        .endRecord();

    private static final Schema USER_SCHEMA_V2 = SchemaBuilder.record("User")
        .namespace("kafka.automq.table.process")
        .fields()
        .name("name").type().stringType().noDefault()
        .name("age").type().intType().intDefault(0)
        .endRecord();

    @BeforeEach
    void setUp() {
        schemaRegistryClient = new MockSchemaRegistryClient();
        avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        avroSerializer.configure(Map.of("schema.registry.url", "http://mock:8081"), false);
    }

    private Record createKafkaRecord(byte[] key, byte[] value, Header[] headers) {
        return new SimpleRecord(TEST_OFFSET, TEST_TIMESTAMP, key, value, headers);
    }

    private Record createAvroRecord(String topic, Object avroRecord, String key) {
        byte[] value = avroSerializer.serialize(topic, avroRecord);
        byte[] keyBytes = key.getBytes();
        return createKafkaRecord(keyBytes, value, new Header[0]);
    }

    @Test
    void testProcessWithSchemaUpdateShouldChangeSchemaIdentity() throws Exception {
        // Arrange
        String valueSubject = TEST_TOPIC + "-value";

        // Register V1 schema
        schemaRegistryClient.register(valueSubject, USER_SCHEMA_V1);

        // Create processor
        Converter keyConverter = new StringConverter();
        Converter valueConverter = new AvroRegistryConverter(schemaRegistryClient, "http://mock:8081", false);
        DefaultRecordProcessor recordProcessor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter);

        // Create first record with V1
        GenericRecord userRecordV1A = new GenericRecordBuilder(USER_SCHEMA_V1)
            .set("name", "test-user-A")
            .build();
        Record kafkaRecordV1A = createAvroRecord(TEST_TOPIC, userRecordV1A, "key1A");

        // Act 1
        ProcessingResult resultV1A = recordProcessor.process(TEST_PARTITION, kafkaRecordV1A);

        // Assert 1
        assertTrue(resultV1A.isSuccess());
        assertNotNull(resultV1A.getFinalSchemaIdentity());
        String identityV1A = resultV1A.getFinalSchemaIdentity();

        // Arrange 2: Create second record with the same V1 schema
        GenericRecord userRecordV1B = new GenericRecordBuilder(USER_SCHEMA_V1)
            .set("name", "test-user-B")
            .build();
        Record kafkaRecordV1B = createAvroRecord(TEST_TOPIC, userRecordV1B, "key1B");

        // Act 2
        ProcessingResult resultV1B = recordProcessor.process(TEST_PARTITION, kafkaRecordV1B);

        // Assert 2
        assertTrue(resultV1B.isSuccess());
        String identityV1B = resultV1B.getFinalSchemaIdentity();
        assertEquals(identityV1A, identityV1B, "Schema identity should be the same for the same schema version");

        // Arrange 3: Update schema to V2
        schemaRegistryClient.register(valueSubject, USER_SCHEMA_V2);

        // Create record with V2
        GenericRecord userRecordV2 = new GenericRecordBuilder(USER_SCHEMA_V2)
            .set("name", "test-user-2")
            .set("age", 30)
            .build();
        Record kafkaRecordV2 = createAvroRecord(TEST_TOPIC, userRecordV2, "key2");

        // Act 3
        ProcessingResult resultV2 = recordProcessor.process(TEST_PARTITION, kafkaRecordV2);

        // Assert 3
        assertTrue(resultV2.isSuccess());
        assertNotNull(resultV2.getFinalSchemaIdentity());
        String identityV2 = resultV2.getFinalSchemaIdentity();

        // Final assertion
        assertNotEquals(identityV1A, identityV2, "Schema identity should change after schema evolution");

        GenericRecord finalRecordV2 = resultV2.getFinalRecord();
        GenericRecord valueRecordV2 = (GenericRecord) finalRecordV2.get(RecordAssembler.KAFKA_VALUE_FIELD);
        assertEquals("test-user-2", valueRecordV2.get("name").toString());
        assertEquals(30, valueRecordV2.get("age"));
    }

    @Test
    void testBasicRawProcessing() {
        // Arrange
        Converter rawConverter = new RawConverter();
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, rawConverter, rawConverter);
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        Record kafkaRecord = createKafkaRecord(key, value, new Header[0]);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        assertNull(result.getError());
        GenericRecord finalRecord = result.getFinalRecord();
        assertNotNull(finalRecord);
        assertEquals(ByteBuffer.wrap(key), ByteBuffer.wrap((byte[]) finalRecord.get(RecordAssembler.KAFKA_KEY_FIELD)));
        assertEquals(ByteBuffer.wrap(value), ByteBuffer.wrap((byte[]) finalRecord.get(RecordAssembler.KAFKA_VALUE_FIELD)));
    }

    @Test
    void testConverterErrorHandling() {
        // Arrange
        Converter errorConverter = (topic, buffer) -> {
            throw new ConverterException("Test conversion error");
        };
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new StringConverter(), errorConverter);
        Record kafkaRecord = createKafkaRecord("key".getBytes(), "value".getBytes(), new Header[0]);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertEquals(DataError.ErrorType.CONVERT_ERROR, result.getError().getType());
        assertTrue(result.getError().getMessage().contains("Test conversion error"));
    }

    @Test
    void testWithFlattenTransform() {
        // Arrange
        Converter keyConverter = new StringConverter();

        Schema innerSchema = SchemaBuilder.record("Inner").fields().name("data").type().stringType().noDefault().endRecord();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord innerRecord = new GenericRecordBuilder(innerSchema)
                .set("data", "some-data")
                .build();
            return new ConversionResult(innerRecord, "id1");
        };

        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter, List.of(new FlattenTransform()));
        Record kafkaRecord = createKafkaRecord("key".getBytes(), "value".getBytes(), new Header[0]);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        GenericRecord finalRecord = result.getFinalRecord();

        // After flatten, the "data" field from innerRecord should be at the top level.
        assertEquals("some-data", finalRecord.get("data").toString());
        assertEquals("key", finalRecord.get(RecordAssembler.KAFKA_KEY_FIELD));
        assertFalse(finalRecord.hasField("value"), "The original 'value' wrapper field should be gone after flatten.");
    }

    @Test
    void testHeaderProcessing() {
        // Arrange
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), new RawConverter());
        Header[] headers = {new RecordHeader("h1", "v1".getBytes())};
        Record kafkaRecord = createKafkaRecord("key".getBytes(), "value".getBytes(), headers);

        // Act
        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        // Assert
        assertTrue(result.isSuccess());
        GenericRecord finalRecord = result.getFinalRecord();
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> headerMap = (Map<String, ByteBuffer>) finalRecord.get(RecordAssembler.KAFKA_HEADER_FIELD);
        assertEquals(1, headerMap.size());
        assertEquals(ByteBuffer.wrap("v1".getBytes()), headerMap.get("h1"));
    }

    @Test
    void testProcessHeadersWithMultipleEntriesIncludingNullValue() {
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), new RawConverter());
        Header[] headers = {
            new RecordHeader("h1", "v1".getBytes(StandardCharsets.UTF_8)),
            new RecordHeader("h2", null),
            new RecordHeader("h3", "v3".getBytes(StandardCharsets.UTF_8))
        };
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), headers);

        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        assertTrue(result.isSuccess());
        GenericRecord finalRecord = result.getFinalRecord();
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> headerMap = (Map<String, ByteBuffer>) finalRecord.get(RecordAssembler.KAFKA_HEADER_FIELD);
        assertEquals(3, headerMap.size());
        assertEquals(ByteBuffer.wrap("v1".getBytes(StandardCharsets.UTF_8)), headerMap.get("h1"));
        assertNull(headerMap.get("h2"));
        assertEquals(ByteBuffer.wrap("v3".getBytes(StandardCharsets.UTF_8)), headerMap.get("h3"));
    }

    @Test
    void testProcessHeadersReuseEmptyResultInstance() {
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), new RawConverter());

        Record recordWithoutHeaders = createKafkaRecord("key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8), null);
        ProcessingResult firstResult = processor.process(TEST_PARTITION, recordWithoutHeaders);
        assertTrue(firstResult.isSuccess());
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> firstHeaders = (Map<String, ByteBuffer>) firstResult.getFinalRecord().get(RecordAssembler.KAFKA_HEADER_FIELD);
        assertTrue(firstHeaders.isEmpty());

        Record recordWithEmptyHeaders = createKafkaRecord("key2".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8), new Header[0]);
        ProcessingResult secondResult = processor.process(TEST_PARTITION, recordWithEmptyHeaders);
        assertTrue(secondResult.isSuccess());
        @SuppressWarnings("unchecked")
        Map<String, ByteBuffer> secondHeaders = (Map<String, ByteBuffer>) secondResult.getFinalRecord().get(RecordAssembler.KAFKA_HEADER_FIELD);
        assertTrue(secondHeaders.isEmpty());
        assertSame(firstHeaders, secondHeaders);
    }

    @Test
    void testMetadataFieldsPopulatedOnSuccess() {
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), new RawConverter());
        int partition = 7;
        long offset = 456L;
        long timestamp = 1_234_567_890L;
        Record kafkaRecord = new SimpleRecord(offset, timestamp,
            "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8), new Header[0]);

        ProcessingResult result = processor.process(partition, kafkaRecord);

        assertTrue(result.isSuccess());
        GenericRecord finalRecord = result.getFinalRecord();
        GenericRecord metadata = (GenericRecord) finalRecord.get(RecordAssembler.KAFKA_METADATA_FIELD);
        assertNotNull(metadata);
        assertEquals(partition, ((Integer) metadata.get(RecordAssembler.METADATA_PARTITION_FIELD)).intValue());
        assertEquals(offset, ((Long) metadata.get(RecordAssembler.METADATA_OFFSET_FIELD)).longValue());
        assertEquals(timestamp, ((Long) metadata.get(RecordAssembler.METADATA_TIMESTAMP_FIELD)).longValue());
    }

    @Test
    void testWrapValueSchemaCacheReusedBetweenCalls() {
        Schema valueSchema = SchemaBuilder.record("CachedValue")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("field").type().stringType().noDefault()
            .endRecord();
        AtomicBoolean alternateIdentity = new AtomicBoolean(false);

        Converter keyConverter = new StringConverter();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord record = new GenericRecordBuilder(valueSchema)
                .set("field", alternateIdentity.get() ? "value-b" : "value-a")
                .build();
            String identity = alternateIdentity.get() ? "value-schema-b" : "value-schema-a";
            return new ConversionResult(record, identity);
        };
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter);

        Record kafkaRecord1 = createKafkaRecord("key1".getBytes(StandardCharsets.UTF_8), "value1".getBytes(StandardCharsets.UTF_8), new Header[0]);
        ProcessingResult result1 = processor.process(TEST_PARTITION, kafkaRecord1);
        assertTrue(result1.isSuccess());
        Cache<String, Schema> cache = extractValueWrapperSchemaCache(processor);
        assertEquals(1L, cache.size());

        Record kafkaRecord2 = createKafkaRecord("key2".getBytes(StandardCharsets.UTF_8), "value2".getBytes(StandardCharsets.UTF_8), new Header[0]);
        ProcessingResult result2 = processor.process(TEST_PARTITION, kafkaRecord2);
        assertTrue(result2.isSuccess());
        assertEquals(1L, cache.size());

        alternateIdentity.set(true);
        Record kafkaRecord3 = createKafkaRecord("key3".getBytes(StandardCharsets.UTF_8), "value3".getBytes(StandardCharsets.UTF_8), new Header[0]);
        ProcessingResult result3 = processor.process(TEST_PARTITION, kafkaRecord3);
        assertTrue(result3.isSuccess());
        assertEquals(2L, cache.size());
    }

    @Test
    void testCompositeSchemaIdentityReflectsTransformChain() {
        Schema valueSchema = SchemaBuilder.record("IdentityRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("field").type().stringType().noDefault()
            .endRecord();
        Converter keyConverter = new StringConverter();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord record = new GenericRecordBuilder(valueSchema)
                .set("field", "payload")
                .build();
            return new ConversionResult(record, "identity-value");
        };

        DefaultRecordProcessor ordered = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter,
            List.of(new NamedPassthroughTransform("A"), new NamedPassthroughTransform("B")));
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), new Header[0]);
        ProcessingResult orderedResult = ordered.process(TEST_PARTITION, kafkaRecord);
        assertTrue(orderedResult.isSuccess());
        String orderedIdentity = orderedResult.getFinalSchemaIdentity();
        assertTrue(orderedIdentity.endsWith("|t:A,B"));

        DefaultRecordProcessor reversed = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter,
            List.of(new NamedPassthroughTransform("B"), new NamedPassthroughTransform("A")));
        ProcessingResult reversedResult = reversed.process(TEST_PARTITION, kafkaRecord);
        assertTrue(reversedResult.isSuccess());
        assertNotEquals(orderedIdentity, reversedResult.getFinalSchemaIdentity());
    }

    @Test
    void testTransformReturningNullProducesError() {
        Schema valueSchema = SchemaBuilder.record("NullRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("field").type().stringType().noDefault()
            .endRecord();
        Converter keyConverter = new StringConverter();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord record = new GenericRecordBuilder(valueSchema)
                .set("field", "payload")
                .build();
            return new ConversionResult(record, "null-transform-identity");
        };

        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter, List.of(new NullingTransform()));
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), new Header[0]);

        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        assertFalse(result.isSuccess());
        assertEquals(DataError.ErrorType.TRANSFORMATION_ERROR, result.getError().getType());
        assertTrue(result.getError().getMessage().contains("NullingTransform"));
    }

    @Test
    void testTransformThrowsInvalidDataException() {
        Schema valueSchema = SchemaBuilder.record("InvalidRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("field").type().stringType().noDefault()
            .endRecord();
        Converter keyConverter = new StringConverter();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord record = new GenericRecordBuilder(valueSchema)
                .set("field", "payload")
                .build();
            return new ConversionResult(record, "invalid-transform-identity");
        };

        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter, List.of(new InvalidDataThrowingTransform()));
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), new Header[0]);

        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        assertFalse(result.isSuccess());
        assertEquals(DataError.ErrorType.DATA_ERROR, result.getError().getType());
        assertTrue(result.getError().getMessage().contains("Invalid data"));
    }

    @Test
    void testTransformThrowsTransformException() {
        Schema valueSchema = SchemaBuilder.record("TransformRecord")
            .namespace("kafka.automq.table.process.test")
            .fields()
            .name("field").type().stringType().noDefault()
            .endRecord();
        Converter keyConverter = new StringConverter();
        Converter valueConverter = (topic, buffer) -> {
            GenericRecord record = new GenericRecordBuilder(valueSchema)
                .set("field", "payload")
                .build();
            return new ConversionResult(record, "transform-exception-identity");
        };

        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, keyConverter, valueConverter, List.of(new ThrowingTransform()));
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), new Header[0]);

        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        assertFalse(result.isSuccess());
        assertEquals(DataError.ErrorType.TRANSFORMATION_ERROR, result.getError().getType());
        assertTrue(result.getError().getMessage().contains("transform failure"));
    }

    @Test
    void testConverterRestClientNotFoundReturnsDataError() {
        Converter restNotFoundConverter = (topic, buffer) -> {
            RestClientException restException = new RestClientException("missing", HTTP_NOT_FOUND, 40403);
            throw new RuntimeException("wrapper", restException);
        };
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), restNotFoundConverter);
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), null);

        ProcessingResult result = processor.process(TEST_PARTITION, kafkaRecord);

        assertFalse(result.isSuccess());
        assertEquals(DataError.ErrorType.CONVERT_ERROR, result.getError().getType());
        String errorMessage = result.getError().getMessage();
        assertNotNull(errorMessage);
        assertTrue(errorMessage.contains("Schema or subject not found for record"), () -> "actual message: " + errorMessage);
        assertTrue(errorMessage.contains("topic=" + TEST_TOPIC), () -> "actual message: " + errorMessage);
    }

    @Test
    void testConverterRestClientServerErrorPropagates() {
        Converter restErrorConverter = (topic, buffer) -> {
            RestClientException restException = new RestClientException("server", 500, 50001);
            throw new RuntimeException("wrapper", restException);
        };
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), restErrorConverter);
        Record kafkaRecord = createKafkaRecord("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8), null);

        assertThrows(SchemaRegistrySystemException.class, () -> processor.process(TEST_PARTITION, kafkaRecord));
    }

    @Test
    void testProcessWithNullRecordThrows() {
        DefaultRecordProcessor processor = new DefaultRecordProcessor(TEST_TOPIC, new RawConverter(), new RawConverter());

        assertThrows(NullPointerException.class, () -> processor.process(TEST_PARTITION, null));
    }

    private Cache<String, Schema> extractValueWrapperSchemaCache(DefaultRecordProcessor processor) {
        try {
            Field cacheField = DefaultRecordProcessor.class.getDeclaredField("valueWrapperSchemaCache");
            cacheField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Cache<String, Schema> cache = (Cache<String, Schema>) cacheField.get(processor);
            return cache;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access valueWrapperSchemaCache", e);
        }
    }

    /**
     * Test helper implementations.
     */
    private static class SimpleRecord implements Record {
        private final long offset;
        private final long timestamp;
        private final byte[] key;
        private final byte[] value;
        private final Header[] headers;

        public SimpleRecord(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
            this.headers = headers != null ? headers : new Header[0];
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
            return headers;
        }
    }

    private static final class NamedPassthroughTransform implements Transform {
        private final String name;

        private NamedPassthroughTransform(String name) {
            this.name = name;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // no-op
        }

        @Override
        public GenericRecord apply(GenericRecord record, TransformContext context) {
            return record;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private static final class NullingTransform implements Transform {
        @Override
        public void configure(Map<String, ?> configs) {
            // no-op
        }

        @Override
        public GenericRecord apply(GenericRecord record, TransformContext context) {
            return null;
        }
    }

    private static final class InvalidDataThrowingTransform implements Transform {
        @Override
        public void configure(Map<String, ?> configs) {
            // no-op
        }

        @Override
        public GenericRecord apply(GenericRecord record, TransformContext context) {
            throw new InvalidDataException("Invalid data from transform");
        }

        @Override
        public String getName() {
            return "InvalidDataTransform";
        }
    }

    private static final class ThrowingTransform implements Transform {
        @Override
        public void configure(Map<String, ?> configs) {
            // no-op
        }

        @Override
        public GenericRecord apply(GenericRecord record, TransformContext context) {
            throw new TransformException("transform failure");
        }
    }
}
