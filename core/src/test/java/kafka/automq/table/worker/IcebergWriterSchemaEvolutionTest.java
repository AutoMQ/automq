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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @Test
    void testAddRequiredFieldsInNestedStruct() throws IOException {
        // v1: {id, user{name}}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Collections.singletonList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: Add required primitive field in nested struct (age)
        // Add required nested struct field (profile)
        // Add required list field in nested struct (hobbies)
        Schema profileV2 = Schema.createRecord("Profile", null, null, false);
        profileV2.setFields(Arrays.asList(
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("level", Schema.create(Schema.Type.INT), null, null)));

        Schema userV2 = Schema.createRecord("User", null, null, false);
        Schema hobbiesSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("profile", profileV2, null, null),
            new Schema.Field("hobbies", hobbiesSchema, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord profileRecordV2 = new GenericData.Record(profileV2);
        profileRecordV2.put("city", "Shanghai");
        profileRecordV2.put("level", 5);

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("age", 30);
        userRecordV2.put("profile", profileRecordV2);
        userRecordV2.put("hobbies", Arrays.asList("reading", "coding"));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify nested primitive field
        assertNotNull(table.schema().findField("_kafka_value.user.age"));
        // Verify nested struct field
        assertNotNull(table.schema().findField("_kafka_value.user.profile"));
        assertNotNull(table.schema().findField("_kafka_value.user.profile.city"));
        // Verify nested list field
        assertNotNull(table.schema().findField("_kafka_value.user.hobbies"));
    }

    @Test
    void testAddRequiredFieldsInCollectionElement() throws IOException {
        // v1: {id, addresses: list<{street}>}
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Collections.singletonList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("street", "Main St");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: Add required primitive field in list element (zipCode, floor)
        // Add required nested struct field in list element (location)
        Schema locationV2 = Schema.createRecord("Location", null, null, false);
        locationV2.setFields(Arrays.asList(
            new Schema.Field("lat", Schema.create(Schema.Type.DOUBLE), null, null),
            new Schema.Field("lng", Schema.create(Schema.Type.DOUBLE), null, null)));

        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        addressV2.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("zipCode", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("floor", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("location", locationV2, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        GenericRecord locationRecordV2 = new GenericData.Record(locationV2);
        locationRecordV2.put("lat", 39.9042);
        locationRecordV2.put("lng", 116.4074);

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Second St");
        addressRecordV2.put("zipCode", "100000");
        addressRecordV2.put("floor", 5);
        addressRecordV2.put("location", locationRecordV2);

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify primitive fields in list element
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.zipCode"));
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.floor"));
        // Verify struct field in list element
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.location"));
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.location.lat"));
    }

    @Test
    void testAddOptionalCollection() throws IOException {
        // v1: {id, name}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "alice");

        // v2: {id, name, tags: list<string>}
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema optionalList = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listSchema));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("tags", optionalList, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", "bob");
        avroRecordV2.put("tags", Arrays.asList("tag1", "tag2"));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.tags"));
        assertEquals(true, table.schema().findField("_kafka_value.tags").type().isListType());
    }


    @Test
    void testAddRequiredCollections() throws IOException {
        // v1: {id, name}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "alice");

        // v2: Add list<primitive> (tags)
        // Add list<struct> (addresses)
        // Add map<K,primitive> (scores)
        // Add map<K,struct> (locations)
        Schema addressSchema = Schema.createRecord("Address", null, null, false);
        addressSchema.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema locationSchema = Schema.createRecord("Location", null, null, false);
        locationSchema.setFields(Arrays.asList(
            new Schema.Field("lat", Schema.create(Schema.Type.DOUBLE), null, null),
            new Schema.Field("lng", Schema.create(Schema.Type.DOUBLE), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema tagsList = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema addressesList = Schema.createArray(addressSchema);
        Schema scoresMap = Schema.createMap(Schema.create(Schema.Type.INT));
        Schema locationsMap = Schema.createMap(locationSchema);

        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("tags", tagsList, null, null),
            new Schema.Field("addresses", addressesList, null, null),
            new Schema.Field("scores", scoresMap, null, null),
            new Schema.Field("locations", locationsMap, null, null)));

        GenericRecord addressRecord = new GenericData.Record(addressSchema);
        addressRecord.put("street", "Main St");
        addressRecord.put("city", "Beijing");

        GenericRecord locationRecord = new GenericData.Record(locationSchema);
        locationRecord.put("lat", 39.9042);
        locationRecord.put("lng", 116.4074);

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", "bob");
        avroRecordV2.put("tags", Arrays.asList("tag1", "tag2"));
        avroRecordV2.put("addresses", Collections.singletonList(addressRecord));
        java.util.Map<String, Integer> scoresMapData = new java.util.HashMap<>();
        scoresMapData.put("math", 95);
        avroRecordV2.put("scores", scoresMapData);
        java.util.Map<String, GenericRecord> locationsMapData = new java.util.HashMap<>();
        locationsMapData.put("home", locationRecord);
        avroRecordV2.put("locations", locationsMapData);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify list<primitive>
        assertNotNull(table.schema().findField("_kafka_value.tags"));
        assertEquals(true, table.schema().findField("_kafka_value.tags").type().isListType());
        // Verify list<struct>
        assertNotNull(table.schema().findField("_kafka_value.addresses"));
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.street"));
        // Verify map<K,primitive>
        assertNotNull(table.schema().findField("_kafka_value.scores"));
        assertEquals(true, table.schema().findField("_kafka_value.scores").type().isMapType());
        // Verify map<K,struct>
        assertNotNull(table.schema().findField("_kafka_value.locations"));
        assertNotNull(table.schema().findField("_kafka_value.locations.value.lat"));
    }


    @Test
    void testAddRequiredFieldInCollectionElement() throws IOException {
        // v1: addresses: list<{street}>
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Collections.singletonList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("street", "Main St");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: addresses: list<{street, required city}>
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        addressV2.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Second St");
        addressRecordV2.put("city", "Beijing");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.city"));
        assertEquals(Types.StringType.get(), table.schema().findField("_kafka_value.addresses.element.city").type());
    }

    @Test
    void testAddRequiredFieldInMapValueStruct() throws IOException {
        // v1: locations: map<string, {city}>
        Schema locationV1 = Schema.createRecord("Location", null, null, false);
        locationV1.setFields(Collections.singletonList(
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV1 = Schema.createMap(locationV1);
        Schema optionalMapV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("locations", optionalMapV1, null, null)));

        GenericRecord locationRecordV1 = new GenericData.Record(locationV1);
        locationRecordV1.put("city", "Shanghai");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        java.util.Map<String, GenericRecord> mapData1 = new java.util.HashMap<>();
        mapData1.put("home", locationRecordV1);
        avroRecordV1.put("locations", mapData1);

        // v2: locations: map<string, {city, required country}>
        Schema locationV2 = Schema.createRecord("Location", null, null, false);
        locationV2.setFields(Arrays.asList(
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("country", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV2 = Schema.createMap(locationV2);
        Schema optionalMapV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("locations", optionalMapV2, null, null)));

        GenericRecord locationRecordV2 = new GenericData.Record(locationV2);
        locationRecordV2.put("city", "Beijing");
        locationRecordV2.put("country", "China");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        java.util.Map<String, GenericRecord> mapData2 = new java.util.HashMap<>();
        mapData2.put("work", locationRecordV2);
        avroRecordV2.put("locations", mapData2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.locations.value.country"));
        assertEquals(Types.StringType.get(), table.schema().findField("_kafka_value.locations.value.country").type());
    }


    // ========== Add Optional Field Tests ==========

    @Test
    void testAddOptionalFieldInNestedStruct() throws IOException {
        // v1: user{name}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Collections.singletonList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: user{name, email}
        Schema userV2 = Schema.createRecord("User", null, null, false);
        Schema optionalEmail = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", optionalEmail, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("email", "bob@example.com");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.user.email"));
    }


    @Test
    void testDropRequiredCollection() throws IOException {
        // v1: {id, tags: list<string>}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("tags", listSchema, null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("tags", Arrays.asList("tag1", "tag2"));

        // v2: {id} - dropped tags
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Collections.singletonList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.tags"));
        assertEquals(false, table.schema().findField("_kafka_value.tags").isRequired());
    }

    @Test
    void testDropRequiredFieldInNestedStruct() throws IOException {
        // v1: user{name, email, age}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        userRecordV1.put("email", "alice@example.com");
        userRecordV1.put("age", 25);
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: user{name, email} - dropped age
        Schema userV2 = Schema.createRecord("User", null, null, false);
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("email", "bob@example.com");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.user.age"));
        assertEquals(false, table.schema().findField("_kafka_value.user.age").isRequired());
    }

    @Test
    void testMakeRequiredFieldOptionalInNestedStruct() throws IOException {
        // v1: user{required name, required age}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        userRecordV1.put("age", 25);
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: user{required name, optional age}
        Schema userV2 = Schema.createRecord("User", null, null, false);
        Schema optionalAge = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("age", optionalAge, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("age", null);
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertEquals(false, table.schema().findField("_kafka_value.user.age").isRequired());
    }

    @Test
    void testPromoteFieldTypeInNestedStruct() throws IOException {
        // v1: user{age: int}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Collections.singletonList(
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("age", 25);
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: user{age: long}
        Schema userV2 = Schema.createRecord("User", null, null, false);
        userV2.setFields(Collections.singletonList(
            new Schema.Field("age", Schema.create(Schema.Type.LONG), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("age", 30L);
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertEquals(Types.LongType.get(), table.schema().findField("_kafka_value.user.age").type());
    }

    // ========== Collection Field Tests ==========



    @Test
    void testAddOptionalMapCollection() throws IOException {
        // v1: {id, name}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("name", "alice");

        // v2: {id, name, metadata: map<string,string>}
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        Schema optionalMap = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapSchema));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("metadata", optionalMap, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("name", "bob");
        java.util.Map<String, String> metadataMap = new java.util.HashMap<>();
        metadataMap.put("key1", "value1");
        metadataMap.put("key2", "value2");
        avroRecordV2.put("metadata", metadataMap);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.metadata"));
        assertEquals(true, table.schema().findField("_kafka_value.metadata").type().isMapType());
    }


    @Test
    void testAddOptionalFieldInCollectionElement() throws IOException {
        // v1: addresses: list<{street}>
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Collections.singletonList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("street", "Main St");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: addresses: list<{street, zipCode}>
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        Schema optionalZip = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        addressV2.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("zipCode", optionalZip, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Second St");
        addressRecordV2.put("zipCode", "12345");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.zipCode"));
    }

    @Test
    void testDropRequiredFieldInMapValueStruct() throws IOException {
        // v1: attributes: map<string, {city, country}>
        Schema locationV1 = Schema.createRecord("Location", null, null, false);
        locationV1.setFields(Arrays.asList(
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("country", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV1 = Schema.createMap(locationV1);
        Schema optionalMapV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("attributes", optionalMapV1, null, null)));

        GenericRecord locationRecordV1 = new GenericData.Record(locationV1);
        locationRecordV1.put("city", "Beijing");
        locationRecordV1.put("country", "China");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        java.util.Map<String, GenericRecord> mapData1 = new java.util.HashMap<>();
        mapData1.put("home", locationRecordV1);
        avroRecordV1.put("attributes", mapData1);

        // v2: attributes: map<string, {city}> - dropped country
        Schema locationV2 = Schema.createRecord("Location", null, null, false);
        locationV2.setFields(Collections.singletonList(
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV2 = Schema.createMap(locationV2);
        Schema optionalMapV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("attributes", optionalMapV2, null, null)));

        GenericRecord locationRecordV2 = new GenericData.Record(locationV2);
        locationRecordV2.put("city", "Shanghai");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        java.util.Map<String, GenericRecord> mapData2 = new java.util.HashMap<>();
        mapData2.put("work", locationRecordV2);
        avroRecordV2.put("attributes", mapData2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.attributes.value.country"));
        assertEquals(false, table.schema().findField("_kafka_value.attributes.value.country").isRequired());
    }

    @Test
    void testPromoteFieldTypeInCollectionElement() throws IOException {
        // v1: addresses: list<{zip: int}>
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Collections.singletonList(
            new Schema.Field("zip", Schema.create(Schema.Type.INT), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("zip", 12345);
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: addresses: list<{zip: long}>
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        addressV2.setFields(Collections.singletonList(
            new Schema.Field("zip", Schema.create(Schema.Type.LONG), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("zip", 67890L);
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertEquals(Types.LongType.get(), table.schema().findField("_kafka_value.addresses.element.zip").type());
    }

    // ========== Drop Required Field Tests ==========

    @Test
    void testDropRequiredFieldInCollectionElement() throws IOException {
        // v1: addresses: list<{street, city, zipCode}>
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("zipCode", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", listV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("street", "Main St");
        addressRecordV1.put("city", "Beijing");
        addressRecordV1.put("zipCode", "100000");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: addresses: list<{street, city}> - dropped required zipCode
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        addressV2.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", listV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Second St");
        addressRecordV2.put("city", "Shanghai");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that the dropped required field is now optional
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.zipCode"));
        assertEquals(false, table.schema().findField("_kafka_value.addresses.element.zipCode").isRequired());
    }

    // ========== Drop Optional Field Tests ==========

    @Test
    void testDropOptionalFieldInNestedStruct() throws IOException {
        // v1: user{name, email, phone}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        Schema optionalEmail = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        Schema optionalPhone = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        userV1.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", optionalEmail, null, null),
            new Schema.Field("phone", optionalPhone, null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        userRecordV1.put("email", "alice@example.com");
        userRecordV1.put("phone", "123-456-7890");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: user{name, email} - dropped optional phone
        Schema userV2 = Schema.createRecord("User", null, null, false);
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", optionalEmail, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("email", "bob@example.com");
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that the dropped optional field still exists and remains optional
        assertNotNull(table.schema().findField("_kafka_value.user.phone"));
        assertEquals(false, table.schema().findField("_kafka_value.user.phone").isRequired());
    }

    @Test
    void testDropOptionalCollections() throws IOException {
        // v1: {id, tags: optional list<string>, metadata: optional map<string,string>}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema optionalList = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listSchema));
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        Schema optionalMap = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapSchema));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("tags", optionalList, null, null),
            new Schema.Field("metadata", optionalMap, null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("tags", Arrays.asList("tag1", "tag2"));
        java.util.Map<String, String> metadataMap = new java.util.HashMap<>();
        metadataMap.put("key1", "value1");
        avroRecordV1.put("metadata", metadataMap);

        // v2: {id} - dropped both optional collections
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Collections.singletonList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that dropped optional collections still exist and remain optional
        assertNotNull(table.schema().findField("_kafka_value.tags"));
        assertEquals(false, table.schema().findField("_kafka_value.tags").isRequired());
        assertNotNull(table.schema().findField("_kafka_value.metadata"));
        assertEquals(false, table.schema().findField("_kafka_value.metadata").isRequired());
    }

    // ========== Make Field Optional Tests ==========

    @Test
    void testMakeRequiredFieldOptional() throws IOException {
        // v1: {id, required email}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("email", Schema.create(Schema.Type.STRING), null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("email", "alice@example.com");

        // v2: {id, optional email}
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema optionalEmail = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("email", optionalEmail, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("email", null);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.email"));
        assertEquals(false, table.schema().findField("_kafka_value.email").isRequired());
    }

    @Test
    void testMakeRequiredFieldOptionalInCollectionElement() throws IOException {
        // v1: addresses: list<{street, required city}>
        Schema addressV1 = Schema.createRecord("Address", null, null, false);
        addressV1.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(addressV1);
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV1, null, null)));

        GenericRecord addressRecordV1 = new GenericData.Record(addressV1);
        addressRecordV1.put("street", "Main St");
        addressRecordV1.put("city", "Beijing");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("addresses", Collections.singletonList(addressRecordV1));

        // v2: addresses: list<{street, optional city}>
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        Schema optionalCity = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        addressV2.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("city", optionalCity, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Second St");
        addressRecordV2.put("city", null);
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("addresses", Collections.singletonList(addressRecordV2));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that the field in collection element is now optional
        assertNotNull(table.schema().findField("_kafka_value.addresses.element.city"));
        assertEquals(false, table.schema().findField("_kafka_value.addresses.element.city").isRequired());
    }

    @Test
    void testMakeRequiredCollectionOptional() throws IOException {
        // v1: {id, required tags: list<string>}
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(Schema.create(Schema.Type.STRING));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("tags", listV1, null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("tags", Arrays.asList("tag1", "tag2"));

        // v2: {id, optional tags: list<string>}
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(Schema.create(Schema.Type.STRING));
        Schema optionalList = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("tags", optionalList, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("tags", null);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.tags"));
        assertEquals(false, table.schema().findField("_kafka_value.tags").isRequired());
    }

    // ========== Promote Field Type Tests ==========

    @Test
    void testPromoteCollectionElementType() throws IOException {
        // v1: scores: list<int>
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV1 = Schema.createArray(Schema.create(Schema.Type.INT));
        Schema optionalListV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("scores", optionalListV1, null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("scores", Arrays.asList(90, 85, 95));

        // v2: scores: list<long>
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema listV2 = Schema.createArray(Schema.create(Schema.Type.LONG));
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("scores", optionalListV2, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("scores", Arrays.asList(100L, 95L, 98L));

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that the collection element type cannot be promoted from int to long
        assertNotNull(table.schema().findField("_kafka_value.scores"));
        assertEquals(true, table.schema().findField("_kafka_value.scores").type().isListType());
        assertEquals(Types.IntegerType.get(),
            table.schema().findField("_kafka_value.scores").type().asListType().elementType());
    }

    @Test
    void testPromoteMapValueType() throws IOException {
        // v1: metadata: map<string, int>
        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV1 = Schema.createMap(Schema.create(Schema.Type.INT));
        Schema optionalMapV1 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV1));
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("metadata", optionalMapV1, null, null)));

        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        java.util.Map<String, Integer> mapData1 = new java.util.HashMap<>();
        mapData1.put("score", 90);
        mapData1.put("rank", 5);
        avroRecordV1.put("metadata", mapData1);

        // v2: metadata: map<string, long>
        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        Schema mapV2 = Schema.createMap(Schema.create(Schema.Type.LONG));
        Schema optionalMapV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), mapV2));
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("metadata", optionalMapV2, null, null)));

        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        java.util.Map<String, Long> mapData2 = new java.util.HashMap<>();
        mapData2.put("score", 100L);
        mapData2.put("rank", 1L);
        avroRecordV2.put("metadata", mapData2);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));

        Table table = catalog.loadTable(tableId);
        // Verify that the map value type cannot be promoted from int to long
        assertNotNull(table.schema().findField("_kafka_value.metadata"));
        assertEquals(true, table.schema().findField("_kafka_value.metadata").type().isMapType());
        assertEquals(Types.IntegerType.get(),
            table.schema().findField("_kafka_value.metadata").type().asMapType().valueType());
    }

    // ========== Complex Mixed Scenarios ==========

    @Test
    void testComplexNestedAndCollectionEvolution() throws IOException {
        // v1: {id, user{name}}
        Schema userV1 = Schema.createRecord("User", null, null, false);
        userV1.setFields(Collections.singletonList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        Schema avroSchemaV1 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV1.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV1, null, null)));

        GenericRecord userRecordV1 = new GenericData.Record(userV1);
        userRecordV1.put("name", "alice");
        GenericRecord avroRecordV1 = new GenericData.Record(avroSchemaV1);
        avroRecordV1.put("id", 1L);
        avroRecordV1.put("user", userRecordV1);

        // v2: {id, user{name, addresses: list<{street}>}}
        Schema addressV2 = Schema.createRecord("Address", null, null, false);
        addressV2.setFields(Collections.singletonList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null)));

        Schema userV2 = Schema.createRecord("User", null, null, false);
        Schema listV2 = Schema.createArray(addressV2);
        Schema optionalListV2 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV2));
        userV2.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("addresses", optionalListV2, null, null)));

        Schema avroSchemaV2 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV2.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV2, null, null)));

        GenericRecord addressRecordV2 = new GenericData.Record(addressV2);
        addressRecordV2.put("street", "Main St");
        GenericRecord userRecordV2 = new GenericData.Record(userV2);
        userRecordV2.put("name", "bob");
        userRecordV2.put("addresses", Collections.singletonList(addressRecordV2));
        GenericRecord avroRecordV2 = new GenericData.Record(avroSchemaV2);
        avroRecordV2.put("id", 2L);
        avroRecordV2.put("user", userRecordV2);

        // v3: {id, user{name, addresses: list<{street, zipCode}>}}
        Schema addressV3 = Schema.createRecord("Address", null, null, false);
        Schema optionalZip = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        addressV3.setFields(Arrays.asList(
            new Schema.Field("street", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("zipCode", optionalZip, null, null)));

        Schema userV3 = Schema.createRecord("User", null, null, false);
        Schema listV3 = Schema.createArray(addressV3);
        Schema optionalListV3 = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), listV3));
        userV3.setFields(Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("addresses", optionalListV3, null, null)));

        Schema avroSchemaV3 = Schema.createRecord("TestRecord", null, null, false);
        avroSchemaV3.setFields(Arrays.asList(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null),
            new Schema.Field("user", userV3, null, null)));

        GenericRecord addressRecordV3 = new GenericData.Record(addressV3);
        addressRecordV3.put("street", "Second St");
        addressRecordV3.put("zipCode", "12345");
        GenericRecord userRecordV3 = new GenericData.Record(userV3);
        userRecordV3.put("name", "charlie");
        userRecordV3.put("addresses", Collections.singletonList(addressRecordV3));
        GenericRecord avroRecordV3 = new GenericData.Record(avroSchemaV3);
        avroRecordV3.put("id", 3L);
        avroRecordV3.put("user", userRecordV3);

        when(kafkaAvroDeserializer.deserialize(anyString(), any(), any(ByteBuffer.class)))
            .thenReturn(avroRecordV1)
            .thenReturn(avroRecordV2)
            .thenReturn(avroRecordV3);

        writer.write(0, createMockKafkaRecord(1, 0));
        writer.write(0, createMockKafkaRecord(2, 1));
        writer.write(0, createMockKafkaRecord(3, 2));

        Table table = catalog.loadTable(tableId);
        assertNotNull(table.schema().findField("_kafka_value.user.addresses"));
        assertNotNull(table.schema().findField("_kafka_value.user.addresses.element.street"));
        assertNotNull(table.schema().findField("_kafka_value.user.addresses.element.zipCode"));
    }

    // ========== Helper Methods ==========

    private String generateRandomTableName() {
        return "test_table_" + ThreadLocalRandom.current().nextInt(100000, 999999);
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
