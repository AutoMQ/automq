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

package kafka.automq.table.process.transform;

import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class DebeziumUnwrapTransformTest {

    private static final Schema ROW_SCHEMA =
        SchemaBuilder.record("row")
            .fields()
            .requiredLong("account_id")
            .requiredDouble("balance")
            .requiredString("last_updated")
            .endRecord();

    private static final Schema SOURCE_SCHEMA =
        SchemaBuilder.record("source")
            .fields()
            .requiredString("db")
            .optionalString("schema")
            .requiredString("table")
            .endRecord();

    private static final Schema DEBEZIUM_SCHEMA =
        SchemaBuilder.record("debezium_event")
            .fields()
            .requiredString("op")
            .optionalLong("ts_ms")
            .name("source").type(SOURCE_SCHEMA).noDefault()
            .name("before").type().unionOf().nullType().and().type(ROW_SCHEMA).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(ROW_SCHEMA).endUnion().nullDefault()
            .endRecord();

    private static final Schema SOURCE_SCHEMA_WITHOUT_SCHEMA_FIELD =
        SchemaBuilder.record("source_no_schema")
            .fields()
            .requiredString("db")
            .requiredString("table")
            .endRecord();

    private static final Schema DEBEZIUM_SCHEMA_WITHOUT_SCHEMA_IN_SOURCE =
        SchemaBuilder.record("debezium_event_no_source_schema")
            .fields()
            .requiredString("op")
            .optionalLong("ts_ms")
            .name("source").type(SOURCE_SCHEMA_WITHOUT_SCHEMA_FIELD).noDefault()
            .name("before").type().unionOf().nullType().and().type(ROW_SCHEMA).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(ROW_SCHEMA).endUnion().nullDefault()
            .endRecord();

    private DebeziumUnwrapTransform transform;
    private FlattenTransform flattenTransform;
    private TransformContext context;
    private Record kafkaRecord;

    @BeforeEach
    void setUp() {
        transform = new DebeziumUnwrapTransform();
        transform.configure(Collections.emptyMap());

        flattenTransform = new FlattenTransform();
        flattenTransform.configure(Collections.emptyMap());

        context = mock(TransformContext.class);
        kafkaRecord = mock(Record.class);
        when(context.getKafkaRecord()).thenReturn(kafkaRecord);
        when(kafkaRecord.offset()).thenReturn(123L);
    }

    @Test
    void testNullRecord() {
        assertThrows(NullPointerException.class, () -> transform.apply(null, context));
    }

    @Test
    void testCreateOperation() throws TransformException {
        GenericRecord event = createDebeziumEvent("c", 1L, 100.0);
        GenericRecord result = transform.apply(event, context);

        assertNotNull(result);
        assertEquals(1L, result.get("account_id"));
        assertEquals(100.0, result.get("balance"));
        GenericRecord cdc = (GenericRecord) result.get("_cdc");
        assertNotNull(cdc);
        assertEquals("I", cdc.get("op"));
        assertEquals(123L, cdc.get("offset"));
        assertTrue(cdc.get("ts") instanceof Long);
        assertEquals("test_schema.test_table", cdc.get("source"));
    }

    @Test
    void testUpdateOperation() throws TransformException {
        GenericRecord event = createDebeziumEvent("u", 2L, 200.0);
        GenericRecord result = transform.apply(event, context);

        assertNotNull(result);
        assertEquals(2L, result.get("account_id"));
        assertEquals(200.0, result.get("balance"));
        GenericRecord cdc = (GenericRecord) result.get("_cdc");
        assertNotNull(cdc);
        assertEquals("U", cdc.get("op"));
    }

    @Test
    void testDeleteOperation() throws TransformException {
        GenericRecord event = createDebeziumEvent("d", 3L, 300.0);
        GenericRecord result = transform.apply(event, context);

        assertNotNull(result);
        assertEquals(3L, result.get("account_id"));
        assertEquals(300.0, result.get("balance"));
        GenericRecord cdc = (GenericRecord) result.get("_cdc");
        assertNotNull(cdc);
        assertEquals("D", cdc.get("op"));
    }

    @Test
    void testDeleteWithNullBefore() {
        GenericRecordBuilder builder = new GenericRecordBuilder(DEBEZIUM_SCHEMA)
            .set("op", "d")
            .set("ts_ms", System.currentTimeMillis())
            .set("source", createSourceRecord())
            .set("before", null)
            .set("after", null);
        GenericRecord event = builder.build();

        TransformException e = assertThrows(TransformException.class, () -> transform.apply(event, context));
        assertTrue(e.getMessage().contains("Invalid DELETE record: missing required 'before' data"));
    }


    @Test
    void testSourceWithNullSchema() throws TransformException {
        GenericRecord sourceWithNullSchema = new GenericRecordBuilder(SOURCE_SCHEMA)
            .set("db", "test_db_from_db_field")
            .set("schema", null)
            .set("table", "test_table")
            .build();

        GenericRecord event = createDebeziumEvent("c", 1L, 100.0, sourceWithNullSchema);
        GenericRecord result = transform.apply(event, context);

        GenericRecord cdc = (GenericRecord) result.get("_cdc");
        assertNotNull(cdc);
        assertEquals("test_db_from_db_field.test_table", cdc.get("source"));
    }

    @Test
    void testSourceWithoutSchemaField() throws TransformException {
        GenericRecord sourceWithoutSchemaField = new GenericRecordBuilder(SOURCE_SCHEMA_WITHOUT_SCHEMA_FIELD)
            .set("db", "db_only")
            .set("table", "table_from_db_only")
            .build();

        GenericRecord row = createRowRecord(1L, 100.0);
        GenericRecord event = new GenericRecordBuilder(DEBEZIUM_SCHEMA_WITHOUT_SCHEMA_IN_SOURCE)
            .set("op", "c")
            .set("ts_ms", System.currentTimeMillis())
            .set("source", sourceWithoutSchemaField)
            .set("after", row)
            .set("before", null)
            .build();

        GenericRecord result = transform.apply(event, context);

        GenericRecord cdc = (GenericRecord) result.get("_cdc");
        assertNotNull(cdc);
        assertEquals("db_only.table_from_db_only", cdc.get("source"));
    }

    private GenericRecord createDebeziumEvent(String op, long accountId, double balance) {
        return createDebeziumEvent(op, accountId, balance, createSourceRecord());
    }

    private GenericRecord createDebeziumEvent(String op, long accountId, double balance, GenericRecord source) {
        GenericRecord row = createRowRecord(accountId, balance);
        GenericRecordBuilder builder = new GenericRecordBuilder(DEBEZIUM_SCHEMA)
            .set("op", op)
            .set("ts_ms", System.currentTimeMillis())
            .set("source", source);

        if ("c".equals(op) || "r".equals(op) || "u".equals(op)) {
            builder.set("after", row);
        }
        if ("d".equals(op) || "u".equals(op)) {
            builder.set("before", row);
        }
        return builder.build();
    }

    private GenericRecord createRowRecord(long accountId, double balance) {
        return new GenericRecordBuilder(ROW_SCHEMA)
            .set("account_id", accountId)
            .set("balance", balance)
            .set("last_updated", "2025-01-01T00:00:00Z")
            .build();
    }

    private GenericRecord createSourceRecord() {
        return new GenericRecordBuilder(SOURCE_SCHEMA)
            .set("db", "test_db")
            .set("schema", "test_schema")
            .set("table", "test_table")
            .build();
    }
}
